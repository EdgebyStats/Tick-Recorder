"""Account setup logic — AccountSetupMixin."""

from __future__ import annotations

import time

from twisted.internet import defer, reactor

from ctrader_open_api import Protobuf
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq,
    ProtoOAGetAccountListByAccessTokenReq,
    ProtoOAAccountAuthReq,
    ProtoOATraderReq,
    ProtoOASymbolsListReq,
    ProtoOASymbolByIdReq,
    ProtoOASubscribeSpotsReq,
)
from google.protobuf.json_format import MessageToDict

from tick_recorder.core.retry_policy import ApiError
from tick_recorder.storage.constants import SETUP_RETRY_DELAY
from tick_recorder.utils.time_utils import utc_now_iso


class AccountSetupMixin:
    """Account authorization and symbol subscription.

    Mixed into HostSession. Provides:
    - setup_all(generation): app auth + per-account setup (called on connect)
    - setup_account(a): orchestrates one account's setup with generation guard
    - _setup_account(a, gen): actual setup sequence
    - schedule_setup(a, delay): delayed retry on failure
    """

    @defer.inlineCallbacks
    def setup_all(self, generation):
        try:
            yield self.call(ProtoOAApplicationAuthReq(
                clientId=self.deps.client_id,
                clientSecret=self.deps.client_secret,
            ))
            self.app_authed = True
            self.storage.event("app_authorised", host=self.host_key)
        except Exception as e:
            self.storage.event("app_auth_failed", host=self.host_key, error=e)
            return  # Wrong credentials — nothing else will work.
        for account in self.accounts:
            if generation != self.generation:
                return
            yield self.setup_account(account)

    @defer.inlineCallbacks
    def setup_account(self, a):
        generation = self.generation
        if not self.client.isConnected or not self.app_authed or a.subscribed:
            return
        if a.setup_running == generation:
            return
        a.setup_running = generation
        try:
            yield self._setup_account(a, generation)
        except Exception as e:
            # BUG FIX: clear subscribed on failure so retry actually runs.
            # Without this, account could be stuck "subscribed but broken"
            # and the next setup_account call would early-return.
            a.subscribed = False
            self.storage.event("account_setup_failed", label=a.label, error=e)
            if generation == self.generation:
                self.schedule_setup(a, SETUP_RETRY_DELAY)
        finally:
            if a.setup_running == generation:
                a.setup_running = False

    def schedule_setup(self, a, delay):
        reactor.callLater(delay, self.setup_account, a)

    @defer.inlineCallbacks
    def _setup_account(self, a, generation):
        accounts = yield self.call(ProtoOAGetAccountListByAccessTokenReq(
            accessToken=a.access_token
        ))
        entry = next((x for x in accounts.ctidTraderAccount
                      if x.traderLogin == a.login), None)
        if entry is None:
            raise RuntimeError(f"login {a.login} is not on this access token; "
                               f"regenerate it and tick this account on the consent screen")
        if entry.isLive != (self.host_key == "live"):
            raise RuntimeError(f"login {a.login} is {'live' if entry.isLive else 'demo'}; "
                               f"set host to \"{'live' if entry.isLive else 'demo'}\" in the config")
        a.ctid = entry.ctidTraderAccountId

        try:
            yield self.call(ProtoOAAccountAuthReq(
                ctidTraderAccountId=a.ctid, accessToken=a.access_token
            ))
        except ApiError as e:
            if e.code == "CH_ACCESS_TOKEN_INVALID" and a.refresh_token:
                yield self.refresh_token(a)  # from AuthMixin
                yield self.call(ProtoOAAccountAuthReq(
                    ctidTraderAccountId=a.ctid, accessToken=a.access_token
                ))
            else:
                raise
        self.storage.event("account_authorised", label=a.label,
                           login=a.login, ctid=a.ctid)

        trader = yield self.call(ProtoOATraderReq(ctidTraderAccountId=a.ctid))
        symbols = yield self.call(ProtoOASymbolsListReq(ctidTraderAccountId=a.ctid))
        by_name = {s.symbolName.upper(): s for s in symbols.symbol}
        wanted = []
        for name in a.symbol_names:
            s = by_name.get(name.upper())
            if s is None:
                self.storage.event("symbol_not_found", label=a.label, symbol=name)
            else:
                wanted.append(s)
        if not wanted:
            raise RuntimeError("none of the configured symbols exist on this account")
        details = yield self.call(ProtoOASymbolByIdReq(
            ctidTraderAccountId=a.ctid,
            symbolId=[s.symbolId for s in wanted],
        ))
        a.symbols = {s.symbolId: s.symbolName for s in wanted}

        self.storage.meta(a.label, {
            "recorded_utc": utc_now_iso(),
            "host": self.host_key,
            "account": MessageToDict(entry),
            "trader": MessageToDict(trader.trader),
            "recorded_symbols": [MessageToDict(s) for s in details.symbol],
            "all_symbols": [MessageToDict(s) for s in symbols.symbol],
        })

        if generation != self.generation:
            return
        try:
            yield self.call(ProtoOASubscribeSpotsReq(
                ctidTraderAccountId=a.ctid,
                symbolId=list(a.symbols),
                subscribeToSpotTimestamp=True,
            ))
        except ApiError as e:
            if e.code != "ALREADY_SUBSCRIBED":
                raise
        a.subscribed = True
        a.last_spot_ns = time.time_ns()
        a.silent_logged = False
        self.storage.event("subscribed", label=a.label,
                           symbols=list(a.symbols.values()))
