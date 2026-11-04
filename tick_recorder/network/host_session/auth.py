"""Token refresh logic — AuthMixin."""

from __future__ import annotations

from twisted.internet import defer

from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOARefreshTokenReq



class AuthMixin:
    """Token refresh logic.

    Mixed into HostSession. Provides:
    - refresh_token(a): refreshes an expired access token, updates all accounts
      sharing that token, persists to config.

    TODO: Add per-token DeferredLock to handle concurrent refresh race.
    Current implementation has a race if multiple accounts share an access
    token and both detect CH_ACCESS_TOKEN_INVALID simultaneously.
    """

    @defer.inlineCallbacks
    def refresh_token(self, a):
        res = yield self.call(ProtoOARefreshTokenReq(refreshToken=a.refresh_token))
        old = a.access_token
        for acc in self.deps.all_accounts:
            if acc.access_token == old:
                acc.access_token = res.accessToken
                acc.refresh_token = res.refreshToken
        self.deps.save_tokens()
        self.storage.event("token_refreshed", label=a.label,
                           expires_in_s=res.expiresIn)
