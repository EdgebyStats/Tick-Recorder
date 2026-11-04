# Tick Recorder

A production-grade. multi-broker tick recorder built on cTrader Open API + Python Twisted. Handles Millions ticks per day with DST-safe time zone rollover and buffered I/O. Captures the live price feed of one or more cTrader accounts (FTMO free trials, paid challenges, True ECN etc.) byte-for-byte, with daily-rolling files, async I/O, and crash-safe critical event logging.

"Note: This project is actively being built in public. Code reviews, architectural suggestions, and pull requests are highly welcome. Let's build better trading infrastructure together."

📅 Timezone & Timestamp Strategy
This recorder uses a strict, DST-safe timezone strategy to ensure data aligns perfectly with prop firm trading days.

Default Timezone: The default firm_timezone for all accounts is set to Europe/Prague. We intentionally apply the FTMO timezone to True ECN demo accounts (IC Markets, Pepperstone) so that daily file rollovers match the base firm we are comparing against. If you want to record data with a different timezone, you can easily change the firm_timezone field in config.json.

The Nanosecond Safety Net: Even if timezone configuration is overlooked, the raw UTC nanosecond timestamp (recv_ns) is recorded in every row. This allows you to easily separate and reconstruct specific day data during post-processing or backtesting.

The Timestamp Bottleneck: This recorder uses Python's time.time_ns(). Because the Twisted reactor runs in user-space, there is an inherent software jitter of 10–50 milliseconds between when the network card receives the packet and when Python stamps it. We accept this limitation for medium-frequency strategies. Implementing kernel-level hardware timestamping (SO_TIMESTAMPNS) would require forking the cTrader SDK and is reserved for future high-frequency trading (HFT) optimizations.

---

## Quick Start

### 1. Install dependencies

```bash
pip install pydantic twisted ctrader-open-api protobuf tzdata
```
Note: Install tzdata if your host machine is windows. 

### 2. Create a config file

Copy `config.example.json` and fill in your cTrader Open API credentials:

```json
{
  "client_id": "YOUR_OPEN_API_CLIENT_ID",
  "client_secret": "YOUR_OPEN_API_CLIENT_SECRET",
  "data_dir": "ticks",
  "accounts": [
    {
      "label": "free_trial",
      "login": 1234567,
      "host": "demo",
      "firm_timezone": "Europe/Prague",
      "access_token": "ACCESS_TOKEN_FOR_THIS_ACCOUNT",
      "refresh_token": "REFRESH_TOKEN_FOR_THIS_ACCOUNT",
      "symbols": ["EURUSD", "GBPUSD", "XAUUSD", "US30.cash"]
    }
  ]
}
```

Get your `client_id` / `client_secret` by registering an Open API application
at https://openapi.ctrader.com/ — approval usually takes 1-3 business days.

Get per-account `access_token` / `refresh_token` via the cTrader OAuth flow
(see cTrader Open API docs).

### 3. Start recording

```bash
python -m tick_recorder.main record config.json
```

Press Ctrl+C to stop. The shutdown sequence drains all in-memory buffers to
disk and closes sockets cleanly — no data is lost.

### 4. Inspect recorded data

```bash
# Print first 100 frames from a raw .bin file
python -m tick_recorder.main inspect ticks/raw/demo/2026-10-29.bin --limit 100

# Print ALL frames (use with caution on large files)
python -m tick_recorder.main inspect ticks/raw/demo/2026-10-29.bin --limit 0
```

---

## Configuration Reference

The config file is validated by Pydantic on load. Typos produce clear errors
before the recorder starts; the process exits with a message like:

```
invalid config in config.json:
  accounts.1: Field required (access_token)
  accounts.1: Extra inputs are not permitted (acces_token)
```

### Top-level fields

| Field           | Type     | Required | Default       | Description                          |
|-----------------|----------|----------|---------------|--------------------------------------|
| `client_id`     | string   | yes      | —             | Open API application client ID       |
| `client_secret` | string   | yes      | —             | Open API application client secret   |
| `accounts`      | array    | yes      | —             | List of accounts (min 1)             |
| `data_dir`      | string   | no       | `ticks`       | Where to write recorded data         |

### Per-account fields

| Field           | Type     | Required | Default | Description                              |
|-----------------|----------|----------|---------|------------------------------------------|
| `label`         | string   | yes      | —       | Unique identifier (no slashes/spaces)   |
| `login`         | integer  | yes      | —       | cTrader login (traderLogin)             |
| `host`          | string   | no       | `demo`  | `demo` or `live` (case-insensitive)     |
| `access_token`  | string   | yes      | —       | OAuth access token for this account     |
| `refresh_token` | string   | no       | `""`    | OAuth refresh token (for auto-renewal)  |
| `symbols`       | array    | yes      | —       | List of symbol names (min 1)            |

### Inline documentation fields

Any field starting with `_` is silently stripped before validation. This
lets you add comments to your config:

```json
{
  "label": "paid_challenge",
  "_comment": "host must be exactly 'demo' or 'live'",
  ...
}
```

---

## Output Directory Layout

```
<data_dir>/
├── raw/
│   ├── demo/
│   │   └── 2026-10-29.bin          # Every server frame, byte-for-byte
│   └── live/
│       └── 2026-10-29.bin
├── spots/
│   ├── free_trial/
│   │   └── 2026-10-29.csv          # One row per ProtoOASpotEvent
│   └── paid_challenge/
│       └── 2026-10-29.csv
├── events/
│   └── 2026-10-29.jsonl            # All events (routine + critical), chronological
├── meta/
│   ├── free_trial/
│   │   └── 2026-10-29T12-34-56Z_session.json  # Snapshot per (re)connect
│   └── paid_challenge/
│       └── 2026-10-29T12-34-56Z_session.json
└── critical_events.jsonl           # Critical events only (sync writes, durable)
```

All files roll over daily at 00:00 UTC. Days are determined by the receive
timestamp of the data, not by file-creation time — keep NTP synced on the
host machine.

### File formats

**`raw/*.bin`** — binary, packed records:

```
int64  recv_ns      (receive time in nanoseconds since epoch)
uint32 len          (frame length in bytes)
bytes  frame        (raw protobuf frame, exactly len bytes)
... (repeat)
```

**`spots/*.csv`** — text, one row per spot event:

```
recv_ns, server_ts_ms, symbol_id, symbol, bid, ask, session_close
```

**`events/*.jsonl`** and **`critical_events.jsonl`** — one JSON object per line:

```json
{"utc": "2026-10-29T12:34:56.789Z", "event": "subscribed", "label": "free_trial", "symbols": ["EURUSD", "GBPUSD"]}
```

Critical events (server errors, account setup failures, disconnects, etc.)
are written synchronously to `critical_events.jsonl` so they survive even a
hard crash. Routine events are buffered in RAM and flushed once per second
to `events/<date>.jsonl`.

---

## Architecture

```
tick_recorder/
├── main.py                          # CLI entry point
├── config/
│   ├── schema.py                    # Pydantic models (RecorderConfig, AccountConfig)
│   └── loader.py                    # load_config() + save_config() (atomic, with .bak)
├── core/
│   ├── account.py                   # Account dataclass + from_config()
│   ├── session_deps.py              # SessionDeps (injection contract)
│   └── retry_policy.py              # RetryPolicy + ApiError
├── storage/
│   ├── buffered_file.py             # BufferedDailyFile (RAM-buffered, daily-rolling)
│   ├── storage.py                   # Storage (single I/O gateway)
│   └── constants.py                 # Timeouts, buffer sizes, file headers
├── network/
│   ├── protocol.py                  # RecordingProtocol (TcpProtocol subclass)
│   └── host_session/
│       ├── session.py               # HostSession — lifecycle + call()
│       ├── handlers.py              # MessageHandlerMixin — dispatch + 5 handlers
│       ├── setup.py                 # AccountSetupMixin — auth + symbol subscribe
│       └── auth.py                  # AuthMixin — token refresh
├── recorder/
│   ├── recorder.py                  # Recorder — top-level orchestrator
│   └── inspect.py                   # inspect_raw() utility
└── cli/
    ├── parser.py                    # argparse setup
    └── commands.py                  # _run_record, _run_inspect
```

### Key design decisions

- **Twisted, single-threaded reactor.** No locks, no thread
  synchronization. All I/O is non-blocking via `LoopingCall` and
  `inlineCallbacks`.
- **BufferedDailyFile.** Routine writes accumulate in a `bytearray` and
  flush once per second — eliminates per-event disk blocking on the
  reactor.
- **Critical events split.** Rare but important events (errors,
  disconnects) are written synchronously to a separate file so they
  survive hard crashes.
- **Mixin pattern for HostSession.** One class, four files — handlers,
  setup, auth, and lifecycle are kept separate for readability but
  share `self` cleanly via Python's MRO.
- **Generation counter.** Each (re)connection bumps `generation`;
  in-flight setup tasks check it to avoid stomping on a newer
  connection's state.
- **Pydantic v2 config.** Typos are caught at load time with clear
  error messages; `_comment` fields are silently stripped.

---

## Events Reference

The recorder logs these events to `events/<date>.jsonl`:

### Lifecycle events
- `starting` — Recorder.start() called
- `stopping` — shutdown initiated
- `shutdown_complete` — clean shutdown finished

### Connection events
- `connected` — TCP connection to cTrader host established
- `disconnected` — TCP connection lost
- `app_authorised` — Open API application authenticated
- `app_auth_failed` — application auth failed (bad credentials)

### Per-account events
- `account_authorised` — account access token accepted
- `account_setup_failed` — setup raised an exception (will retry)
- `account_disconnected_by_server` — cTrader disconnected this account
- `subscribed` — spot subscription active for this account
- `spots_resumed` — spots flowing again after silence
- `no_spots` — no spots received for `SILENCE_ALERT_MIN` minutes
- `token_invalidated` — access token invalidated by server
- `token_refreshed` — refresh token used to get a new access token

### Other events
- `symbol_not_found` — a configured symbol doesn't exist on this account
- `client_disconnected_by_server` — cTrader forced a client disconnect
- `server_error` — unsolicited error from cTrader
- `session_stop_failed` — exception during shutdown (rare)

Critical events (also written synchronously to `critical_events.jsonl`):
`server_error`, `account_setup_failed`, `app_auth_failed`,
`token_invalidated`, `no_spots`, `disconnected`.

---

## Operational Notes

### VPS requirements
- Linux VPS (Ubuntu 22.04+ recommended)
- 1 GB RAM minimum (cTrader connections use ~200 MB)
- 10 GB disk per month per account (rough estimate for tick recording)
- Location: London or New York (close to cTrader servers, lower latency)

### Time sync
Keep NTP running. Day rollover uses receive-time nanoseconds, so clock
skew will misfile ticks into the wrong daily file.

```bash
sudo systemctl enable systemd-timesyncd
sudo systemctl start systemd-timesyncd
```

### Running as a service (systemd)

Create `/etc/systemd/system/tick-recorder.service`:

```ini
[Unit]
Description=cTrader Tick Recorder
After=network-online.target

[Service]
Type=simple
User=User
WorkingDirectory=/opt/tick-recorder
ExecStart=/usr/bin/python3 -m tick_recorder.main record /opt/tick-recorder/config.json
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable tick-recorder
sudo systemctl start tick-recorder
sudo journalctl -u tick-recorder -f   # follow logs
```

### Backup
The `data_dir` accumulates daily files. Back it up regularly. Each day's
files are independent — easy to rsync to cold storage:

```bash
rsync -avz /opt/tick-recorder/ticks/ user@backup:/backups/ticks/
```

---

## Troubleshooting

### "login X is not on this access token"
The `login` field in your config doesn't match any account on the
access token. Regenerate the token at https://openapi.ctrader.com/,
making sure to tick the right account on the consent screen.

### "login X is live; set host to 'live' in the config"
You put a live account in the config with `host: "demo"` (or vice versa).
Fix the `host` field.

### `no_spots` events appearing
The account connected and subscribed but no spot events are arriving.
Common causes:
- Markets closed (weekend, holiday)
- Wrong symbol names (check `meta/<label>/*.json` for the actual symbol list)
- cTrader server issue (check https://status.spotware.com/)

### `app_auth_failed`
Wrong `client_id` / `client_secret`. Double-check at
https://openapi.ctrader.com/apps.

### High CPU usage
If you're recording many symbols across many accounts, the 1 Hz flusher
might be doing too much work. Check:
- `BUFFER_MAX_BYTES` in `storage/constants.py` (raise to 4 MB if needed)
- Number of accounts (split across multiple processes if > 10)

---

## Development

### Running tests (when added)

### Adding a new subcommand
1. Add a function to `cli/commands.py` (e.g., `_run_trade(args)`).
2. Add a subparser to `cli/parser.py` (`p_trade = subparsers.add_parser("trade", ...)`).
3. Add a dispatch branch in `main.py`.

### Adding a new event type
1. Call `storage.event("your_event_name", **fields)` from anywhere.
2. If it's critical (errors, unexpected state), add it to `CRITICAL_EVENTS`
   in `storage/storage.py`.

### Extending to a trading bot
The recorder is the foundation. Next layers (planned):
- `trading/` — order submission, position tracking, reconciliation
- `risk/` — FTMO rule enforcement, position sizing, news blackout
- `strategy/` — trend following, mean reversion, statistical arbitrage
- `monitoring/` — alerts, metrics, dashboard publishing

Each layer will follow the same pattern: clean interfaces, no upward
dependencies, independently testable.

---

## License

MIT License.

You are free to use, copy, modify, and distribute this software for any purpose.

However, the following components remain proprietary and are NOT included in this repository:

Strategy engine (trend following, mean reversion, statistical arbitrage)
Risk manager (Prop firm rule enforcement, position sizing)
Backtest framework
Recorded tick data
