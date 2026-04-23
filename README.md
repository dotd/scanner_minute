# Scanner Minute

A minute-resolution stock scanner built on the [Polygon.io](https://polygon.io)
API. The repo contains a Python data/scanning stack, a Node.js dashboard, and
a Docker packaging that lets you run everything in one container (locally or
on AWS).

Two entry points you'll most likely care about:

- `tst/realtime_scanner/tst_scan_realtime.py` — live breakout scanner; posts
  detections, news and minute-bar candles to the Node dashboard (`/`).
- `tst/industries_scanner/tst_industries_scanner.py` — hourly industry ranking;
  writes reports under `data/industries_reports/` and the dashboard serves the
  latest one at `/industries`.

---

## Quick reference

| Thing | Location |
|---|---|
| Python source | `ScannerMinute/` (importable as `ScannerMinute.src.*`) |
| Node dashboard | `node_server/server.js` |
| Scanner scripts | `tst/realtime_scanner/`, `tst/industries_scanner/` |
| Dockerfile | `Dockerfile` |
| Container entrypoint | `docker/entrypoint.sh` |
| Local Docker scripts | `scripts/docker/` |
| AWS deployment scripts | `scripts/aws/` *(stage 2 — coming)* |
| Env / allowlist templates | `docker/templates/` |
| Secrets (gitignored) | `api_keys/` |

---

## 1. Prerequisites

- **macOS / Linux** host
- **Python 3.11+** (the repo has been tested on 3.11 and 3.13)
- **Node.js 20+** (for running the dashboard on the host)
- **Docker 24+** (for the containerised mode)
- A **Polygon.io** API key

Clone:

```bash
git clone https://github.com/dotd/scanner_minute.git
cd scanner_minute
```

---

## 2. Python environment

Create a venv at the project root and install the package in editable mode
plus the runtime deps. The automation scripts expect `./venv/bin/python` to
exist.

```bash
python3 -m venv venv
./venv/bin/pip install --upgrade pip
./venv/bin/pip install -r docker/requirements.txt
./venv/bin/pip install -e .
```

`docker/requirements.txt` is the minimal set actually used by the two
scanners (`polygon-api-client`, `rocksdict`, `requests`, `python-dateutil`,
`pytz`). `pip install -e .` registers the `ScannerMinute` package so the
scripts under `tst/` can `from ScannerMinute.src import ...`.

Put your Polygon key at:

```
api_keys/polygon_api_key.txt      # one line, the key itself
```

`api_keys/` is gitignored and will never be committed.

---

## 3. Node environment (dashboard)

The realtime scanner spawns `node node_server/server.js` on the host. Install
its deps once:

```bash
cd node_server
npm install --omit=dev
cd ..
```

This creates `node_server/node_modules/` (also gitignored). Node 20+ is
required — `passport-google-oauth20` depends on modern crypto.

By default the dashboard runs on `127.0.0.1:3000` **with auth disabled**
(matches the pre-Docker behaviour). To turn auth on, see §6.

---

## 4. Run the scanners locally (no Docker)

```bash
# from repo root, so api_keys/polygon_api_key.txt resolves
./venv/bin/python tst/realtime_scanner/tst_scan_realtime.py
# browser opens at http://localhost:3000/
```

```bash
./venv/bin/python tst/industries_scanner/tst_industries_scanner.py
# first run downloads ~4 years of daily bars for ~500 tickers (slow),
# then writes a report into data/industries_reports/
```

---

## 5. Run everything in Docker (local)

One container runs the Node server + realtime scanner + an hourly loop that
reruns the industries scanner. Useful for verifying the image works before
shipping it to AWS.

Prerequisites:

1. `api_keys/polygon_api_key.txt` (same file used in §4).
2. If you want auth on, copy and fill:
   ```bash
   cp docker/templates/oauth.env.example       api_keys/oauth.env
   cp docker/templates/allowed_emails.txt.example api_keys/allowed_emails.txt
   # edit both files
   ```

Then:

```bash
./scripts/docker/build.sh            # builds scanner-minute:latest
./scripts/docker/run_local.sh        # runs with mounts + OAuth env-file
# visit http://localhost:3000/
```

To smoke-test the image without OAuth creds:

```bash
SKIP_AUTH=1 ./scripts/docker/run_local.sh
```

`run_local.sh` mounts `api_keys/`, `data/`, `logs/`, `ScannerMinute/`, `tst/`,
and `node_server/` into the container, so code changes are picked up on
restart without rebuilding the image. Rebuild only when `Dockerfile` or
`docker/requirements.txt` change.

---

## 6. Google OAuth (optional, required for AWS)

Create an OAuth 2.0 Client ID in Google Cloud Console
(<https://console.cloud.google.com/apis/credentials>):

- Application type: **Web application**
- Authorized redirect URIs:
  - Local: `http://localhost:3000/auth/google/callback`
  - AWS (stage 2): `https://<your-subdomain>.nip.io/auth/google/callback`

Then fill `api_keys/oauth.env`:

```
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
OAUTH_CALLBACK_URL=http://localhost:3000/auth/google/callback
SESSION_SECRET=<random string — python -c "import secrets; print(secrets.token_urlsafe(48))">
```

And `api_keys/allowed_emails.txt` with one Gmail address per line. Only
those emails can log in.

Auth is gated by the `AUTH_ENABLED` env var. The Dockerfile sets it to `1`;
on the host (`node node_server/server.js`) it defaults to `0` so pre-existing
workflows keep working.

---

## 7. AWS deployment (stage 2)

Scripts under `scripts/aws/` are the next milestone. Plan:

1. Build + push image to ECR (account: dotan.dicastro@gmail.com).
2. Provision an EC2 + Elastic IP, security group open on 80/443.
3. Run Caddy in front of the Node server for automatic HTTPS via Let's
   Encrypt, on a free `*.nip.io` subdomain pointing at the Elastic IP.
4. Add the Elastic-IP-based redirect URI to the Google OAuth client.

Not written yet; see task tracker.

---

## 8. Layout

```
scanner_minute/
├── Dockerfile
├── docker/
│   ├── entrypoint.sh           # supervisor: node + realtime + hourly industries
│   ├── requirements.txt        # minimal Python deps for the image
│   └── templates/              # OAuth + allowlist templates (committed)
├── node_server/
│   ├── package.json
│   └── server.js               # dashboard, /industries, Google OAuth
├── ScannerMinute/
│   └── src/                    # importable Python utilities
├── tst/
│   ├── realtime_scanner/tst_scan_realtime.py
│   └── industries_scanner/tst_industries_scanner.py
├── scripts/
│   ├── docker/                 # build + run_local
│   └── aws/                    # (stage 2)
├── api_keys/                   # gitignored — Polygon key, OAuth env, allowlist
└── data/                       # gitignored — RocksDict snapshots, reports
```
