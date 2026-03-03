# Setup

## Requirements
- Linux host (recommended)
- Python 3.10+
- SQLite (bundled with Python)
- Discord bot application + OAuth app

## Installation
```bash
cd /root/discordbot
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
```

## Environment Variables
Create/update `.env` (key examples):
- `DISCORD_TOKEN`
- `CLIENT_ID`
- `CLIENT_SECRET`
- `REDIRECT_URI`
- `SECRET_KEY`
- `OWNER_ID`
- `SUPPORT_ADMIN_IDS`
- `LOG_LEVEL`
- `MAX_JSON_BODY_BYTES`
- `MAX_CUSTOM_COMMANDS_PER_GUILD`
- `MAX_CUSTOM_RESPONSE_LEN`
- `COMMAND_RATE_LIMIT_PER_USER`
- `COMMAND_RATE_LIMIT_PER_GUILD`
- `COMMAND_RATE_LIMIT_WINDOW_SEC`
- `COMMAND_BURST_LIMIT`
- `COMMAND_BURST_WINDOW_SEC`

## Run Bot + Dashboard
```bash
cd /root/discordbot
./venv/bin/python bot.py
```
This starts:
- Discord bot runtime
- Dashboard server thread (waitress)
- Internal websocket server

## Database Initialization
No manual migration command is required for current schema:
- Core tables are created by `SQLitePool.init_schema()`.
- Support schema is ensured during dashboard startup.

## Health Check
- Public liveness endpoint: `GET /health`
- Dashboard internal health endpoint: `GET /dashboard/health`

## Production Recommendations
- Run behind reverse proxy (Nginx/Caddy) with TLS.
- Set strict `ALLOWED_ORIGINS`.
- Keep `SESSION_COOKIE_SECURE=true` behavior via HTTPS.
- Configure webhook/logging routes and monitoring.
- Backup `platform.db` regularly.
- Rotate Discord and OAuth secrets periodically.
