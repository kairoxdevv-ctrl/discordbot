# DiscordBot Platform

Production-style Discord bot with dashboard, multi-server support, and live support case handling.

## Features

- Discord bot with modular systems
  - Welcome
  - Autorole
  - Automod
  - Logging
  - Custom commands
  - Moderation tools
  - Reaction roles
  - Leveling
- Web dashboard (OAuth with Discord)
- Multi-server (per-guild config)
- Support system
  - Customer portal (`/support`)
  - Support admin inbox (`/dashboard/support`)
  - Live case updates and case threads
  - SLA tracking and urgent alert webhook support

## Tech Stack

- Python 3.10+
- discord.py
- Flask + Waitress
- SQLite
- WebSockets

## Project Structure

- `bot.py` -> Discord bot runtime
- `dashboard.py` -> Flask dashboard + support APIs
- `core/` -> config, storage, metrics, realtime, security
- `modules/` -> bot feature modules
- `templates/` -> Jinja templates
- `static/` -> frontend JS/CSS

## Environment

Create `.env` with at least:

- `DISCORD_TOKEN`
- `CLIENT_ID`
- `CLIENT_SECRET`
- `REDIRECT_URI`
- `SECRET_KEY`
- `OWNER_ID`

Optional:

- `SUPPORT_ADMIN_IDS` (comma-separated Discord user IDs)
- `SUPPORT_ALERT_WEBHOOK_URL`
- `GITHUB_TOKEN` (for autosync script)

## Run

```bash
cd /root/discordbot
./venv/bin/python bot.py
```

## Main URLs

- Dashboard: `/dashboard`
- Support portal: `/support`
- Support inbox (support admin): `/dashboard/support`
- Owner admin: `/admin`

## Notes

- Sensitive files are ignored in git (`.env`, `platform.db`, `venv`, logs, etc).
- Per-server runtime config is stored in SQLite and local config files.
