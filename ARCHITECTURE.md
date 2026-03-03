# Architecture

## Layered Structure
- Presentation layer:
  - `bot.py` for Discord events and slash command entrypoints.
  - `dashboard.py` for HTTP routes and API endpoints.
- Service layer:
  - `services/*.py` for business rules, validation orchestration, cooldown policy, permission checks, and realtime orchestration.
- Repository layer:
  - `repositories/*.py` for all persistence operations used by services.
- Core layer:
  - `core/*.py` for infrastructure primitives (storage pool, scheduler engine, ws server, config engine, security utils, metrics).
- Feature module layer:
  - `modules/*.py` for specific bot features (automod, welcome, logging, custom commands, etc).

## Slash Command Flow
1. Discord invokes slash command handler in `bot.py`.
2. Handler performs minimal context checks.
3. Handler calls `PermissionService` and `AbuseService` checks.
4. Handler delegates data operations to `ModerationService` / config/realtime services.
5. Services use repositories for persistence.
6. Response is sent back to Discord interaction.

## Dashboard Request Flow
1. Flask route in `dashboard.py` receives request.
2. Global security gate applies rate limits, origin checks, payload bounds.
3. Route validates auth + CSRF + guild access.
4. Route delegates to services (support/config/realtime).
5. Service calls repositories for DB operations.
6. JSON/template response is returned; errors are handled by global exception middleware.

## Scheduler Job Flow
1. `TaskScheduler` loads pending jobs from `SchedulerRepository`.
2. In-memory heap is protected by async lock.
3. Due job is claimed atomically (`pending -> running`).
4. Handler executes with timeout.
5. Success marks `done`; failures retry with backoff; exhausted retries mark `dead`.

## Custom Command Flow
1. `CustomCommandManager.sync()` reads config and sanitizes commands.
2. Commands are registered to guild-scoped app command tree.
3. Runtime callback enforces per-user custom command cooldown.
4. Cooldown breach returns ephemeral throttle response.

## Service/Repository Interaction
- Services contain business decisions and side effects.
- Repositories only perform persistence operations.
- Presentation never writes raw SQL directly for support/moderation/scheduler flows.

## Realtime System
- `RealtimeBus` handles in-process topic pub/sub.
- `WsAuthManager` issues short-lived, single-use guild-scoped tokens.
- WS server enforces max payload, idle timeout, global/per-guild connection caps.

## Permission System
- `PermissionService` centralizes:
  - moderation hierarchy validation
  - dashboard access validation
  - permission-flag checks
- Command handlers call service methods instead of duplicating role hierarchy logic.

## Abuse System
- `AbuseService` enforces:
  - per-user limits
  - per-guild limits
  - burst detection window
- Internal state is lock-protected and periodically cleaned to prevent memory growth.
