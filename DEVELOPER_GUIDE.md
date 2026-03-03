# Developer Guide

## Add a New Slash Command
1. Add handler in `bot.py` with `@tree.command(...)`.
2. Keep handler thin:
   - Validate interaction context.
   - Call `PermissionService` and `AbuseService`.
   - Delegate domain logic to relevant service.
3. Persist data via repository-facing service, not raw SQL.
4. Add audit/realtime side effects where relevant.

## Add a New Feature Module
1. Create file in `modules/` with `VERSION` and callable hooks.
2. Ensure module can be discovered by `ModuleRegistry`.
3. Add config defaults in dashboard defaults factory.
4. Add dashboard UI/template bindings if needed.

## Add a New Service
1. Create `services/<name>_service.py`.
2. Put business rules and orchestration there.
3. Inject repositories/services via constructor.
4. Do not import presentation concerns (Flask request/Discord response formatting).

## Add a New Repository
1. Create `repositories/<name>_repository.py`.
2. Keep repository methods persistence-only.
3. Return simple primitives/dicts/lists.
4. Do not enforce permission/business policy in repository methods.

## Where NOT to Put Logic
- Do not put business rules in:
  - Discord event handlers (`bot.py`) beyond minimal guards.
  - Flask routes (`dashboard.py`) beyond request validation/access checks.
- Do not put SQL directly in handlers/routes for new work.

## Common Mistakes to Avoid
- Duplicating permission hierarchy checks in handlers.
- Skipping cooldown/abuse checks for high-impact commands.
- Returning raw exceptions to clients.
- Storing unvalidated payloads from dashboard directly.
- Introducing circular imports between presentation/service/repository layers.
