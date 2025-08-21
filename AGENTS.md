# Repository Guidelines

## Project Structure & Module Organization
- Root: solution/workspace files and top-level configs (e.g., `.editorconfig`, `.gitignore`).
- `src/`: application source by project/module. Example: `src/DbScripter/`.
- `tests/`: test projects mirroring `src/` structure. Example: `tests/DbScripter.Tests/`.
- `scripts/`: helper scripts for build/release (PowerShell/Bash). Example: `scripts/dev.ps1`.
- `configs/`: environment or tool configs (e.g., `appsettings.*.json`, `.env`).
- `dist/` or `artifacts/`: build outputs not checked in.

## Build, Test, and Development Commands
Use the repo’s scripts when available; they encapsulate correct flags.
- Build: `dotnet restore && dotnet build --configuration Release`
- Test: `dotnet test --configuration Release --logger trx`
- Run (app): `dotnet run --project src/DbScripter`
- Lint/Format (if configured): `dotnet format` or `pwsh scripts/lint.ps1`
- Local dev loop (example): `pwsh scripts/dev.ps1` (starts watcher or local run)

If the project uses scripts instead of direct CLI, prefer `pwsh scripts/build.ps1` and `pwsh scripts/test.ps1`.

## Coding Style & Naming Conventions
- Indentation: 4 spaces; UTF-8 encoding; end lines with LF.
- C# conventions: PascalCase for types/namespaces, camelCase for locals/fields, ALL_CAPS for constants. One type per file.
- File layout: keep public types in `src/<Project>/` matching the namespace path.
- Formatting: run `dotnet format` before commits. Respect `.editorconfig`.
- SQL/Script assets: keep idempotent scripts in `src/<Project>/Sql/` with descriptive, timestamped names (e.g., `2025-01-12_create_indexes.sql`).

## Testing Guidelines
- Framework: xUnit or NUnit (typical). Mirror production namespaces.
- Naming: `*Tests.cs` and methods `MethodName_Should_ExpectedBehavior`.
- Coverage: target ≥80% for core logic; exclude pure DTOs and Program.cs.
- Run all tests locally via `dotnet test` before PRs.

## Commit & Pull Request Guidelines
- Commits: follow Conventional Commits (e.g., `feat: add table scripting options`). Keep messages imperative and scoped.
- Branches: `feature/<short-description>` or `fix/<issue-id>`.
- PRs: include purpose, linked issues (`Closes #123`), screenshots/logs when behavior changes, and test notes.
- CI green required. Address review feedback with follow-up commits (avoid force-push after review starts).

## Security & Configuration Tips
- Do not commit secrets. Use `dotnet user-secrets` or `.env.local` ignored by Git.
- Parameterize database connections via config; never hardcode.
- Review SQL for safety (idempotency, transactions, no destructive defaults).

