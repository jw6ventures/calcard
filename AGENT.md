# CalCard Agent Guide

## Mission
- Act as an implementation-focused Go agent for CalCard.
- Preserve DAV correctness, auth behavior, UI behavior, and PostgreSQL-backed data semantics.
- Prefer small, testable changes over broad refactors.

## Repository Map
- `cmd/server/main.go`: application entrypoint and startup wiring.
- `internal/http`: router, middleware, health endpoints, and top-level HTTP composition.
- `internal/dav`: CalDAV/CardDAV handlers, XML helpers, protocol behavior, and compliance-heavy tests.
- `internal/ui`: server-rendered handlers, templates, and UI-specific helpers.
- `internal/store`: repository interfaces, PostgreSQL implementations, parsing helpers, and storage models.
- `internal/auth`: OAuth, sessions, request context, and DAV auth.
- `internal/config`: environment-driven configuration loading.
- `db.sql`, `deploy/`, `Dockerfile`, `docker-compose.yaml`: schema and deployment assets.

## Agent Workflow
1. Inspect the relevant package and nearby tests before changing code.
2. Match the local pattern already used in that package unless there is a clear defect in the pattern itself.
3. For any bug fix or behavior change, add or update a failing test first.
4. Implement the smallest code change that makes the new test pass.
5. Run `gofmt -w` on each changed Go file.
6. Run `go test ./...` before finishing.
7. Report any untested area, blocked verification, or unresolved risk explicitly.

## Decision Rules
- Default to editing the narrowest package that owns the behavior.
- Prefer extending existing tests over introducing new test scaffolding.
- Prefer package-local helpers over cross-package abstractions.
- Preserve route shapes, auth semantics, config behavior, and DAV resource paths unless the task explicitly requires changing them.
- Do not introduce new dependencies, frameworks, or architecture layers without a strong repo-specific reason.
- Keep unrelated cleanup out of the change unless it is required for correctness or readability.

## TDD Expectations
- TDD is the default workflow in this repository.
- Add regression tests for protocol, parsing, routing, and auth bugs using the exact request, payload, or path shape that failed.
- Prefer table-driven tests when the package already uses them.
- Keep tests close to the code under change.
- Preserve or improve the existing test surface in `internal/dav`, `internal/store`, `internal/ui`, and utility packages.
- If meaningful automated coverage is not possible, explain the gap and the manual verification that would be required.

## Go Coding Rules
- Write self-documenting code first: clear names, simple control flow, small focused functions.
- Return early on errors and keep the happy path easy to scan.
- Accept and propagate `context.Context` where the surrounding code already does.
- Avoid speculative abstractions, hidden control flow, and unnecessary indirection.
- Keep data transformations near their use unless immediate reuse justifies extraction.
- Reuse existing repository, handler, and helper patterns before inventing new ones.

## Comments Rules
- Comments are the exception, not the default.
- Add comments only when they capture intent or constraints that the code alone cannot express clearly.
- Good comment targets:
  - DAV or XML interoperability quirks.
  - Security-sensitive behavior.
  - Non-obvious edge cases.
  - Required Go doc comments for exported declarations.
- Do not add comments that narrate obvious code or compensate for poor naming.

## High-Risk Areas
- DAV discovery, `PROPFIND`, `REPORT`, sync behavior, and collection/resource path handling.
- OAuth session flow, DAV basic auth, and request context assumptions.
- Database-backed repository behavior and parsing logic for iCal/vCard content.
- HTML form method overrides, CSRF-protected flows, and template-backed handlers.

## Done Criteria
- Relevant tests were written or updated first for behavior changes.
- Changed Go files were formatted with `gofmt -w`.
- `go test ./...` passes.
- The change stays scoped to the task.
- New comments exist only where intent or constraints needed explanation.
