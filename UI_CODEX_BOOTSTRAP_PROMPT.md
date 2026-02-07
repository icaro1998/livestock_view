You are Codex in a UI-only repo. Your single source of truth is `docs/CONTRACT_PACK.json`. Read it first and treat it as authoritative. Do not invent fields, endpoints, roles, headers, or schema keys. If anything is missing or ambiguous in the contract, surface it explicitly and do not guess.

Recommended stack (unless the repo already enforces another):
- Vite + React + TypeScript
- Zustand for state (event-sourcing friendly)
- TanStack Query for async fetch caching
- Zod optional for client-side validation, but types must be generated from the contract JSON

Non-negotiables:
- Generate strict TypeScript types from `docs/CONTRACT_PACK.json`. Do not handwrite types unless absolutely necessary. Use a generator script (preferred) or `resolveJsonModule` + `as const` + `typeof` so the types are derived directly from the JSON.
- Implement adapter boundaries exactly:
  `IDataAdapter`: list/get/create/update for animals/events/dimensions/costs/analytics.
  `IRealtimeAdapter`: connect/subscribe/unsubscribe and dispatch messages into state.
  `MockAdapter`: in-memory event log + derived state + emits WS-like updates locally.
  `HttpAdapter`/`WsAdapter`: stubs with correct method signatures and URL paths based on the contract.
- Encode ALL gates and invariants from the contract: required roles per operation, `idempotency-key` header usage, `if-match-version` header usage, event dedupe semantics, and dimension auto-create behavior.
- Provide wiring instructions (env vars `API_URL`, `WS_URL`; how to switch adapters; how to validate contract drift and regenerate types).
- Read and honor `meta.warnings` from the contract. If the contract flags a caveat, the UI must not assume the ideal behavior.

Contract highlights you must use (exact values from `docs/CONTRACT_PACK.json`):

REST endpoints and roles:
- `GET /animals` (role: viewer)
- `GET /animals/:uid` (role: viewer)
- `POST /animals` (role: admin)
- `PATCH /animals/:uid` (role: manager) requires header `if-match-version`
- `GET /animals/:uid/events` (role: viewer)
- `POST /events` (role: manager) optional header `idempotency-key`
- `POST /events/bulk` (role: manager) optional header `idempotency-key`
- `GET /events` (role: viewer)
- `POST /costs` (role: manager)
- `POST /costs/bulk` (role: manager)
- `GET /costs` (role: viewer)
- `GET /locations` (role: viewer)
- `POST /locations` (role: admin)
- `GET /groups` (role: viewer)
- `POST /groups` (role: admin)
- `GET /parties` (role: viewer)
- `POST /parties` (role: admin)
- `GET /products` (role: viewer)
- `POST /products` (role: admin)
- `GET /dashboards/summary` (role: viewer)
- `GET /animals/:uid/metrics` (role: viewer)

WebSocket:
- Path: `/ws`
- Auth: JWT access token via `Authorization: Bearer <token>` or query param `token`/`accessToken`
- Topics: `animal.updated`, `event.created`, `cost.created`, `dimension.updated`
- Envelope: `{ topic, ts, requestId?, data }`

Exact schema keys you must use:
- Animal create/update fields: `uid`, `eid`, `vid`, `registration_at`, `alert`, `race`, `sex`, `color`, `mother_name`, `father_name`, `brand_mark`, `birth_year`, `birth_month`, `birth_place`, `diagnostic`, `warning`, `notes`
- Event create fields (base): `uid`, `event_at`, `event_type`, `event_subtype`, `source_ref`, `batch_id`, `confidence`, `notes`, `payload`
- Event dimension codes: `location_from_code`, `location_to_code`, `group_code`, `party_code`, `product_code`
- Event subtype fields: `weight_kg`, `method`, `shrink_pct`, `reason`, `distance_km`, `transport_party_code`, `repro_action`, `sire_uid`, `dam_uid`, `result`, `calf_uid`, `gestation_days`, `action`, `diagnosis`, `dose`, `dose_unit`, `withdrawal_days`, `ration_code`, `intake_kg_day`, `supplement_code`
- Cost create fields: `cost_at`, `scope`, `uid`, `group_code`, `location_code`, `category`, `product_code`, `party_code`, `amount`, `currency`, `quantity`, `unit`, `source_ref`, `batch_id`, `notes`, `payload`
- Dimensions create fields: `code`, `name`, `type`, `category`, `unit`, `meta`
When creating dimensions, only send fields listed in `dimensions.field_support_by_table` for that table.

Enums you must implement as literal unions (from the contract):
- `event_type`: `weight`, `movement`, `repro`, `health`, `nutrition`, `inventory`, `management`, `diagnostic`, `environment`
- `cost.scope`: `animal`, `group`, `location`, `ranch`, `batch`
- `cost.category`: `feed`, `health`, `labor`, `transport`, `capex`, `misc`

Serialization notes (from contract):
- BigInt and Decimal values are serialized as strings in API responses. Do not assume numeric types in the UI.

Known quirks from `meta.warnings` you must respect:
- Costs accept `payload` but it is not persisted in the backend.
- Dimensions CSV export serializes `meta` via `String(value)` and may return `[object Object]`.

Idempotency and concurrency gates:
- Events are deduped by unique index on `(uid, event_at, event_type, event_subtype, source_ref)`; duplicates return HTTP 200 with the existing event.
- `idempotency-key` overrides `source_ref` for `/events` and `/events/bulk`.
- `if-match-version` is required for `PATCH /animals/:uid`; mismatch returns 409 with `currentVersion`.
- Dimensions auto-create when missing during event/cost creation; codes are matched exactly as provided.

Implementation plan (prioritized):
1. Phase 1: Types + Mock UI shell. Add `docs/CONTRACT_PACK.json` to the repo, implement a type generation script and commit generated types, build basic layout and navigation for Animals/Events/Costs/Dimensions/Analytics, implement `MockAdapter` with in-memory event log and entity store.
2. Phase 2: REST adapter. Implement `HttpAdapter` with exact REST paths and headers, use contract-derived types for request/response, enforce role and header gates in the UI.
3. Phase 3: WebSocket adapter. Implement `WsAdapter` that connects to `/ws`, subscribes to all four topics, and dispatches messages into state. Example mapping: on `{ topic: "animal.updated", data: { uid, version } }`, refetch that animal or update the entity store and bump its `version`.
4. Phase 4: Hardening. Add error handling consistent with contract error shapes, loading/empty states, and a contract drift check that fails CI if `docs/CONTRACT_PACK.json` changes without regenerating types.

Adapter interfaces (must implement):
- `IDataAdapter` must expose methods for animals list/get/create/update/list events, events list/create/bulk, costs list/create/bulk, dimensions list/create, analytics summary and animal metrics.
- `IRealtimeAdapter` must expose `connect`, `disconnect`, `subscribe(topics)`, `unsubscribe(topics)`, and `onMessage(handler)`.

Store/state shape (event-sourcing aligned):
- Entity store: `animals`, `dimensions` (locations/groups/parties/products), `costs`.
- Event log: optional but recommended, keyed by `event_id` and ordered by `event_at`.
- Selectors: derived metrics (last weight, ADG, last location) based on event log and analytics endpoints.

Wiring instructions (must include in README or equivalent):
- Env vars: `API_URL`, `WS_URL`.
- Adapter switch: `DATA_ADAPTER=MOCK|HTTP` and `REALTIME_ADAPTER=MOCK|WS` (or equivalent config).
- Contract drift check: compare a hash of `docs/CONTRACT_PACK.json` with a hash embedded in the generated types; fail if mismatch.

Do not invent fields. Use only values present in `docs/CONTRACT_PACK.json`.
