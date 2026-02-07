# Livestock View

Contract-driven UI shell for livestock operations. The single source of truth is `docs/CONTRACT_PACK.json`.

## Quick Start

```bash
npm install
npm run generate:contract
npm run dev
```

## Configuration

Environment variables (client-side):

- `API_URL`: Base URL for REST calls (default: current origin).
- `WS_URL`: WebSocket URL (default: current origin + `/ws`).
- `DATA_ADAPTER`: `MOCK` or `HTTP`.
- `REALTIME_ADAPTER`: `MOCK` or `WS`.

The UI enforces required roles for each operation using contract ACLs. Switch roles in the header to simulate authorization.

## Adapters

- `MockAdapter`: In-memory event log + entity store + WS-like updates.
- `HttpAdapter`: REST client with exact paths/headers from the contract.
- `WsAdapter`: WebSocket client for `/ws` (JWT via query param `token`).

Mock-specific behavior is aligned with contract gates:

- Events are deduped on `(uid, event_at, event_type, event_subtype, source_ref)`; duplicates return existing events.
- `idempotency-key` overrides `source_ref` for `/events` and `/events/bulk`.
- `if-match-version` is required for `PATCH /animals/:uid`; mismatch returns 409 with `currentVersion`.
- Dimensions auto-create when referenced by event/cost codes.

## Contract Drift Check

Generated types embed a hash of `docs/CONTRACT_PACK.json`.

- Regenerate types: `npm run generate:contract`
- Validate drift: `npm run check:contract`

If the hash mismatches, update `docs/CONTRACT_PACK.json` and regenerate types.

## Warnings from Contract

- Costs accept `payload` but it is not persisted by the backend.
- Dimensions CSV export serializes `meta` via `String(value)` and may return `[object Object]`.
- OpenAPI docs endpoints are registered without response schemas.

The UI avoids assuming ideal behavior for these cases.

## Dataset Organization (GIS)

- Canonical dataset catalog: `docs/DATASET_CATALOG.md`
- Machine inventory: `output/_index/DATASET_INDEX.csv`
- Quick explorer shortcuts: `output/_index/open_datasets.ps1`
- Strong-arm map: `output/_index/STRONG_ARMS.md`
- Branch indexes:
  - `output/strong_arms/hydrology`
  - `output/strong_arms/topography`
  - `output/strong_arms/world_imagery`

Rebuild index after running new data pipelines:

```bash
python scripts/rebuild_dataset_index.py
```
