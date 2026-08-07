# mapathataf-server

Backend for מפת הטף (mapathataf) — Firebase project `mapathataf`. Python 3.12 Cloud Functions (2nd gen) wrapping a single Flask app, plus a daily data pipeline.

## Layout

- `functions/main.py` — Firebase entry points: `api` (HTTPS, europe-west4, wraps the Flask app), `data_processing` (scheduler, daily 00:00 Israel time), `data_processing_s` (HTTP trigger for the same pipeline). `initialize_app()` runs here before importing `api`.
- `functions/api/__init__.py` — the whole REST API (Flask). Module-level globals: `db = firestore.client()`, `app`, `GOOGLE_MAPS_API_KEY` (SecretParam read at import time — set env var `GOOGLE_MAPS_API_KEY` to anything when importing locally), `logos_cache`.
- `functions/process_data/__init__.py` — pipeline that creates city docs from `city_names.csv` (beware trailing commas) and cluster docs from `eshkol.csv` (`slug,name,city_links` with `;`-separated slugs).
- `functions/venv/` — the venv that matches `functions/requirements.txt` (`functions/venv/bin/python`).

## Firestore data model (single database, all client access denied by rules; admin SDK only)

- Top-level collection `c` (constant `WS`): one doc per city/workspace, doc id = city slug (`slugify_row` in process_data).
  - `key`: uuid string — the admin API key for that workspace (compared against the `Authorization` header).
  - `metadata.city`: Hebrew name.
  - `metadata.logo_url` (optional, set manually via `PUT /<workspace>`): logo URL in the `mapathataf.firebasestorage.app` bucket under `logos/`. **Presence of `logo_url` is the de-facto "active city" marker** (as of Aug 2026: dymonh, khyph, rht, rkhobot, sorek-dromi). Values may have stray whitespace — strip before use.
  - `metadata.city_links` (only on cluster/eshkol docs like `sorek-dromi`): list of member city slugs; `GET /<ws>/items` aggregates items across them.
  - `metadata.bounds`, `metadata.neighborhoods`, `metadata.links` (optional, per-city).
- Subcollection `c/{slug}/items`: facility items with `key`, `info`, `official`, `user`, `admin` maps. Item-level flags: `admin.app_publication is False` hides from public, `admin._private_deleted` = soft delete, `_private_`-prefixed keys are stripped for non-admin (`sanitize_metadata`).

## API conventions (`functions/api/__init__.py`)

- Auth: `authenticate(workspace, key, roles)` — "admin" requires the workspace doc's `key`; "view" is public (no key needed). Privileges: 4 admin / 3 item-key / 0 public.
- Routes are `/<workspace>`-rooted; static routes like `/logos` are safe (Werkzeug prefers static rules over converters).
- An `after_request` hook forces no-cache headers on **all** responses.
- `GET /logos` — public, no auth; returns `[{id, city, logo_url}]` for all docs in `c` that have `metadata.logo_url`; cached in memory per instance for 1 day (`logos_cache`, `LOGOS_CACHE_TTL`).
- Handlers return `(dict_or_list, status)` tuples; queries stream whole collections and filter in Python (no composite indexes; `firestore.indexes.json` is empty).

## Deploy & test

- Deploy: `npx firebase-tools deploy --only functions:api` (project alias `default` → `mapathataf`).
- Live URLs: `https://api-m5crpfzdeq-ez.a.run.app/...` or `https://europe-west4-mapathataf.cloudfunctions.net/api/...`.
- If local gcloud ADC cannot read mapathataf Firestore (e.g. 403 / wrong quota project), ask the user to log in with the correct credentials (`gcloud auth application-default login`) rather than working around it. Meanwhile the Firebase MCP tools can inspect Firestore, and Flask routing/logic can still be tested locally via `app.test_client()` (import with `GOOGLE_MAPS_API_KEY=dummy`).
