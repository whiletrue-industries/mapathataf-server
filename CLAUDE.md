# mapathataf-server

Backend for מפת הטף (mapathataf) — Firebase project `mapathataf`. Python 3.12 Cloud Functions (2nd gen) wrapping a single Flask app, plus a daily data pipeline.

## Layout

- `functions/main.py` — Firebase entry points: `api` (HTTPS, europe-west4, wraps the Flask app), `data_processing` (scheduler, daily 00:00 Israel time), `data_processing_s` (HTTP trigger for the same pipeline). `initialize_app()` runs here before importing `api`.
- `functions/api/__init__.py` — the whole REST API (Flask). Module-level globals: `db = firestore.client()`, `app`, `GOOGLE_MAPS_API_KEY` (SecretParam read at import time — set env var `GOOGLE_MAPS_API_KEY` to anything when importing locally), `logos_cache`.
- `functions/process_data/__init__.py` — pipeline that creates city docs from `city_names.csv` (beware trailing commas) and cluster docs from `eshkol.csv` (`slug,name,city_links` with `;`-separated slugs).
- `functions/venv/` — the venv that matches `functions/requirements.txt` (`functions/venv/bin/python`).
- `functions/tests/` — pytest suite (`conftest.py` stubs the Firestore client at import and fakes the `c/{ws}/items` layout in memory). Install `functions/requirements-test.txt` (minimal, no pipeline stack); CI runs it on PRs via `.github/workflows/tests.yml`.
- `functions/migrate_address_fields.py` — one-off `admin.address` → `geocode_address` migration (dry run by default, `--apply` to write). Run once after deploying the Aug 2026 location-fields change; delete afterwards.

## Firestore data model (single database, all client access denied by rules; admin SDK only)

- Top-level collection `c` (constant `WS`): one doc per city/workspace, doc id = city slug (`slugify_row` in process_data).
  - `key`: uuid string — the admin API key for that workspace (compared against the `Authorization` header).
  - `metadata.city`: Hebrew name.
  - `metadata.logo_url` (optional; set via the manage logo-upload endpoint or manually): logo URL in the `mapathataf.firebasestorage.app` bucket under `logos/`. Values may have stray whitespace — strip before use.
  - `favorite`, `active` (top-level, next to `key`, superadmin-managed booleans; default False when absent). **`active` (AND a non-empty logo_url) is what puts a city in `GET /logos`** — as of Aug 2026: dymonh, khyph, rht, rkhobot, sorek-dromi.
  - `metadata.city_links` (only on cluster/eshkol docs like `sorek-dromi`): list of member city slugs; `GET /<ws>/items` aggregates items across them.
  - `metadata.bounds`, `metadata.neighborhoods`, `metadata.links` (optional, per-city).
- Subcollection `c/{slug}/items`: facility items with `key`, `info`, `official`, `user`, `admin` maps. Item-level flags: `admin.app_publication is False` hides from public, `admin._private_deleted` = soft delete, `_private_`-prefixed keys are stripped for non-admin (`sanitize_metadata`).

## API conventions (`functions/api/__init__.py`)

- Auth: `authenticate(workspace, key, roles)` — "admin" requires the workspace doc's `key`; "view" is public (no key needed). A `Bearer <Firebase id-token>` Authorization header that verifies (`auth.verify_id_token`) AND whose email is in the `settings/superadmins` doc (`{emails: [...]}`, lowercase) yields privilege 5 (lenient: bad Bearer degrades to public on view routes). Privileges: 5 superadmin / 4 admin / 3 item-key / 0 public.
- `/manage/*` routes (superadmin-only via `authenticate_superadmin()`, strict 401/403): `GET /manage/workspaces` (all docs incl. `key` + flags), `PUT /manage/workspaces/<ws>` (merge-update: dotted `metadata.<k>` paths, `null` deletes a key, `favorite`/`active` bools; body keys other than metadata/favorite/active → 400), `POST /manage/workspaces/<ws>/logo` (multipart field `logo`, png/jpeg/svg/webp ≤2MB → bucket `logos/<slug>-<uuid8>.<ext>`, updates `metadata.logo_url`, busts `logos_cache`).
- Routes are `/<workspace>`-rooted; static routes like `/logos` and `/manage/...` are safe (Werkzeug prefers static rules over converters).
- An `after_request` hook forces no-cache headers on **all** responses.
- `GET /logos` — public, no auth; returns `[{id, city, logo_url}]` for docs in `c` with top-level `active` truthy and a non-empty `metadata.logo_url`; cached in memory per instance for 1 day (`logos_cache`, `LOGOS_CACHE_TTL`) — writes bust only the handling instance.
- Handlers return `(dict_or_list, status)` tuples; queries stream whole collections and filter in Python (no composite indexes; `firestore.indexes.json` is empty).
- Location fields (Aug 2026): admin PUTs with `geocode_address` (address or plus code; admin-only trigger) get geocoded via Google into `admin.lat/lng/formatted_address` + `_private_geocoding_status`/`_private_geocoded_input`; short plus codes get the workspace city appended; plus-code inputs bypass the ROOFTOP/RANGE_INTERPOLATED gate. Failed geocodes clear stale coords; empty `geocode_address` clears all geocode fields. `admin.display_address` is display-only, never geocoded. Item-key holders may WRITE `_private_` fields (owner form relies on it); sanitization is read-side only.

## Deploy & test

- Deploy: `npx firebase-tools deploy --only functions:api` (project alias `default` → `mapathataf`).
- Live URLs: `https://api-m5crpfzdeq-ez.a.run.app/...` or `https://europe-west4-mapathataf.cloudfunctions.net/api/...`.
- If local gcloud ADC cannot read mapathataf Firestore (e.g. 403 / wrong quota project), ask the user to log in with the correct credentials (`gcloud auth application-default login`) rather than working around it. Meanwhile the Firebase MCP tools can inspect Firestore, and Flask routing/logic can still be tested locally via `app.test_client()` (import with `GOOGLE_MAPS_API_KEY=dummy`).
