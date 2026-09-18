import csv
import io
import json
import re

import api

# Daily public CSV dump of every publicly visible item in the database.
# Mirrors the admin Excel export (section-prefixed columns), minus anything
# the public API would not serve: the rows come out of api.process_item() at
# public privilege, so hidden / soft-deleted items and _private_ fields never
# reach the file.
EXPORT_PATH = 'exports/facilities.csv'
EXPORT_CACHE_CONTROL = 'public, max-age=3600'

LEADING_COLUMNS = ['workspace', 'workspace_city', 'id']
# Same precedence and fields as resolveItem() in the frontend's api.service.ts
RESOLVED_FIELDS = [
    'name', 'phone', 'url', 'email', 'manager_name', 'owner_kind',
    'address', 'display_address', 'formatted_address', 'original_address',
    'license_status', 'license_status_code', 'licensing_not_needed',
    'symbol', 'source', 'lat', 'lng', 'facility_kind', 'facility_sub_kind',
    'age_group', 'mentoring_type', 'school_year', 'subsidized',
    'activity_hours', 'more_details',
]
AGE_GROUP_IDS = ['birth_to_1', '1_to_2', '2_to_3', '3_to_6']
LICENSE_STATUS_CODES = {
    'לא הוגשה בקשה לרישוי': 'did_not_apply',
    'רישיון בתוקף': 'valid',
    'בתהליך רישוי': 'in_progress',
}
# item key and per-item derived values that have no place in an export
SKIPPED_KEYS = {'key', 'symbol_text', 'office', 'facility_kind_editable'}
ENGLISH_KEY_RE = re.compile(r'^[A-Za-z0-9_\- ]+$')
# A few facilities carry a dozen records from the same source; past this many
# they go, as JSON, into a single official_<source>_more_records column
MAX_OFFICIAL_PER_SOURCE = 3
MORE_RECORDS = 'more_records'
OFFICIAL_COLUMN_RE = re.compile(r'^official_([a-z]+)(\d*)_(.+)$')


def resolve(item, field):
    sections = [item.get('user'), item.get('admin'), item.get('info'), *(item.get('official') or [])]
    for section in sections:
        value = (section or {}).get(field)
        if value:
            return value
    return None


def original_address(item):
    for official in item.get('official') or []:
        address = official.get('address')
        if address:
            city = official.get('city')
            return f'{address}, {city}' if city and city not in address else address
    return None


def normalize_age_groups(value):
    if not value:
        return None
    values = value if isinstance(value, list) else [value]
    if 'all_ages' in values:
        return list(AGE_GROUP_IDS)
    return [v for v in values if v in AGE_GROUP_IDS] or None


def resolve_item(item):
    resolved = {field: resolve(item, field) for field in RESOLVED_FIELDS}
    resolved['address'] = resolved['display_address'] or resolved['formatted_address'] or resolved['address']
    resolved['original_address'] = original_address(item)
    resolved['facility_kind'] = resolved['facility_kind'] or 'not-set'
    resolved['mentoring_type'] = resolved['mentoring_type'] or 'not-mentored'
    resolved['age_group'] = normalize_age_groups(resolved['age_group'])
    resolved['subsidized'] = any(o.get('source') == 'mol' for o in item.get('official') or [])
    if resolved['license_status']:
        resolved['license_status_code'] = LICENSE_STATUS_CODES.get(resolved['license_status'])
    elif resolved['licensing_not_needed']:
        resolved['license_status'] = 'מתחת ל-7 ילדים ואינו דורש רישוי'
        resolved['license_status_code'] = 'not_needed'
    else:
        resolved['license_status'] = 'לא ידוע'
        resolved['license_status_code'] = 'none'
    return resolved


def column_name(prefix, key):
    # Headers must be lowercase english snake_case; keys that cannot be are dropped
    if not ENGLISH_KEY_RE.match(key):
        return None
    key = re.sub(r'[^a-z0-9]+', '_', key.lower()).strip('_')
    return f'{prefix}_{key}' if key else None


def cell_value(value):
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, list) and all(not isinstance(v, (dict, list)) for v in value):
        return ', '.join(str(v) for v in value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def public_fields(data):
    for key, value in (data or {}).items():
        if key in SKIPPED_KEYS or key.startswith(api.PRIVATE_KEY) or value is None:
            continue
        # inline base64 uploads (owner photos) run to hundreds of KB per cell
        if isinstance(value, str) and value.startswith('data:'):
            continue
        yield key, value


def add_section(row, prefix, data):
    for key, value in public_fields(data):
        column = column_name(prefix, key)
        if column:
            row[column] = cell_value(value)


def flatten_item(item):
    row = {'id': item['id']}
    add_section(row, 'resolved', resolve_item(item))
    add_section(row, 'admin', item.get('admin'))
    add_section(row, 'owner', item.get('user'))
    add_section(row, 'info', item.get('info'))
    source_counts = {}
    more_records = {}
    for official in item.get('official') or []:
        source = re.sub(r'[^a-z]+', '', str(official.get('source') or '').lower()) or 'unknown'
        source_counts[source] = count = source_counts.get(source, 0) + 1
        if count > MAX_OFFICIAL_PER_SOURCE:
            more_records.setdefault(source, []).append(dict(public_fields(official)))
        else:
            add_section(row, f'official_{source}{count}' if count > 1 else f'official_{source}', official)
    for source, records in more_records.items():
        row[f'official_{source}_{MORE_RECORDS}'] = cell_value(records)
    return row


def official_column_order(column):
    # by source, then record number (the first record is unnumbered), overflow column last
    source, number, key = OFFICIAL_COLUMN_RE.match(column).groups()
    return source, key == MORE_RECORDS, int(number or 1), key


def collect_headers(rows):
    # Deterministic order, so the file's layout doesn't shuffle from day to day
    columns = set().union(*rows) if rows else set()
    resolved = [f'resolved_{field}' for field in RESOLVED_FIELDS]
    headers = [c for c in LEADING_COLUMNS + resolved if c in columns]
    for section in ['admin_', 'owner_', 'info_']:
        headers.extend(sorted(c for c in columns if c.startswith(section)))
    headers.extend(sorted((c for c in columns if c.startswith('official_')), key=official_column_order))
    return headers


def public_rows():
    for ws in api.db.collection(api.WS).stream():
        config = ws.to_dict() or {}
        metadata = config.get('metadata') or {}
        if metadata.get('city_links'):
            # clusters only aggregate their member cities' items
            continue
        for doc in api.db.collection(api.WS, ws.id, api.ITEMS).stream():
            item = api.process_item(dict(**doc.to_dict(), _doc_id=doc.id), api.PRIVILEGE_PUBLIC)
            if item is None:
                continue
            yield dict(workspace=ws.id, workspace_city=metadata.get('city'), **flatten_item(item))


def render_csv(rows, headers):
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()


def export_data():
    rows = sorted(public_rows(), key=lambda row: (row['workspace'], row['id']))
    headers = collect_headers(rows)
    data = render_csv(rows, headers)
    bucket = api.storage.bucket(api.STORAGE_BUCKET)
    blob = bucket.blob(EXPORT_PATH)
    blob.cache_control = EXPORT_CACHE_CONTROL
    blob.upload_from_string(data.encode('utf-8'), content_type='text/csv; charset=utf-8')
    api.make_blob_public(blob)
    return dict(
        rows=len(rows),
        columns=len(headers),
        url=f'https://storage.googleapis.com/{bucket.name}/{blob.name}',
    )
