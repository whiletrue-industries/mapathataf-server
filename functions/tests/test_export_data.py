import csv
import io
import json
import re

import export_data
from conftest import ITEM_ID, WORKSPACE


def run_export(bucket):
    summary = export_data.export_data()
    blob = bucket.blobs[export_data.EXPORT_PATH]
    rows = list(csv.DictReader(io.StringIO(blob.data.decode('utf-8'))))
    return summary, blob, rows


def add_item(db, item_id, workspace=WORKSPACE, **sections):
    db.items[(workspace, item_id)] = dict(
        dict(key=f'key-{item_id}', info={'_id': item_id}, official=[], admin={}, user={}),
        **sections,
    )


def test_writes_public_csv_to_bucket(db, bucket):
    summary, blob, rows = run_export(bucket)
    assert summary == {
        'rows': 1,
        'columns': len(rows[0]),
        'url': f'https://storage.googleapis.com/{bucket.name}/exports/facilities.csv',
    }
    assert blob.public
    assert blob.content_type == 'text/csv; charset=utf-8'
    assert blob.cache_control == export_data.EXPORT_CACHE_CONTROL
    assert rows[0]['workspace'] == WORKSPACE
    assert rows[0]['workspace_city'] == 'תל אביב'
    assert rows[0]['id'] == ITEM_ID


def test_private_fields_and_keys_never_exported(db, bucket):
    add_item(
        db, 'secretive',
        admin={'name': 'גן', '_private_notes': 'ADMIN-SECRET', '_private_mentor_phone': '050-SECRET'},
        user={'phone': '03-1234567', '_private_email': 'SECRET@example.com'},
        info={'_id': 'secretive', '_private_leak': 'INFO-SECRET'},
        official=[{'source': 'mol', 'name': 'גן', '_private_x': 'OFFICIAL-SECRET', 'key': 'NESTED-KEY'}],
    )
    _, blob, rows = run_export(bucket)
    text = blob.data.decode('utf-8')
    assert 'SECRET' not in text
    assert 'private' not in text
    assert 'key-secretive' not in text and 'NESTED-KEY' not in text
    row = next(r for r in rows if r['id'] == 'secretive')
    assert row['admin_name'] == 'גן'
    assert row['owner_phone'] == '03-1234567'


def test_hidden_and_deleted_items_are_skipped(db, bucket):
    add_item(db, 'unpublished', admin={'app_publication': False})
    add_item(db, 'deleted', admin={'_private_deleted': True})
    add_item(db, 'published', admin={'app_publication': True})
    _, _, rows = run_export(bucket)
    assert [r['id'] for r in rows] == [ITEM_ID, 'published']


def test_covers_all_workspaces_but_not_clusters(db, bucket):
    db.workspaces['other'] = {'key': 'k', 'metadata': {'city': 'חיפה'}}
    db.workspaces['cluster'] = {'key': 'k', 'metadata': {'city': 'אשכול', 'city_links': [WORKSPACE, 'other']}}
    add_item(db, 'a', workspace='other')
    add_item(db, 'stray', workspace='cluster')
    _, _, rows = run_export(bucket)
    assert [(r['workspace'], r['workspace_city'], r['id']) for r in rows] == [
        ('other', 'חיפה', 'a'),
        (WORKSPACE, 'תל אביב', ITEM_ID),
    ]


def test_headers_are_lowercase_english_snake_case(db, bucket):
    add_item(
        db, 'messy',
        info={'_id': 'messy', 'id-slug': 'x', 'city-slug': 'y', 'Some Key': 1, 'שדה': 'dropped'},
        official=[{'source': 'MOL', 'name': 'א'}, {'source': 'mol', 'name': 'ב'}, {'name': 'ג'}],
    )
    _, _, rows = run_export(bucket)
    headers = list(rows[0])
    assert all(re.fullmatch(r'[a-z][a-z0-9]*(_[a-z0-9]+)*', h) for h in headers), headers
    assert {'info_id', 'info_id_slug', 'info_city_slug', 'info_some_key'} <= set(headers)
    assert {'official_mol_name', 'official_mol2_name', 'official_unknown_name'} <= set(headers)
    assert 'dropped' not in ''.join(next(r for r in rows if r['id'] == 'messy').values())


def test_header_order_is_deterministic(db, bucket):
    add_item(db, 'full', admin={'url': 'u', 'email': 'e'}, user={'name': 'n'},
             official=[{'source': 'welfare', 'symbol': '1'}, {'source': 'mol', 'symbol': '2'}])
    _, _, rows = run_export(bucket)
    headers = list(rows[0])
    assert headers[:3] == ['workspace', 'workspace_city', 'id']
    groups = [re.match(r'(resolved|admin|owner|info|official)_', h).group(1) for h in headers[3:]]
    assert groups == sorted(groups, key=['resolved', 'admin', 'owner', 'info', 'official'].index)
    for group in ('admin', 'owner', 'info'):
        columns = [h for h in headers if h.startswith(group + '_')]
        assert columns == sorted(columns)
    assert [h for h in headers if h.startswith('official_')] == [
        'official_mol_source', 'official_mol_symbol', 'official_welfare_source', 'official_welfare_symbol']


def test_official_records_past_the_cap_go_to_a_json_column(db, bucket):
    add_item(db, 'many', official=[
        {'source': 'mol', 'name': f'רשומה {i}', '_private_x': 'SECRET', 'phone': None} for i in range(1, 6)])
    _, blob, rows = run_export(bucket)
    assert [h for h in rows[0] if h.startswith('official_')] == [
        'official_mol_name', 'official_mol_source',
        'official_mol2_name', 'official_mol2_source',
        'official_mol3_name', 'official_mol3_source',
        'official_mol_more_records',
    ]
    row = next(r for r in rows if r['id'] == 'many')
    assert row['official_mol3_name'] == 'רשומה 3'
    assert json.loads(row['official_mol_more_records']) == [
        {'source': 'mol', 'name': 'רשומה 4'}, {'source': 'mol', 'name': 'רשומה 5'}]
    assert 'SECRET' not in blob.data.decode('utf-8')


def test_inline_base64_values_are_dropped(db, bucket):
    add_item(db, 'photo', user={'photo': 'data:image/jpeg;base64,' + 'A' * 5000, 'url': 'https://example.com/a.jpg'})
    _, blob, rows = run_export(bucket)
    assert 'base64' not in blob.data.decode('utf-8')
    assert next(r for r in rows if r['id'] == 'photo')['owner_url'] == 'https://example.com/a.jpg'


def test_resolved_values_follow_frontend_precedence(db, bucket):
    add_item(
        db, 'resolved',
        user={'name': 'שם בעלים', 'age_group': 'all_ages'},
        admin={'name': 'שם רשות', 'display_address': 'רחוב 1', 'formatted_address': 'Geocoded 1',
               'licensing_not_needed': True},
        info={'_id': 'resolved', 'lat': 32.1, 'lng': 34.8, 'facility_kind': 'daycare'},
        official=[{'source': 'mol', 'name': 'שם רשמי', 'address': 'הרצל 5', 'city': 'תל אביב', 'symbol': '123'}],
    )
    _, _, rows = run_export(bucket)
    row = next(r for r in rows if r['id'] == 'resolved')
    assert row['resolved_name'] == 'שם בעלים'
    assert row['resolved_address'] == 'רחוב 1'
    assert row['resolved_original_address'] == 'הרצל 5, תל אביב'
    assert row['resolved_age_group'] == 'birth_to_1, 1_to_2, 2_to_3, 3_to_6'
    assert row['resolved_license_status_code'] == 'not_needed'
    assert row['resolved_subsidized'] == 'true'
    assert row['resolved_mentoring_type'] == 'not-mentored'
    assert (row['resolved_lat'], row['resolved_lng']) == ('32.1', '34.8')
    assert row['resolved_symbol'] == '123'


def test_license_status_codes():
    def code(**official):
        return export_data.resolve_item({'official': [official]})['license_status_code']
    assert code(license_status='רישיון בתוקף') == 'valid'
    assert code(license_status='בתהליך רישוי') == 'in_progress'
    assert code(license_status='לא הוגשה בקשה לרישוי') == 'did_not_apply'
    assert code(license_status='משהו אחר') is None
    assert code() == 'none'


def test_cell_values():
    assert export_data.cell_value(['a', 1]) == 'a, 1'
    assert export_data.cell_value({'a': 'א'}) == '{"a": "א"}'
    assert export_data.cell_value([{'a': 1}]) == '[{"a": 1}]'
    assert export_data.cell_value(False) == 'false'
    assert export_data.cell_value(3.5) == 3.5


def test_empty_database_still_writes_a_file(db, bucket):
    db.items.clear()
    summary, blob, rows = run_export(bucket)
    assert summary['rows'] == 0
    assert rows == []
