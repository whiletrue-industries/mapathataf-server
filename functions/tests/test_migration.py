import pytest

from conftest import FakeDB

import migrate_address_fields


@pytest.fixture
def db(monkeypatch):
    fake = FakeDB()
    monkeypatch.setattr(migrate_address_fields.firebase_admin, 'initialize_app', lambda: None)
    monkeypatch.setattr(migrate_address_fields.firestore, 'client', lambda: fake)
    return fake


def test_successful_geocode_is_preserved(db):
    db.workspaces['ws'] = {}
    db.items[('ws', 'a')] = {'admin': {
        'address': 'ויצמן 100',
        'lat': 32.0, 'lng': 34.8, 'formatted_address': 'ויצמן 100, תל אביב',
        '_private_geocoding_status': 'OK',
    }}
    migrate_address_fields.migrate(apply=True)
    admin = db.items[('ws', 'a')]['admin']
    assert 'address' not in admin
    assert admin['geocode_address'] == 'ויצמן 100'
    assert admin['_private_geocoded_input'] == 'ויצמן 100'
    assert admin['_private_geocoding_status'] == 'OK'
    assert admin['lat'] == 32.0


def test_failed_geocode_is_cleared(db):
    db.workspaces['ws'] = {}
    db.items[('ws', 'a')] = {'admin': {
        'address': 'שכונת אל-קסם',
        'lat': 31.0,
        '_private_geocoding_status': 'INACCURATE',
    }}
    migrate_address_fields.migrate(apply=True)
    admin = db.items[('ws', 'a')]['admin']
    assert admin['geocode_address'] == 'שכונת אל-קסם'
    assert admin['_private_geocoding_status'] == 'ZERO_RESULTS'
    assert 'lat' not in admin
    assert 'formatted_address' not in admin


def test_legacy_city_is_dropped(db):
    db.workspaces['ws'] = {}
    db.items[('ws', 'a')] = {'admin': {'city': 'תל אביב', 'name': 'שם'}}
    migrate_address_fields.migrate(apply=True)
    admin = db.items[('ws', 'a')]['admin']
    assert 'city' not in admin
    assert admin['name'] == 'שם'


def test_items_without_address_are_untouched(db):
    db.workspaces['ws'] = {}
    db.items[('ws', 'a')] = {'admin': {'name': 'שם'}, 'info': {'lat': 31.0}}
    before = db.items[('ws', 'a')]
    migrate_address_fields.migrate(apply=True)
    assert db.items[('ws', 'a')] == before


def test_dry_run_does_not_write(db):
    db.workspaces['ws'] = {}
    db.items[('ws', 'a')] = {'admin': {'address': 'ויצמן 100', 'city': 'תל אביב'}}
    migrate_address_fields.migrate(apply=False)
    admin = db.items[('ws', 'a')]['admin']
    assert admin == {'address': 'ויצמן 100', 'city': 'תל אביב'}
