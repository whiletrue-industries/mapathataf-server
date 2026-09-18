import pytest

from conftest import FakeDB

import migrate_tipat_halav_ids as migration
import tipat_halav

WS, OLD_ID, STATION_ID = 'rkhobot', '79bc428ff8c0', '5120000395'
NEW_ID = tipat_halav.doc_id(f'tipat-{STATION_ID}')


@pytest.fixture
def db(monkeypatch):
    fake = FakeDB()
    monkeypatch.setattr(migration.firebase_admin, 'initialize_app', lambda: None)
    monkeypatch.setattr(migration.firestore, 'client', lambda: fake)
    monkeypatch.setattr(migration, 'RENAMES', [(WS, OLD_ID, STATION_ID)])
    fake.workspaces[WS] = {}
    fake.items[(WS, OLD_ID)] = {
        'key': 'manual-key',
        'info': {'_id': OLD_ID, 'source': 'admin'},
        'admin': {'name': 'טיפת חלב אהרוני', 'facility_kind': 'health', '_private_notes': 'הערה'},
        'user': {'phone': '08-0000000'},
    }
    return fake


def imported_item():
    return {'key': 'imported-key', 'id': NEW_ID, 'info': {'_id': f'tipat-{STATION_ID}', 'city': 'רחובות'},
            'official': [{'source': 'moh', 'symbol': STATION_ID}]}


def test_dry_run_changes_nothing(db):
    before = dict(db.items)
    migration.migrate(apply=False)
    assert db.items == before


def test_hand_entered_item_moves_to_the_official_doc_id(db):
    migration.migrate(apply=True)
    assert (WS, OLD_ID) not in db.items
    item = db.items[(WS, NEW_ID)]
    assert item['key'] == 'manual-key'
    assert item['admin'] == {'name': 'טיפת חלב אהרוני', 'facility_kind': 'health', '_private_notes': 'הערה'}
    assert item['user'] == {'phone': '08-0000000'}
    assert item['info']['_id'] == f'tipat-{STATION_ID}'


def test_folds_into_an_official_item_the_import_already_created(db):
    db.items[(WS, NEW_ID)] = imported_item()
    migration.migrate(apply=True)
    assert (WS, OLD_ID) not in db.items
    item = db.items[(WS, NEW_ID)]
    assert item['key'] == 'manual-key'
    assert item['admin']['name'] == 'טיפת חלב אהרוני'
    assert item['user'] == {'phone': '08-0000000'}
    assert item['info'] == {'_id': f'tipat-{STATION_ID}', 'city': 'רחובות'}
    assert item['official'] == [{'source': 'moh', 'symbol': STATION_ID}]


def test_official_item_with_its_own_edits_is_left_alone(db):
    db.items[(WS, NEW_ID)] = dict(imported_item(), admin={'name': 'נערך'})
    before = {k: dict(v) for k, v in db.items.items()}
    migration.migrate(apply=True)
    assert db.items == before


def test_rerun_is_a_no_op(db):
    migration.migrate(apply=True)
    after_first = dict(db.items)
    migration.migrate(apply=True)
    assert db.items == after_first


def test_pairing_has_no_repeats():
    renames = migration.RENAMES
    assert len({(ws, old) for ws, old, _ in renames}) == len(renames)
    assert len({station for *_, station in renames}) == len(renames)
