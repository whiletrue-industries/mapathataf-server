import pytest

from conftest import api, ADMIN_KEY, ITEM_KEY, WORKSPACE, ITEM_ID


def admin_put(client, payload):
    return client.put(f'/{WORKSPACE}/{ITEM_ID}', json=payload,
                      headers={'Authorization': ADMIN_KEY})


def stored_admin(db):
    return db.items[(WORKSPACE, ITEM_ID)]['admin']


@pytest.fixture
def geocoder(monkeypatch):
    state = {
        'result': dict(lat=32.0, lng=34.8, formatted_address='ויצמן 100, תל אביב',
                       _private_geocoding_status='OK'),
        'calls': [],
    }

    def fake_geocode(address, city=None):
        state['calls'].append((address, city))
        return dict(state['result'], _private_geocoded_input=address)

    monkeypatch.setattr(api, 'geocode', fake_geocode)
    return state


def test_admin_geocode_address_success(client, db, geocoder):
    response = admin_put(client, {'geocode_address': 'ויצמן 100 תל אביב'})
    assert response.status_code == 200
    admin = stored_admin(db)
    assert admin['geocode_address'] == 'ויצמן 100 תל אביב'
    assert admin['lat'] == 32.0
    assert admin['lng'] == 34.8
    assert admin['formatted_address'] == 'ויצמן 100, תל אביב'
    assert admin['_private_geocoding_status'] == 'OK'
    assert admin['_private_geocoded_input'] == 'ויצמן 100 תל אביב'
    assert geocoder['calls'] == [('ויצמן 100 תל אביב', None)]


def test_short_plus_code_passes_workspace_city(client, db, geocoder):
    admin_put(client, {'geocode_address': 'CWC8+R9'})
    assert geocoder['calls'] == [('CWC8+R9', 'תל אביב')]


def test_failed_geocode_clears_stale_location(client, db, geocoder):
    db.items[(WORKSPACE, ITEM_ID)]['admin'] = {
        'geocode_address': 'כתובת ישנה',
        'lat': 31.0, 'lng': 34.0, 'formatted_address': 'ישן',
        '_private_geocoding_status': 'OK', '_private_geocoded_input': 'כתובת ישנה',
    }
    geocoder['result'] = dict(_private_geocoding_status='ZERO_RESULTS')
    admin_put(client, {'geocode_address': 'כתובת שלא נמצאת'})
    admin = stored_admin(db)
    assert admin['geocode_address'] == 'כתובת שלא נמצאת'
    assert admin['_private_geocoding_status'] == 'ZERO_RESULTS'
    assert 'lat' not in admin
    assert 'lng' not in admin
    assert 'formatted_address' not in admin


def test_clearing_geocode_address_removes_all_geocode_fields(client, db, geocoder):
    db.items[(WORKSPACE, ITEM_ID)]['admin'] = {
        'geocode_address': 'כתובת', 'display_address': 'ליד העירייה',
        'lat': 31.0, 'lng': 34.0, 'formatted_address': 'ישן',
        '_private_geocoding_status': 'OK', '_private_geocoded_input': 'כתובת',
    }
    admin_put(client, {'geocode_address': ''})
    admin = stored_admin(db)
    for field in ('geocode_address', 'lat', 'lng', 'formatted_address',
                  '_private_geocoding_status', '_private_geocoded_input'):
        assert field not in admin
    assert admin['display_address'] == 'ליד העירייה'
    assert geocoder['calls'] == []


def test_item_key_edit_does_not_geocode(client, db, geocoder):
    response = client.put(f'/{WORKSPACE}/{ITEM_ID}?item-key={ITEM_KEY}',
                          json={'geocode_address': 'כתובת', 'phone': '03-1234567'})
    assert response.status_code == 200
    assert geocoder['calls'] == []
    user = db.items[(WORKSPACE, ITEM_ID)]['user']
    assert user['phone'] == '03-1234567'
    assert 'lat' not in user
    assert stored_admin(db) == {}


def test_legacy_address_field_is_not_geocoded(client, db, geocoder):
    admin_put(client, {'address': 'כתובת בשדה הישן'})
    assert geocoder['calls'] == []
    admin = stored_admin(db)
    assert admin['address'] == 'כתובת בשדה הישן'
    assert 'lat' not in admin


def test_display_address_is_stored_without_geocoding(client, db, geocoder):
    admin_put(client, {'display_address': 'ליד בניין העירייה'})
    assert geocoder['calls'] == []
    assert stored_admin(db)['display_address'] == 'ליד בניין העירייה'


def test_public_get_strips_private_fields(client, db):
    db.items[(WORKSPACE, ITEM_ID)]['admin'] = {
        'geocode_address': 'כתובת', '_private_geocoding_status': 'OK',
        '_private_geocoded_input': 'כתובת',
    }
    response = client.get(f'/{WORKSPACE}/{ITEM_ID}')
    admin = response.get_json()['admin']
    assert admin['geocode_address'] == 'כתובת'
    assert '_private_geocoding_status' not in admin
    assert '_private_geocoded_input' not in admin


def test_admin_get_includes_private_fields(client, db):
    db.items[(WORKSPACE, ITEM_ID)]['admin'] = {'_private_geocoding_status': 'OK'}
    response = client.get(f'/{WORKSPACE}/{ITEM_ID}', headers={'Authorization': ADMIN_KEY})
    assert response.get_json()['admin']['_private_geocoding_status'] == 'OK'
