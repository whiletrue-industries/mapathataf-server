from unittest import mock

import pytest
import requests

from conftest import api


def google_response(status='OK', location_type='ROOFTOP', lat=32.07, lng=34.79,
                    formatted='ויצמן 100, תל אביב'):
    return {
        'status': status,
        'results': [{
            'geometry': {
                'location_type': location_type,
                'location': {'lat': lat, 'lng': lng},
            },
            'formatted_address': formatted,
            'address_components': [
                {'types': ['locality', 'political'], 'long_name': 'תל אביב'},
            ],
        }] if status == 'OK' else [],
    }


@pytest.fixture
def gmaps(monkeypatch):
    state = {'response': google_response(), 'calls': [], 'raise': None}

    def fake_get(url, params=None):
        if state['raise']:
            raise state['raise']
        state['calls'].append(params)
        response = mock.Mock()
        response.json.return_value = state['response']
        return response

    monkeypatch.setattr(api.requests, 'get', fake_get)
    return state


def test_rooftop_address_ok(gmaps):
    update = api.geocode('ויצמן 100 תל אביב')
    assert update['_private_geocoding_status'] == 'OK'
    assert update['lat'] == 32.07
    assert update['lng'] == 34.79
    assert update['formatted_address'] == 'ויצמן 100, תל אביב'
    assert update['_private_geocoded_input'] == 'ויצמן 100 תל אביב'
    assert 'city' not in update


def test_inaccurate_result_has_no_coordinates(gmaps):
    gmaps['response'] = google_response(location_type='GEOMETRIC_CENTER')
    update = api.geocode('שכונת אל-קסם')
    assert update['_private_geocoding_status'] == 'INACCURATE'
    assert 'lat' not in update
    assert 'formatted_address' not in update


def test_global_plus_code_bypasses_accuracy_gate(gmaps):
    gmaps['response'] = google_response(location_type='GEOMETRIC_CENTER')
    update = api.geocode('849VCWC8+R9')
    assert update['_private_geocoding_status'] == 'OK'
    assert update['lat'] == 32.07


def test_short_plus_code_with_locality_bypasses_accuracy_gate(gmaps):
    gmaps['response'] = google_response(location_type='GEOMETRIC_CENTER')
    update = api.geocode('CWC8+R9 אום אל-פחם')
    assert update['_private_geocoding_status'] == 'OK'


def test_short_plus_code_gets_city_appended(gmaps):
    api.geocode('CWC8+R9', city='אום אל-פחם')
    assert gmaps['calls'][0]['address'] == 'CWC8+R9 אום אל-פחם'


def test_short_plus_code_records_original_input(gmaps):
    update = api.geocode('CWC8+R9', city='אום אל-פחם')
    assert update['_private_geocoded_input'] == 'CWC8+R9'


def test_full_address_is_not_modified_by_city(gmaps):
    api.geocode('ויצמן 100 תל אביב', city='רחובות')
    assert gmaps['calls'][0]['address'] == 'ויצמן 100 תל אביב'


def test_zero_results(gmaps):
    gmaps['response'] = google_response(status='ZERO_RESULTS')
    update = api.geocode('לא קיים')
    assert update['_private_geocoding_status'] == 'ZERO_RESULTS'
    assert 'lat' not in update


def test_api_error_status(gmaps):
    gmaps['response'] = {'status': 'REQUEST_DENIED', 'results': []}
    update = api.geocode('כתובת')
    assert update['_private_geocoding_status'] == 'ERROR'


def test_network_error(gmaps):
    gmaps['raise'] = requests.RequestException('boom')
    update = api.geocode('כתובת')
    assert update['_private_geocoding_status'] == 'ERROR'
    assert update['_private_geocoded_input'] == 'כתובת'
