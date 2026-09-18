import decimal
import hashlib

import tipat_halav


def station(**overrides):
    row = dict(
        id='5120000395', name='טיפת חלב אהרוני', folder_num='5114404406', status='פעיל', status_code=1,
        owner='משרד הבריאות', owner_code=1, address='אהרוני ישראל 21, רחובות', address_comments=None,
        city='רחובות', city_code='8400', district='רחובות', region='מחוז מרכז',
        lat=decimal.Decimal('31.9009'), lng=decimal.Decimal('34.8142'),
        phone_numbers=['08-9466544', '08-1111111'], fax='08-9318851', emails=['a@example.com'],
        opening_hours='א-ג 08:00-15:45', notes='לקביעת תור 5400*',
    )
    row.update(overrides)
    return row


def test_row_takes_the_all_facilities_shape():
    row = tipat_halav.coerce_row(station())
    assert row['_id'] == 'tipat-5120000395'
    assert row['city'] == 'רחובות'
    assert row['formatted_address'] == 'אהרוני ישראל 21, רחובות'
    assert (row['lat'], row['lng']) == (decimal.Decimal('31.9009'), decimal.Decimal('34.8142'))
    assert row['geocode_source'] == 'source'
    assert row['facility_kind'] == 'health'
    assert row['facility_sub_kind'] == 'טיפת חלב'
    assert row['age_group'] == ['birth_to_1', '1_to_2', '2_to_3', '3_to_6']
    assert len(row['official']) == 1


def test_official_record_uses_the_field_names_the_app_resolves():
    official = tipat_halav.coerce_row(station())['official'][0]
    assert official['source'] == 'moh'
    assert official['symbol'] == '5120000395'
    assert official['name'] == 'טיפת חלב אהרוני'
    assert official['address'] == 'אהרוני ישראל 21, רחובות'
    assert official['phone'] == '08-9466544'
    assert official['phone_numbers'] == ['08-9466544', '08-1111111']
    assert official['email'] == 'a@example.com'
    assert official['activity_hours'] == 'א-ג 08:00-15:45'
    assert official['more_details'] == 'לקביעת תור 5400*'
    assert official['owner'] == 'משרד הבריאות'
    assert official['status_code'] == 1


def test_empty_values_are_left_out_of_the_official_record():
    official = tipat_halav.coerce_row(station(phone_numbers=[], emails=None, fax='', notes=None))['official'][0]
    assert not {'phone', 'phone_numbers', 'email', 'fax', 'more_details', 'address_comments'} & set(official)


def test_station_without_coordinates_has_no_geocode_source():
    row = tipat_halav.coerce_row(station(lat=None, lng=None))
    assert row['lat'] is None and row['lng'] is None
    assert row['geocode_source'] is None


def test_only_active_stations_are_imported():
    assert tipat_halav.is_active(station())
    assert not tipat_halav.is_active(station(status='תחנה סגורה זמנית', status_code=2))
    assert not tipat_halav.is_active(station(status='סגור', status_code=3))


def test_doc_id_is_the_first_8_hex_chars_of_the_md5():
    assert tipat_halav.doc_id('tipat-5120000395') == '8dda400a'
    # unchanged for the education items already in the database
    assert tipat_halav.doc_id('moe-10393') == hashlib.md5(b'moe-10393').hexdigest()[:8]
