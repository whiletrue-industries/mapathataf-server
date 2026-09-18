"""Coerces rows of the Ministry of Health's Tipat Halav (well-baby clinic) file
into the shape of the all-facilities file, so they load through the same pipeline.

Kept free of the pipeline's heavy imports so the API test suite can cover it.
"""
import hashlib

URL = 'https://next.obudget.org/datapackages/facilities/tipat-halav/datapackage.json'
SOURCE = 'moh'
FACILITY_KIND = 'health'
FACILITY_SUB_KIND = 'טיפת חלב'
# Tipat Halav serves birth to 6, so the stations match every age filter
AGE_GROUPS = ['birth_to_1', '1_to_2', '2_to_3', '3_to_6']
# 1 = פעיל; temporarily (2) and permanently (3) closed stations are not imported
ACTIVE_STATUS_CODE = 1

# official record field <- source column
OFFICIAL_FIELDS = dict(
    symbol='id',
    name='name',
    city='city',
    address='address',
    owner='owner',
    # the two fields below are the ones the app already displays for an item
    activity_hours='opening_hours',
    more_details='notes',
    phone_numbers='phone_numbers',
    address_comments='address_comments',
    fax='fax',
    status='status',
    status_code='status_code',
    folder_num='folder_num',
    district='district',
    region='region',
    city_code='city_code',
    owner_code='owner_code',
)


def item_id(station_id):
    return f'tipat-{station_id}'


def doc_id(_id):
    # Firestore document id of an item, as derived by process_data.slugify_row()
    return hashlib.md5(_id.encode()).hexdigest()[:8]


def is_active(row):
    return row.get('status_code') == ACTIVE_STATUS_CODE


def coerce_row(row):
    phones = row.get('phone_numbers') or []
    emails = row.get('emails') or []
    official = dict(
        source=SOURCE,
        phone=phones[0] if phones else None,
        email=emails[0] if emails else None,
        **{field: row.get(column) for field, column in OFFICIAL_FIELDS.items()},
    )
    located = row.get('lat') is not None and row.get('lng') is not None
    return dict(
        _id=item_id(row['id']),
        formatted_address=row.get('address'),
        city=row.get('city'),
        geocode_source='source' if located else None,
        lat=row.get('lat'),
        lng=row.get('lng'),
        official=[{k: v for k, v in official.items() if v not in (None, '', [])}],
        facility_kind=FACILITY_KIND,
        facility_sub_kind=FACILITY_SUB_KIND,
        age_group=list(AGE_GROUPS),
    )
