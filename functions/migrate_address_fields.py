"""One-off migration: split admin.address into the new location field model.

- admin.address -> admin.geocode_address
- items with a successful geocode (lat/lng/formatted_address present) keep it,
  marked with _private_geocoded_input so the admin UI shows it as up-to-date
- items with a failed geocode get their stale lat/lng/formatted_address cleared
  and status ZERO_RESULTS, so they surface in the admin UI as needing a fix
- legacy admin.city (written by the old geocoder, unused) is dropped

Run locally with admin credentials (dry run by default):

    GOOGLE_APPLICATION_CREDENTIALS=<service-account.json> \
        python migrate_address_fields.py [--apply]
"""
import argparse

import firebase_admin
from firebase_admin import firestore

WS = 'c'
ITEMS = 'items'
GEOCODE_RESULT_FIELDS = ('lat', 'lng', 'formatted_address')


def migrate(apply):
    firebase_admin.initialize_app()
    db = firestore.client()
    counts = dict(scanned=0, migrated=0, cleared=0, city_dropped=0)
    for ws_doc in db.collection(WS).stream():
        for doc in db.collection(WS, ws_doc.id, ITEMS).stream():
            counts['scanned'] += 1
            item = doc.to_dict() or {}
            admin = item.get('admin') or {}
            changed = False
            if 'address' in admin:
                admin['geocode_address'] = admin.pop('address')
                admin['_private_geocoded_input'] = admin['geocode_address']
                if all(admin.get(field) for field in GEOCODE_RESULT_FIELDS):
                    admin['_private_geocoding_status'] = 'OK'
                else:
                    for field in GEOCODE_RESULT_FIELDS:
                        admin.pop(field, None)
                    admin['_private_geocoding_status'] = 'ZERO_RESULTS'
                    counts['cleared'] += 1
                counts['migrated'] += 1
                changed = True
            if 'city' in admin:
                admin.pop('city')
                counts['city_dropped'] += 1
                changed = True
            if changed:
                print(f"{ws_doc.id}/{doc.id}: geocode_address={admin.get('geocode_address')!r} "
                      f"status={admin.get('_private_geocoding_status')}")
                if apply:
                    doc.reference.update({'admin': admin})
    print(('APPLIED' if apply else 'DRY RUN'), counts)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='write changes (default: dry run)')
    migrate(parser.parse_args().apply)
