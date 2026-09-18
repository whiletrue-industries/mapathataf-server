"""One-off migration: move hand-entered Tipat Halav items onto the document ids of
their official records, so the daily import merges into them instead of adding a
duplicate next to each one.

The pairing below was done by hand (names + addresses; coordinates are missing or
wrong for several of them), against the file as of 2026-09-18. Not paired, on purpose:
  - rht/46a81316d62c "טיפת חלב ג'" - the file has no Rahat ג' station
  - mzkrt-btyh/2c14ea1a9584, rkhobot/15e167952a30 - soft-deleted, no counterpart

The moved document keeps everything the admins and owners entered (admin, user, key);
the next data_processing run then fills in info + official. If the import got there
first and already created the official document, the hand-entered admin/user/key are
folded into it - unless someone has edited that document too, which is reported and
left alone.

Item ids change, so existing links to these items (share / owner-form links) break.

Run locally with admin credentials (dry run by default):

    GOOGLE_APPLICATION_CREDENTIALS=<service-account.json> \\
        python migrate_tipat_halav_ids.py [--apply]

Delete this script once it has been applied.
"""
import argparse

import firebase_admin
from firebase_admin import firestore

import tipat_halav

WS = 'c'
ITEMS = 'items'

# (workspace, hand-entered item id, Tipat Halav station id)
RENAMES = [
    ('bar-tobyh', '0ebaf9b3e710', '5103000042'),    # באר טוביה קופת חולים כללית -> מרכז בריאות טל
    ('dymonh', 'f49d29885b20', '5120000117'),       # דימונה א'
    ('dymonh', '7abfb29141e9', '5120000118'),       # דימונה ה'
    ('khyph', 'd3adc83435c2', '5120000156'),        # גרנד קניון (temporarily closed: not imported for now)
    ('mzkrt-btyh', '5e49b7b13631', '5120000247'),   # מזכרת בתיה
    ('qryyt-mlaky', '75db9c602e93', '5103000352'),  # כללית
    ('qryyt-mlaky', 'e37bc01613bf', '5114000054'),  # מכבי
    ('qryyt-mlaky', 'ea4e093ab863', '5111000024'),  # לאומית
    ('rht', '41e7f9cdc859', '5120000388'),          # רהט א'
    ('rht', 'f7ab2f8c8400', '5120000389'),          # רהט ב'
    ('rht', 'a67d07c267c9', '5120000392'),          # רהט ה'
    ('rkhobot', '79bc428ff8c0', '5120000395'),      # אהרוני
    ('rkhobot', '5dd6d7c1c2dc', '5120000396'),      # אושיות
    ('rkhobot', 'd8a8112e6276', '5120000397'),      # בתיה מקוב
    ('rkhobot', '75b983882063', '5120000398'),      # פרשני
    ('rkhobot', 'fe01af023762', '5120000399'),      # קרית משה
    ('rkhobot', 'f1e9ee3869c0', '5120000400'),      # שעריים
    ('rkhobot', '18de4e39683f', '5103000163'),      # כפר גבירול
]


def migrate(apply):
    firebase_admin.initialize_app()
    db = firestore.client()
    counts = dict(moved=0, folded=0, already_done=0, missing=0, conflict=0)
    for workspace, old_id, station_id in RENAMES:
        _id = tipat_halav.item_id(station_id)
        new_id = tipat_halav.doc_id(_id)
        label = f'{workspace}/{old_id} -> {new_id} ({_id})'
        old_ref = db.collection(WS, workspace, ITEMS).document(old_id)
        new_ref = db.collection(WS, workspace, ITEMS).document(new_id)
        old = old_ref.get().to_dict()
        new = new_ref.get().to_dict()
        if old is None:
            outcome = 'already_done' if new is not None else 'missing'
            counts[outcome] += 1
            print(f'{label}: {outcome}')
            continue
        if new is None:
            item = old
            item['info'] = dict(old.get('info') or {}, _id=_id)
            outcome = 'moved'
        elif new.get('admin') or new.get('user'):
            counts['conflict'] += 1
            print(f'{label}: conflict - the official item has admin/owner edits of its own, left alone')
            continue
        else:
            item = dict(new, key=old['key'], admin=old.get('admin') or {}, user=old.get('user') or {})
            outcome = 'folded'
        counts[outcome] += 1
        name = (item.get('user') or {}).get('name') or (item.get('admin') or {}).get('name')
        print(f'{label}: {outcome} - {name!r}')
        if apply:
            batch = db.batch()
            batch.set(new_ref, item)
            batch.delete(old_ref)
            batch.commit()
    print(('APPLIED' if apply else 'DRY RUN'), counts)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--apply', action='store_true', help='write the changes (default: dry run)')
    migrate(parser.parse_args().apply)
