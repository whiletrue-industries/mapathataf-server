import decimal
from firebase_admin import firestore
from pathlib import Path
import dataflows as DF
import json
import csv
import slugify
import hashlib
import uuid
import datetime

csv.field_size_limit(1000000000)

CURRENT_DIR = Path(__file__).parent

def count(city_names):
    counts = dict()
    for city in city_names:
        counts[city] = 0
    missing = set()
    out = open(CURRENT_DIR / 'errors.txt', 'w')
    def func(rows):
        for row in rows:
            city = row['city']
            # print(f"Processing city: {city!r}")
            if city in counts:
                counts[city] += 1
                if len(row['official']) > 6:
                    print(f"Row {row['_id']} has {len(row['official'])} records\n")
                    print(json.dumps(row['official'], indent=2, ensure_ascii=False))
            else:
                missing.add(city)
            yield row
        for city in city_names:
            if counts[city] == 0:
                out.write(f"City {city} has no records\n")
        for city in missing:
            out.write(f"City {city} is not in the list\n")
    return func

def map_names(city_name_map):
    def func(row):
        city = row['city']
        if city in city_name_map:
            row['city'] = city_name_map[city]
    return func

def slugify_row():
    used = dict()
    existing_slugs = set()
    def func(row):
        city = row['city']
        if city not in used:
            slug = slugify.slugify(city.replace('א', 'a').replace('ע', 'a').replace('ו', 'o'))
            assert slug not in existing_slugs, f"Slug {slug} already exists for city {city}"
            used[city] = slug
            existing_slugs.add(slug)
            print(f"SLUG {city} ===> {slug}")
        row['city-slug'] = used[city]
        row['id-slug'] = hashlib.md5(row['_id'].encode()).hexdigest()[:8]
        # print(f"ID SLUG {row['_id']} ===> {row['id-slug']}")
        assert row['id-slug'] not in existing_slugs, f"ID slug {row['id-slug']} already exists for id {row['_id']}!"
        existing_slugs.add(row['id-slug'])
    return DF.Flow(
        DF.add_field('city-slug', 'string'),
        DF.add_field('id-slug', 'string'),
        func
    )

def prepare_item(item):
    if isinstance(item, dict):
        return {k: prepare_item(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [prepare_item(v) for v in item]
    elif isinstance(item, decimal.Decimal):
        return float(item)
    else:
        return item

def load_to_storage():
    db = firestore.client()
    added_cities = set()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    def func(row):
        # print(f"Loading row with ID {row['city-slug']}/{row['id-slug']} to storage")
        ref = db.collection('c', row['city-slug'], 'items').document(row['id-slug'])
        item = dict(
            key=str(uuid.uuid4()),
            info=row,
        )
        item['official'] = item['info'].pop('official', [])
        item['info']['updated_at'] = now
        item = prepare_item(item)
        # ref.set(item)
        if ref.get().exists:
            ref.set(item, merge=['info', 'official'])
        else:
            ref.set(item)
        if row['city-slug'] not in added_cities:
            config = db.collection('c').document(row['city-slug'])
            old = db.collection('c', row['city-slug'], 'config').document('.config')
            if old.get().exists:
                old_rec = old.get().to_dict()
                old.delete()
                config.set(old_rec)
            if not config.get().exists:
                config.set({
                    'key': str(uuid.uuid4()),
                    'metadata': {
                        'city': row['city'],
                    }
                })
            added_cities.add(row['city-slug'])
            # for item in db.collection('c', row['city-slug'], 'items').stream():
            #     item_ref = db.collection('c', row['city-slug'], 'items').document(item.id)
            #     item = item_ref.get().to_dict() 
            #     if not item.get('key'):
            #         item_ref.delete()

    return func

def process_data():
    print("Scraping data...")
    URL = 'https://next.obudget.org/datapackages/facilities/all/datapackage.json'
    city_names = list(csv.DictReader(open(CURRENT_DIR / 'city_names.csv')))
    cities = [row['city'] for row in city_names]
    city_name_map = dict()
    for row in city_names:
        for key in ['option1', 'option2', 'option3']:
            if row.get(key):
                city_name_map[row[key]] = row['city']
    print(f"Number of cities: {len(cities)}")
    ds = DF.Flow(
        DF.load(URL),
        # DF.set_type('records', type='array', transform=json.loads),
        map_names(city_name_map),
        # count(cities),
        DF.filter_rows(lambda row: row['city'] in cities),
        slugify_row(),
        load_to_storage(),
        DF.printer(),
    ).datastream()
    for res in ds.res_iter:
        for i, row in enumerate(res):
            if i % 1000 == 0:
                yield(dict(msg=f"Processed {i} rows from resource"))
            # yield row

if __name__ == '__main__':
    process_data()