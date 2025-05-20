from firebase_admin import firestore
from pathlib import Path
import dataflows as DF
import json
import csv
import slugify
import hashlib

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
                if len(row['records']) > 6:
                    print(f"Row {row['_id']} has {len(row['records'])} records\n")
                    print(json.dumps(row['records'], indent=2, ensure_ascii=False))
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

def load_to_storage():
    db = firestore.client()
    added_cities = set()
    def func(row):
        print(f"Loading row with ID {row['city-slug']}/{row['id-slug']} to storage")
        db.collection(row['city-slug']).document(row['id-slug']).set(row, merge=True)
        if row['city-slug'] not in added_cities:
            config = db.collection(row['city-slug']).document('.config')
            if not config.get().exists():
                config.set({
                    'key': str(hashlib.uuid4()),
                    'metadata': {
                        'city': row['city'],
                    }
                })
            added_cities.add(row['city-slug'])

    return func

def process_data():
    print("Scraping data...")
    URL = 'https://next.obudget.org/datapackages/facilities/all/all-facilities.csv?a=2'
    city_names = list(csv.DictReader(open(CURRENT_DIR / 'city_names.csv')))
    cities = [row['city'] for row in city_names]
    city_name_map = dict()
    for row in city_names:
        for key in ['option1', 'option2', 'option3']:
            if row.get(key):
                city_name_map[row[key]] = row['city']
    print(f"Number of cities: {len(cities)}")
    DF.Flow(
        DF.load(URL),
        DF.set_type('records', type='array', transform=json.loads),
        map_names(city_name_map),
        count(cities),
        DF.filter_rows(lambda row: row['city'] in cities),
        slugify_row(),
        load_to_storage(),
        DF.printer(),
    ).process()

if __name__ == '__main__':
    process_data()