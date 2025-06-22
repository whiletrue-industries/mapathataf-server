import json
from firebase_functions.params import SecretParam
from firebase_admin import firestore
import flask
import uuid
from itertools import islice
import datetime
import requests

db = firestore.client()
app = flask.Flask(__name__)

PRIVILEGE_ADMIN = 4
PRIVILEGE_PRIVATE_KEY = 3
PRIVILEGE_PUBLIC = 0

WS = 'c'
ITEMS = 'items'
PRIVATE_KEY = '_private_'

GOOGLE_MAPS_API_KEY = SecretParam("GOOGLE_MAPS_API_KEY").value.strip()


def geocode(address):
    url = f'https://maps.googleapis.com/maps/api/geocode/json'
    params = {
        'address': address,
        'key': GOOGLE_MAPS_API_KEY,
        'language': 'iw',
        'components': 'country:IL',
    }
    response = requests.get(url, params=params)
    result = response.json()
    update = dict(
        _private_geocoding_status='INITIAL'
    )
    if result['status'] == 'OK':
        result = result['results'][0]
        accuracy = result['geometry']['location_type']
        if accuracy in {'ROOFTOP', 'RANGE_INTERPOLATED'}:
            location = result['geometry']['location']
            update.update(dict(
                lat=location['lat'],
                lng=location['lng'],
                formatted_address=result['formatted_address'],
                _private_geocoding_status='OK',
            ))
        else:
            update.update(dict(
                _private_geocoding_status='INACCURATE',
            ))
        for component in result['address_components']:
            if 'locality' in component['types']:
                update.update(dict(
                    city=component['long_name'],
                ))
    return update

# Helper functions for authentication and utility
# def generate_keys():
#     return {
#         "admin": str(uuid.uuid4()),
#         "collaborate": str(uuid.uuid4()),
#         "view": str(uuid.uuid4())
#     }

def authenticate(workspace, key, required_roles):
    config_ref = db.collection(WS).document(workspace)
    config = config_ref.get().to_dict()
    if not config:
        flask.abort(404, "Workspace not found")
    if "admin" in required_roles and key == config["key"]:
        return PRIVILEGE_ADMIN
    if "view" in required_roles:
        return PRIVILEGE_PUBLIC
    flask.abort(403, "Unauthorized")

def sanitize_metadata(metadata, exclude_private=True):
    if metadata and exclude_private:
        return {k: v for k, v in metadata.items() if not k.startswith(PRIVATE_KEY)}
    return metadata

# Endpoints
# @app.post("/")
# def create_workspace():
#     metadata = flask.request.json
#     workspace_id = str(uuid.uuid4())
#     keys = generate_keys()
#     config = {
#         "metadata": metadata,
#         "keys": keys,
#         "config": {"collaborate": False, "public": False}
#     }
#     db.collection(WS, workspace_id).document(".config").set(config)
#     return {"workspace_id": workspace_id, "config": config}, 201

@app.post("/<workspace>")
def create_item(workspace):
    key = flask.request.headers.get("Authorization")
    authenticate(workspace, key, ["admin"])
    metadata = flask.request.json
    item_id = str(uuid.uuid4()).split("-")[-1]
    item_key = str(uuid.uuid4())
    item = {"key": item_key, 'info': {'_id': item_id, 'source': 'admin'}, 'user': {}, 'admin': metadata}
    db.collection(WS, workspace, ITEMS).document(item_id).set(item)
    return {"id": item_id, **item}, 201

@app.get("/<workspace>")
def get_workspace(workspace):
    key = flask.request.headers.get("Authorization")
    privilege = authenticate(workspace, key, ["admin", "view"])
    config_ref = db.collection(WS).document(workspace)
    config = config_ref.get().to_dict()
    ret = sanitize_metadata(config["metadata"], privilege < PRIVILEGE_ADMIN)
    ret['_p'] = privilege
    return ret, 200

def process_item(item, privilege):
    if privilege > PRIVILEGE_PRIVATE_KEY:
        ret = item
    elif item.get('admin', {}).get(PRIVATE_KEY + 'deleted'):
        return None
    elif privilege == PRIVILEGE_PRIVATE_KEY:
        ret = dict(
            user=item.get("user") or {},
            admin=sanitize_metadata(item.get("admin"), privilege < PRIVILEGE_ADMIN) or {},
            info=item.get("info") or {},
            official=item.get("official") or [],
        )
    elif item.get('admin', {}).get(PRIVATE_KEY + 'app_publication') is False:
        return None
    else:
        ret = dict(
            user=sanitize_metadata(item.get("user"), privilege < PRIVILEGE_PRIVATE_KEY) or {},
            admin=sanitize_metadata(item.get("admin"), privilege < PRIVILEGE_ADMIN) or {},
            info=item.get("info") or {},
            official=item.get("official") or [],
        )
    ret['_p'] = privilege
    return ret

@app.get("/<workspace>/items")
def get_items(workspace):
    key = flask.request.headers.get("Authorization")
    privilege = authenticate(workspace, key, ["admin", "view"])
    page = flask.request.args.get("page", 0, type=int)
    page_size = flask.request.args.get("page_size", 10, type=int)
    order_by = flask.request.args.get("order_by")
    filters = flask.request.args.get("filters", type=str)
    direction = firestore.Query.ASCENDING
    items = db.collection(WS, workspace, ITEMS)
    if filters:
        filters = filters.split("|")
        for filter in filters:
            key, op, value = filter.split(None, 2)
            try:
                value = json.loads(value)
            except:
                pass
            items = items.where(key, op, value)
    if order_by:
        if order_by.startswith("-"):
            order_by = order_by[1:]
            direction = firestore.Query.DESCENDING
        items = items.order_by(order_by, direction=direction)
    items = items.stream()
    items = (dict(**doc.to_dict(), id=doc.id) for doc in items)
    items = list(items)
    print(f"Items for {workspace}: {items}")
    try:
        items_metadata = (
            process_item(item, privilege)
            for item in items
        )
        items_metadata = (item for item in items_metadata if item is not None)
        paginated_items = list(islice(items_metadata, page * page_size, (page + 1) * page_size))
    except Exception as e:
        msg = str(e)
        if 'The query requires an index' in msg:
            msg = 'https://' + msg.split('https://')[1].split(' ')[0]
            return {'index-required': msg}, 412
        else:
            return {'error': msg}, 500
    return paginated_items, 200

@app.get("/<workspace>/<item_id>")
def get_item(workspace, item_id):
    key = flask.request.headers.get("Authorization")
    item_key = flask.request.args.get("item-key")
    privilege = authenticate(workspace, key, ["admin", "view"])
    item_ref = db.collection(WS, workspace, ITEMS).document(item_id)
    item = item_ref.get().to_dict()
    if not item:
        flask.abort(404, "Item not found")
    if item_key:
        if not item or item["key"] != item_key:
            flask.abort(403, "Unauthorized")
        privilege = PRIVILEGE_PRIVATE_KEY
    ret = process_item(item, privilege)
    if ret is None:
        flask.abort(403, "Unauthorized or item deleted")
    return ret, 200

@app.put("/<workspace>/<item_id>")
def update_item(workspace, item_id):
    key = flask.request.headers.get("Authorization")
    item_key = flask.request.args.get("item-key")
    privilege = authenticate(workspace, key, ["admin", "view"])
    item_ref = db.collection(WS, workspace, ITEMS).document(item_id)
    item = item_ref.get().to_dict()
    if item_key:
        if not item or item["key"] != item_key:
            flask.abort(403, "Unauthorized")
        privilege = PRIVILEGE_PRIVATE_KEY
    item.setdefault('info', {}).update({'_id': item_id})
    metadata = flask.request.json
    metadata = sanitize_metadata(metadata, privilege < PRIVILEGE_PRIVATE_KEY)
    if 'address' in metadata:
        metadata.update(geocode(metadata['address']))
    metadata['updated_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    if privilege > PRIVILEGE_PRIVATE_KEY:
        item.setdefault('admin', {}).update(metadata)
        item_ref.update({"admin": item["admin"]})
        return item["admin"], 200
    elif privilege == PRIVILEGE_PRIVATE_KEY:
        item.setdefault('user', {}).update(metadata)
        item_ref.update({"user": item["user"]})
        return item["user"], 200
    return {"message": f"Unauthorized {privilege} - {metadata!r}"}, 403

@app.delete("/<workspace>/<item_id>")
def delete_item(workspace, item_id):
    key = flask.request.headers.get("Authorization")
    authenticate(workspace, key, ["admin"])
    item_ref = db.collection(WS, workspace, ITEMS).document(item_id)
    item = item_ref.get().to_dict()
    if item.get('official'):
        return {"message": "Item has records, cannot delete"}, 403
    item_ref.delete()
    return {"message": "Item deleted"}, 200

@app.put("/<workspace>")
def update_workspace(workspace):
    key = flask.request.headers.get("Authorization")
    authenticate(workspace, key, ["admin"])
    metadata = flask.request.json
    updates = {"metadata": metadata}
    db.collection(WS).document(workspace).update(updates)
    return {"message": "Workspace updated"}, 200

@app.delete("/<workspace>/items")
def delete_items(workspace):
    key = flask.request.headers.get("Authorization")
    authenticate(workspace, key, ["admin"])
    items_ref = db.collection(WS, workspace, ITEMS)
    docs = items_ref.stream()
    for doc in docs:
        if doc.id[0] != ".":
            doc.reference.delete()
    return {"message": "Items deleted"}, 200
