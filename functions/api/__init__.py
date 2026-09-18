import json
from firebase_functions.params import SecretParam
from firebase_admin import firestore, auth, storage
import flask
import re
import uuid
from itertools import islice
import datetime
import requests

db = firestore.client()
app = flask.Flask(__name__)

# Disable caching for all API responses
@app.after_request
def add_no_cache_headers(response):
    # Prevent all caching
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

PRIVILEGE_SUPERADMIN = 5
PRIVILEGE_ADMIN = 4
PRIVILEGE_PRIVATE_KEY = 3
PRIVILEGE_PUBLIC = 0

WS = 'c'
ITEMS = 'items'
PRIVATE_KEY = '_private_'
SETTINGS = 'settings'
SUPERADMINS_DOC = 'superadmins'
STORAGE_BUCKET = 'mapathataf.firebasestorage.app'
BEARER_PREFIX = 'Bearer '
METADATA_KEY_RE = re.compile(r'^[A-Za-z0-9_]+$')
LINK_KINDS = {'internal', 'external', 'whatsapp'}
ALLOWED_LOGO_TYPES = {
    'image/png': 'png',
    'image/jpeg': 'jpg',
    'image/svg+xml': 'svg',
    'image/webp': 'webp',
}
MAX_LOGO_SIZE = 2 * 1024 * 1024

GOOGLE_MAPS_API_KEY = SecretParam("GOOGLE_MAPS_API_KEY").value.strip()

LOGOS_CACHE_TTL = datetime.timedelta(days=1)
logos_cache = None

# Location field model:
# - admin.geocode_address: admin-entered address (or plus code) fed to geocoding
# - admin.lat/lng/formatted_address: geocode results for geocode_address
# - admin.display_address: display-only override, never geocoded
# - admin._private_geocoding_status / _private_geocoded_input: status of the last
#   geocode and the exact geocode_address it ran on
PLUS_CODE_CHARS = '23456789CFGHJMPQRVWX'
SHORT_PLUS_CODE_RE = re.compile(rf'^[{PLUS_CODE_CHARS}]{{4,6}}\+[{PLUS_CODE_CHARS}]{{2,3}}$', re.IGNORECASE)
PLUS_CODE_RE = re.compile(rf'^[{PLUS_CODE_CHARS}]{{4,8}}\+[{PLUS_CODE_CHARS}]{{2,3}}(\s.+)?$', re.IGNORECASE)
GEOCODE_RESULT_FIELDS = ('lat', 'lng', 'formatted_address')
GEOCODE_FIELDS = ('geocode_address',) + GEOCODE_RESULT_FIELDS + ('_private_geocoding_status', '_private_geocoded_input')


def geocode(address, city=None):
    query = address.strip()
    # Plus codes come back as GEOMETRIC_CENTER, which the accuracy gate below
    # would reject, but they are precise by construction
    is_plus_code = bool(PLUS_CODE_RE.match(query))
    if city and SHORT_PLUS_CODE_RE.match(query):
        query = f'{query} {city}'
    url = 'https://maps.googleapis.com/maps/api/geocode/json'
    params = {
        'address': query,
        'key': GOOGLE_MAPS_API_KEY,
        'language': 'iw',
        'components': 'country:IL',
    }
    update = dict(
        _private_geocoding_status='ERROR',
        _private_geocoded_input=address,
    )
    try:
        result = requests.get(url, params=params).json()
    except requests.RequestException:
        return update
    if result['status'] == 'ZERO_RESULTS':
        update['_private_geocoding_status'] = 'ZERO_RESULTS'
    elif result['status'] == 'OK':
        result = result['results'][0]
        accuracy = result['geometry']['location_type']
        if is_plus_code or accuracy in {'ROOFTOP', 'RANGE_INTERPOLATED'}:
            location = result['geometry']['location']
            update.update(dict(
                lat=location['lat'],
                lng=location['lng'],
                formatted_address=result['formatted_address'],
                _private_geocoding_status='OK',
            ))
        else:
            update['_private_geocoding_status'] = 'INACCURATE'
    return update

# Helper functions for authentication and utility
# def generate_keys():
#     return {
#         "admin": str(uuid.uuid4()),
#         "collaborate": str(uuid.uuid4()),
#         "view": str(uuid.uuid4())
#     }

def resolve_superadmin(auth_header, strict=True):
    """Verify a 'Bearer <Google/Firebase id-token>' header against the superadmin
    allowlist (settings/superadmins doc). Returns the superadmin email, or None
    when strict=False and the header does not resolve to a superadmin.
    strict=True aborts: 401 for missing/invalid/expired tokens, 403 for valid
    tokens that are unverified or not allowlisted."""
    def fail(code, message):
        if strict:
            flask.abort(code, message)
        return None
    if not auth_header or not auth_header.startswith(BEARER_PREFIX):
        return fail(401, "Missing bearer token")
    token = auth_header[len(BEARER_PREFIX):].strip()
    try:
        decoded = auth.verify_id_token(token)
    except (ValueError, auth.InvalidIdTokenError, auth.ExpiredIdTokenError,
            auth.RevokedIdTokenError, auth.CertificateFetchError):
        return fail(401, "Invalid token")
    email = (decoded.get('email') or '').lower()
    if not email or not decoded.get('email_verified'):
        return fail(403, "Unverified email")
    superadmins = db.collection(SETTINGS).document(SUPERADMINS_DOC).get().to_dict() or {}
    if email not in [e.lower() for e in superadmins.get('emails', [])]:
        return fail(403, "Not a superadmin")
    return email


def authenticate_superadmin():
    return resolve_superadmin(flask.request.headers.get("Authorization"))


def authenticate(workspace, key, required_roles):
    config_ref = db.collection(WS).document(workspace)
    config = config_ref.get().to_dict()
    if not config:
        flask.abort(404, "Workspace not found")
    if key and key.startswith(BEARER_PREFIX) and resolve_superadmin(key, strict=False):
        return PRIVILEGE_SUPERADMIN
    if "admin" in required_roles and key == config["key"]:
        return PRIVILEGE_ADMIN
    if "view" in required_roles:
        return PRIVILEGE_PUBLIC
    flask.abort(403, "Unauthorized")

def sanitize_metadata(metadata, exclude_private=True):
    if metadata and exclude_private:
        return {k: v for k, v in metadata.items() if not k.startswith(PRIVATE_KEY)}
    return metadata

def make_blob_public(blob):
    try:
        blob.make_public()
    except Exception as e:
        # Uniform bucket-level access rejects per-object ACLs; rely on
        # bucket-level public read in that case
        print(f'make_public failed (uniform bucket-level access?): {e}')

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

@app.get("/logos")
def get_logos():
    global logos_cache
    now = datetime.datetime.now(datetime.timezone.utc)
    if logos_cache is None or logos_cache[0] < now:
        payload = []
        for doc in db.collection(WS).stream():
            data = doc.to_dict() or {}
            metadata = data.get('metadata') or {}
            logo_url = (metadata.get('logo_url') or '').strip()
            if data.get('active') and logo_url:
                payload.append(dict(id=doc.id, city=metadata.get('city'), logo_url=logo_url))
        logos_cache = (now + LOGOS_CACHE_TTL, payload)
    return logos_cache[1], 200

def workspace_payload(doc_id, data):
    data = data or {}
    return dict(
        id=doc_id,
        metadata=data.get('metadata') or {},
        key=data.get('key'),
        favorite=data.get('favorite', False),
        active=data.get('active', False),
    )

@app.get("/manage/workspaces")
def manage_list_workspaces():
    authenticate_superadmin()
    payload = [workspace_payload(doc.id, doc.to_dict()) for doc in db.collection(WS).stream()]
    return payload, 200

@app.put("/manage/workspaces/<workspace>")
def manage_update_workspace(workspace):
    global logos_cache
    authenticate_superadmin()
    doc_ref = db.collection(WS).document(workspace)
    if not doc_ref.get().to_dict():
        flask.abort(404, "Workspace not found")
    body = flask.request.json
    if not isinstance(body, dict):
        flask.abort(400, "Invalid body")
    unknown = set(body.keys()) - {'metadata', 'favorite', 'active'}
    if unknown:
        flask.abort(400, f"Unknown keys: {sorted(unknown)}")
    updates = {}
    metadata = body.get('metadata')
    if metadata is not None:
        if not isinstance(metadata, dict):
            flask.abort(400, "metadata must be an object")
        for k, v in metadata.items():
            if not METADATA_KEY_RE.match(k):
                flask.abort(400, f"Invalid metadata key: {k!r}")
            if k == 'links' and v is not None:
                if not isinstance(v, list) or any(
                        not isinstance(link, dict) or link.get('kind') not in LINK_KINDS
                        for link in v):
                    flask.abort(400, "links must be a list of objects with kind internal/external/whatsapp")
            updates[f'metadata.{k}'] = firestore.DELETE_FIELD if v is None else v
    for flag in ('favorite', 'active'):
        if flag in body:
            if not isinstance(body[flag], bool):
                flask.abort(400, f"{flag} must be a boolean")
            updates[flag] = body[flag]
    if updates:
        doc_ref.update(updates)
        logos_cache = None
    return workspace_payload(workspace, doc_ref.get().to_dict()), 200

@app.post("/manage/workspaces/<workspace>/logo")
def manage_upload_logo(workspace):
    global logos_cache
    authenticate_superadmin()
    doc_ref = db.collection(WS).document(workspace)
    if not doc_ref.get().to_dict():
        flask.abort(404, "Workspace not found")
    if (flask.request.content_length or 0) > MAX_LOGO_SIZE + 64 * 1024:
        flask.abort(400, "Logo too large (max 2MB)")
    file = flask.request.files.get('logo')
    if file is None:
        flask.abort(400, "Missing 'logo' file field")
    ext = ALLOWED_LOGO_TYPES.get(file.mimetype)
    if ext is None:
        flask.abort(400, f"Unsupported logo type: {file.mimetype}")
    bucket = storage.bucket(STORAGE_BUCKET)
    blob = bucket.blob(f'logos/{workspace}-{uuid.uuid4().hex[:8]}.{ext}')
    blob.cache_control = 'public, max-age=31536000, immutable'
    blob.upload_from_file(file.stream, content_type=file.mimetype)
    make_blob_public(blob)
    logo_url = f'https://storage.googleapis.com/{bucket.name}/{blob.name}'
    doc_ref.update({'metadata.logo_url': logo_url})
    logos_cache = None
    return {'logo_url': logo_url}, 200

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
    item['id'] = item.get('_doc_id')
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
            id=item['id']
        )
    elif item.get('admin', {}).get('app_publication') is False:
        return None
    else:
        ret = dict(
            user=sanitize_metadata(item.get("user"), privilege < PRIVILEGE_PRIVATE_KEY) or {},
            admin=sanitize_metadata(item.get("admin"), privilege < PRIVILEGE_ADMIN) or {},
            info=item.get("info") or {},
            official=item.get("official") or [],
            id=item['id']
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
    config_ref = db.collection(WS).document(workspace)
    config = config_ref.get().to_dict()
    if config.get('metadata', {}).get('city_links'):
        workspaces = config['metadata']['city_links']
    else:
        workspaces = [workspace]
    ret_items = []
    for ws in workspaces:
        items = db.collection(WS, ws, ITEMS)
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
        items = (dict(**doc.to_dict(), _doc_id=doc.id) for doc in items)
        items = list(items)
        ret_items.extend(items)
        print(f"Items for {ws}: {items}")
    try:
        items_metadata = (
            process_item(item, privilege)
            for item in ret_items
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
    if not item.get('info', {}).get('_id'):
        item.setdefault('info', {}).update({'_id': item_id})
        item_ref.update({"info": item["info"]})
    metadata = flask.request.json
    metadata = sanitize_metadata(metadata, privilege < PRIVILEGE_PRIVATE_KEY)
    geocode_update = None
    clear_geocode = False
    if privilege >= PRIVILEGE_ADMIN and 'geocode_address' in metadata:
        geocode_address = str(metadata.pop('geocode_address') or '').strip()
        if geocode_address:
            metadata['geocode_address'] = geocode_address
            city = None
            if SHORT_PLUS_CODE_RE.match(geocode_address):
                config = db.collection(WS).document(workspace).get().to_dict() or {}
                city = (config.get('metadata') or {}).get('city')
            geocode_update = geocode(geocode_address, city=city)
            metadata.update(geocode_update)
        else:
            clear_geocode = True
    metadata['updated_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    if privilege > PRIVILEGE_PRIVATE_KEY:
        item.setdefault('admin', {}).update(metadata)
        admin = item['admin']
        if clear_geocode:
            for field in GEOCODE_FIELDS:
                admin.pop(field, None)
        elif geocode_update is not None and geocode_update['_private_geocoding_status'] != 'OK':
            for field in GEOCODE_RESULT_FIELDS:
                admin.pop(field, None)
        item_ref.update({"admin": admin})
        return admin, 200
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
