import copy
import os
import sys
from unittest import mock

import pytest

# api reads GOOGLE_MAPS_API_KEY (SecretParam) and opens a Firestore client at
# import time; provide both before the module loads
os.environ.setdefault('GOOGLE_MAPS_API_KEY', 'test-key')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

with mock.patch('firebase_admin.firestore.client'):
    import api  # noqa: E402

DELETE_FIELD = api.firestore.DELETE_FIELD


class FakeSnapshot:
    def __init__(self, data):
        self._data = data

    def to_dict(self):
        return copy.deepcopy(self._data) if self._data is not None else None


class FakeDocRef:
    def __init__(self, docs, key):
        self._docs = docs
        self._key = key

    def get(self):
        return FakeSnapshot(self._docs.get(self._key))

    def set(self, data):
        self._docs[self._key] = copy.deepcopy(data)

    def delete(self):
        self._docs.pop(self._key, None)

    def update(self, values):
        # Mirror Firestore update() semantics: dotted keys address nested maps,
        # DELETE_FIELD removes the addressed key
        doc = self._docs.setdefault(self._key, {})
        for path, value in values.items():
            parts = path.split('.')
            target = doc
            for part in parts[:-1]:
                target = target.setdefault(part, {})
            if value is DELETE_FIELD:
                target.pop(parts[-1], None)
            else:
                target[parts[-1]] = copy.deepcopy(value)


class FakeStreamedDoc:
    def __init__(self, doc_id, data, reference):
        self.id = doc_id
        self._data = data
        self.reference = reference

    def to_dict(self):
        return copy.deepcopy(self._data)


class FakeCollection:
    def __init__(self, db, path):
        self._db = db
        self._path = path

    def _top_level_docs(self):
        if self._path[0] == api.SETTINGS:
            return self._db.settings
        return self._db.workspaces

    def document(self, doc_id):
        if len(self._path) == 1:
            return FakeDocRef(self._top_level_docs(), doc_id)
        return FakeDocRef(self._db.items, (self._path[1], doc_id))

    def stream(self):
        if len(self._path) == 1:
            docs = self._top_level_docs()
            for doc_id in list(docs):
                yield FakeStreamedDoc(doc_id, docs[doc_id],
                                      FakeDocRef(docs, doc_id))
        else:
            workspace = self._path[1]
            for key in list(self._db.items):
                if key[0] == workspace:
                    yield FakeStreamedDoc(key[1], self._db.items[key],
                                          FakeDocRef(self._db.items, key))


class FakeBatch:
    def __init__(self):
        self._ops = []

    def set(self, ref, data):
        self._ops.append(lambda: ref.set(data))

    def delete(self, ref):
        self._ops.append(ref.delete)

    def commit(self):
        for op in self._ops:
            op()


class FakeDB:
    """In-memory stand-in for the Firestore layout (c/{ws}/items/{id} + settings/*)."""

    def __init__(self):
        self.workspaces = {}
        self.items = {}
        self.settings = {}

    def collection(self, *path):
        return FakeCollection(self, path)

    def batch(self):
        return FakeBatch()


class FakeBlob:
    def __init__(self, bucket, name):
        self._bucket = bucket
        self.name = name
        self.cache_control = None
        self.content_type = None
        self.public = False

    def upload_from_file(self, stream, content_type=None):
        self.content_type = content_type
        self._bucket.blobs[self.name] = self
        self.data = stream.read()

    def upload_from_string(self, data, content_type=None):
        self.content_type = content_type
        self._bucket.blobs[self.name] = self
        self.data = data

    def make_public(self):
        self.public = True


class FakeBucket:
    def __init__(self, name):
        self.name = name
        self.blobs = {}

    def blob(self, name):
        return FakeBlob(self, name)


ADMIN_KEY = 'admin-key'
ITEM_KEY = 'item-key-1'
WORKSPACE = 'testws'
ITEM_ID = 'item1'
SUPERADMIN_EMAIL = 'root@example.com'
SUPERADMIN_TOKEN = 'good-token'
SUPERADMIN_HEADER = {'Authorization': f'Bearer {SUPERADMIN_TOKEN}'}


@pytest.fixture
def db(monkeypatch):
    fake = FakeDB()
    fake.workspaces[WORKSPACE] = {'key': ADMIN_KEY, 'metadata': {'city': 'תל אביב'}}
    fake.items[(WORKSPACE, ITEM_ID)] = {
        'key': ITEM_KEY,
        'info': {'_id': ITEM_ID},
        'official': [],
        'admin': {},
        'user': {},
    }
    monkeypatch.setattr(api, 'db', fake)
    return fake


@pytest.fixture
def superadmin(db, monkeypatch):
    """Seeds the allowlist and fakes id-token verification.
    Tokens: 'good-token' -> superadmin, 'stranger-token' -> verified but not
    allowlisted, 'unverified-token' -> unverified email, anything else raises."""
    db.settings[api.SUPERADMINS_DOC] = {'emails': [SUPERADMIN_EMAIL]}

    def fake_verify(token):
        if token == SUPERADMIN_TOKEN:
            return {'email': SUPERADMIN_EMAIL, 'email_verified': True}
        if token == 'stranger-token':
            return {'email': 'stranger@example.com', 'email_verified': True}
        if token == 'unverified-token':
            return {'email': SUPERADMIN_EMAIL, 'email_verified': False}
        raise api.auth.InvalidIdTokenError('bad token')

    monkeypatch.setattr(api.auth, 'verify_id_token', fake_verify)
    return db


@pytest.fixture
def bucket(monkeypatch):
    fake = FakeBucket(api.STORAGE_BUCKET)
    monkeypatch.setattr(api.storage, 'bucket', lambda name: fake)
    return fake


@pytest.fixture
def client():
    api.app.config['TESTING'] = True
    api.logos_cache = None
    return api.app.test_client()
