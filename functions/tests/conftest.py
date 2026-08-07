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

    def update(self, values):
        self._docs.setdefault(self._key, {}).update(copy.deepcopy(values))


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

    def document(self, doc_id):
        if len(self._path) == 1:
            return FakeDocRef(self._db.workspaces, doc_id)
        return FakeDocRef(self._db.items, (self._path[1], doc_id))

    def stream(self):
        if len(self._path) == 1:
            for doc_id in list(self._db.workspaces):
                yield FakeStreamedDoc(doc_id, self._db.workspaces[doc_id],
                                      FakeDocRef(self._db.workspaces, doc_id))
        else:
            workspace = self._path[1]
            for key in list(self._db.items):
                if key[0] == workspace:
                    yield FakeStreamedDoc(key[1], self._db.items[key],
                                          FakeDocRef(self._db.items, key))


class FakeDB:
    """In-memory stand-in for the two-level Firestore layout (c/{ws}/items/{id})."""

    def __init__(self):
        self.workspaces = {}
        self.items = {}

    def collection(self, *path):
        return FakeCollection(self, path)


ADMIN_KEY = 'admin-key'
ITEM_KEY = 'item-key-1'
WORKSPACE = 'testws'
ITEM_ID = 'item1'


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
def client():
    api.app.config['TESTING'] = True
    return api.app.test_client()
