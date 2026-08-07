import io

import api
from conftest import WORKSPACE, SUPERADMIN_HEADER


def upload(client, **kwargs):
    data = {'logo': (io.BytesIO(b'png-bytes'), 'logo.png', 'image/png')}
    data.update(kwargs)
    return client.post(f'/manage/workspaces/{WORKSPACE}/logo', headers=SUPERADMIN_HEADER,
                       data=data, content_type='multipart/form-data')


def test_upload_happy_path(superadmin, bucket, client):
    api.logos_cache = (None, [])
    res = upload(client)
    assert res.status_code == 200
    logo_url = res.json['logo_url']
    assert logo_url.startswith(
        f'https://storage.googleapis.com/{api.STORAGE_BUCKET}/logos/{WORKSPACE}-')
    assert logo_url.endswith('.png')
    assert superadmin.workspaces[WORKSPACE]['metadata']['logo_url'] == logo_url
    assert superadmin.workspaces[WORKSPACE]['metadata']['city'] == 'תל אביב'
    [blob] = bucket.blobs.values()
    assert blob.content_type == 'image/png'
    assert blob.data == b'png-bytes'
    assert blob.cache_control == 'public, max-age=31536000, immutable'
    assert api.logos_cache is None


def test_missing_field(superadmin, bucket, client):
    res = client.post(f'/manage/workspaces/{WORKSPACE}/logo', headers=SUPERADMIN_HEADER,
                      data={}, content_type='multipart/form-data')
    assert res.status_code == 400


def test_unsupported_type(superadmin, bucket, client):
    res = upload(client, logo=(io.BytesIO(b'hi'), 'logo.txt', 'text/plain'))
    assert res.status_code == 400
    assert bucket.blobs == {}


def test_unknown_workspace(superadmin, bucket, client):
    res = client.post('/manage/workspaces/nope/logo', headers=SUPERADMIN_HEADER,
                      data={'logo': (io.BytesIO(b'x'), 'l.png', 'image/png')},
                      content_type='multipart/form-data')
    assert res.status_code == 404
