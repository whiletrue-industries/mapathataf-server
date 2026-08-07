from conftest import ADMIN_KEY, WORKSPACE, SUPERADMIN_HEADER


def test_list_shape_and_defaults(superadmin, client):
    res = client.get('/manage/workspaces', headers=SUPERADMIN_HEADER)
    assert res.status_code == 200
    assert res.json == [{
        'id': WORKSPACE,
        'metadata': {'city': 'תל אביב'},
        'key': ADMIN_KEY,
        'favorite': False,
        'active': False,
    }]


def test_merge_update_does_not_clobber_other_metadata(superadmin, client):
    superadmin.workspaces[WORKSPACE]['metadata']['logo_url'] = 'https://x/logo.png'
    res = client.put(f'/manage/workspaces/{WORKSPACE}', headers=SUPERADMIN_HEADER,
                     json={'metadata': {'bounds': [1, 2, 3, 4]}})
    assert res.status_code == 200
    metadata = superadmin.workspaces[WORKSPACE]['metadata']
    assert metadata['bounds'] == [1, 2, 3, 4]
    assert metadata['city'] == 'תל אביב'
    assert metadata['logo_url'] == 'https://x/logo.png'
    assert res.json['metadata']['bounds'] == [1, 2, 3, 4]


def test_null_metadata_value_deletes_the_key(superadmin, client):
    superadmin.workspaces[WORKSPACE]['metadata']['neighborhoods'] = ['a', 'b']
    res = client.put(f'/manage/workspaces/{WORKSPACE}', headers=SUPERADMIN_HEADER,
                     json={'metadata': {'neighborhoods': None}})
    assert res.status_code == 200
    assert 'neighborhoods' not in superadmin.workspaces[WORKSPACE]['metadata']


def test_flags_update(superadmin, client):
    res = client.put(f'/manage/workspaces/{WORKSPACE}', headers=SUPERADMIN_HEADER,
                     json={'favorite': True, 'active': True})
    assert res.status_code == 200
    assert res.json['favorite'] is True
    assert res.json['active'] is True
    assert superadmin.workspaces[WORKSPACE]['favorite'] is True


def test_non_bool_flag_rejected(superadmin, client):
    res = client.put(f'/manage/workspaces/{WORKSPACE}', headers=SUPERADMIN_HEADER,
                     json={'favorite': 'yes'})
    assert res.status_code == 400


def test_unknown_top_level_key_rejected(superadmin, client):
    res = client.put(f'/manage/workspaces/{WORKSPACE}', headers=SUPERADMIN_HEADER,
                     json={'key': 'evil'})
    assert res.status_code == 400
    assert superadmin.workspaces[WORKSPACE]['key'] == ADMIN_KEY


def test_invalid_metadata_key_rejected(superadmin, client):
    res = client.put(f'/manage/workspaces/{WORKSPACE}', headers=SUPERADMIN_HEADER,
                     json={'metadata': {'bad.key': 1}})
    assert res.status_code == 400


def test_invalid_link_kind_rejected(superadmin, client):
    res = client.put(f'/manage/workspaces/{WORKSPACE}', headers=SUPERADMIN_HEADER,
                     json={'metadata': {'links': [{'kind': 'ftp', 'href': 'x', 'title': 'y'}]}})
    assert res.status_code == 400


def test_valid_links_accepted(superadmin, client):
    links = [{'kind': 'whatsapp', 'href': 'https://chat.whatsapp.com/x', 'title': 'קהילה'}]
    res = client.put(f'/manage/workspaces/{WORKSPACE}', headers=SUPERADMIN_HEADER,
                     json={'metadata': {'links': links}})
    assert res.status_code == 200
    assert superadmin.workspaces[WORKSPACE]['metadata']['links'] == links


def test_unknown_workspace_404(superadmin, client):
    res = client.put('/manage/workspaces/nope', headers=SUPERADMIN_HEADER,
                     json={'favorite': True})
    assert res.status_code == 404
