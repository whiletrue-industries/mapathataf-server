from conftest import WORKSPACE, SUPERADMIN_HEADER


def test_no_header(superadmin, client):
    assert client.get('/manage/workspaces').status_code == 401


def test_non_bearer_header(superadmin, client):
    res = client.get('/manage/workspaces', headers={'Authorization': 'plain-key'})
    assert res.status_code == 401


def test_invalid_token(superadmin, client):
    res = client.get('/manage/workspaces', headers={'Authorization': 'Bearer nope'})
    assert res.status_code == 401


def test_verified_but_not_allowlisted(superadmin, client):
    res = client.get('/manage/workspaces', headers={'Authorization': 'Bearer stranger-token'})
    assert res.status_code == 403


def test_unverified_email(superadmin, client):
    res = client.get('/manage/workspaces', headers={'Authorization': 'Bearer unverified-token'})
    assert res.status_code == 403


def test_superadmin_allowed(superadmin, client):
    res = client.get('/manage/workspaces', headers=SUPERADMIN_HEADER)
    assert res.status_code == 200


def test_superadmin_bearer_on_workspace_route(superadmin, client):
    superadmin.workspaces[WORKSPACE]['metadata']['_private_note'] = 'secret'
    res = client.get(f'/{WORKSPACE}', headers=SUPERADMIN_HEADER)
    assert res.status_code == 200
    assert res.json['_p'] == 5
    assert res.json['_private_note'] == 'secret'


def test_bad_bearer_on_workspace_route_degrades_to_public(superadmin, client):
    superadmin.workspaces[WORKSPACE]['metadata']['_private_note'] = 'secret'
    res = client.get(f'/{WORKSPACE}', headers={'Authorization': 'Bearer nope'})
    assert res.status_code == 200
    assert res.json['_p'] == 0
    assert '_private_note' not in res.json
