def seed(db, slug, active, logo_url):
    metadata = {'city': slug}
    if logo_url:
        metadata['logo_url'] = logo_url
    doc = {'key': 'k-' + slug, 'metadata': metadata}
    if active is not None:
        doc['active'] = active
    db.workspaces[slug] = doc


def test_only_active_workspaces_with_logos_are_listed(db, client):
    del db.workspaces['testws']
    seed(db, 'both', True, 'https://x/both.png')
    seed(db, 'active-no-logo', True, None)
    seed(db, 'logo-not-active', False, 'https://x/inactive.png')
    seed(db, 'logo-no-active-field', None, 'https://x/legacy.png')
    res = client.get('/logos')
    assert res.status_code == 200
    assert res.json == [{'id': 'both', 'city': 'both', 'logo_url': 'https://x/both.png'}]


def test_logo_url_whitespace_stripped(db, client):
    del db.workspaces['testws']
    seed(db, 'padded', True, 'https://x/logo.png\n')
    res = client.get('/logos')
    assert res.json == [{'id': 'padded', 'city': 'padded', 'logo_url': 'https://x/logo.png'}]
