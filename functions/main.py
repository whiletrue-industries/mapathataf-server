from firebase_functions import https_fn, options
from firebase_admin import initialize_app
from process_data import process_data as process_data_fn

initialize_app()

@https_fn.on_request(region='europe-west4', timeout_sec=900)
def process_data(req: https_fn.Request) -> https_fn.Response:
    process_data_fn()
    return dict(success=True), 200