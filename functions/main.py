import json
from firebase_functions import https_fn, options
from firebase_admin import initialize_app

initialize_app()

from process_data import process_data as process_data_fn
from api import app as api_app

@https_fn.on_request(region='europe-west4', timeout_sec=900)
def process_data(req: https_fn.Request) -> https_fn.Response:
    def generate():
        for bit in process_data_fn():
            yield f"data: {json.dumps(bit, ensure_ascii=False)}\n\n"
    return https_fn.Response(generate(), status=200, mimetype='text/event-stream')


@https_fn.on_request(region='europe-west4', cors=options.CorsOptions(cors_origins="*", cors_methods=["post", "get", "put", "delete"]), memory=options.MemoryOption.MB_512)
def api(req: https_fn.Request) -> https_fn.Response:
    with api_app.request_context(req.environ):
        return api_app.full_dispatch_request()
