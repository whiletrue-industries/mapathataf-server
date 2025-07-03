import json
from firebase_functions import https_fn, options, scheduler_fn
from firebase_admin import initialize_app
import time

initialize_app()

from process_data import process_data as process_data_fn
from api import app as api_app


@scheduler_fn.on_schedule(region='europe-west1', schedule="0 0 * * *", timezone=scheduler_fn.Timezone("Israel"), timeout_sec=900)
def data_processing(event: scheduler_fn.ScheduledEvent) -> https_fn.Response:
    print("Data processing triggered by scheduler")
    def generate():
        start = time.time()
        for bit in process_data_fn():
            ret = json.dumps(bit, ensure_ascii=False)
            elapsed = time.time() - start
            print(f'{elapsed:<10} {ret}')
            yield f"data: {ret}\n\n"
    return https_fn.Response(generate(), status=200, mimetype='text/event-stream')


@https_fn.on_request(
        region='europe-west4',
        cors=options.CorsOptions(cors_origins="*", cors_methods=["post", "get", "put", "delete"]),
        memory=options.MemoryOption.MB_512,
        secrets=['GOOGLE_MAPS_API_KEY']
)
def api(req: https_fn.Request) -> https_fn.Response:
    with api_app.request_context(req.environ):
        return api_app.full_dispatch_request()
