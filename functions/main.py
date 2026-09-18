import json
from firebase_functions import https_fn, options, scheduler_fn
from firebase_admin import initialize_app
import time

initialize_app()

from process_data import process_data as process_data_fn
from api import app as api_app
from export_data import export_data as export_data_fn


@scheduler_fn.on_schedule(region='europe-west1', schedule="0 0 * * *", timezone=scheduler_fn.Timezone("Israel"), timeout_sec=900)
def data_processing(event: scheduler_fn.ScheduledEvent) -> https_fn.Response:
    print("Data processing triggered by scheduler")
    start = time.time()
    for bit in process_data_fn():
        ret = json.dumps(bit, ensure_ascii=False)
        elapsed = time.time() - start
        print(f'{elapsed:<10} {ret}')
    print("Data processing completed")
    return 'DONE', 200

@https_fn.on_request(region='europe-west1', timeout_sec=900)
def data_processing_s(event: scheduler_fn.ScheduledEvent) -> https_fn.Response:
    print("Data processing triggered by http request")
    def generate():
        start = time.time()
        for bit in process_data_fn():
            ret = json.dumps(bit, ensure_ascii=False)
            elapsed = time.time() - start
            print(f'{elapsed:<10} {ret}')
            yield f"data: {ret}\n\n"
        print("Data processing completed")
    return https_fn.Response(generate(), status=200, mimetype='text/event-stream')

# Runs after data_processing (00:00, up to 15 minutes) so the export reflects the fresh data
@scheduler_fn.on_schedule(region='europe-west1', schedule="0 2 * * *", timezone=scheduler_fn.Timezone("Israel"),
                          timeout_sec=900, memory=options.MemoryOption.MB_512)
def data_export(event: scheduler_fn.ScheduledEvent) -> None:
    print("Data export triggered by scheduler")
    ret = export_data_fn()
    print(f"Data export completed: {json.dumps(ret, ensure_ascii=False)}")

@https_fn.on_request(
        region='europe-west4',
        cors=options.CorsOptions(cors_origins="*", cors_methods=["post", "get", "put", "delete"]),
        memory=options.MemoryOption.MB_512,
        secrets=['GOOGLE_MAPS_API_KEY']
)
def api(req: https_fn.Request) -> https_fn.Response:
    with api_app.request_context(req.environ):
        return api_app.full_dispatch_request()
