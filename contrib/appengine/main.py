from fava.application import app
from os import path
import settings
from gcs_sync import gcs_sync

app.config['BEANCOUNT_FILES'] = [path.abspath(path.join(settings.GCS_SYNC_DIR, f)) for f in settings.BEANCOUNT_FILES]

@app.before_first_request
def start_gcs_sync():
    gcs_sync(settings.GCS_SYNC_DIR, settings.GCS_BUCKET)

if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
