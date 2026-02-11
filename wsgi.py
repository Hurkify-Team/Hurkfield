import os

import config
from app import app, ensure_drafts_table, init_db


# Ensure required runtime folders/tables exist when running via Gunicorn/Werkzeug.
os.makedirs(config.INSTANCE_DIR, exist_ok=True)
os.makedirs(config.UPLOAD_DIR, exist_ok=True)
os.makedirs(config.EXPORT_DIR, exist_ok=True)
init_db()
ensure_drafts_table()
