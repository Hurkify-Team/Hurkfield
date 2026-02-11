import os
from config import HOST, PORT

bind = os.getenv("OPENFIELD_GUNICORN_BIND", f"{HOST}:{PORT}")
workers = int(os.getenv("OPENFIELD_GUNICORN_WORKERS", "2"))
threads = int(os.getenv("OPENFIELD_GUNICORN_THREADS", "4"))
timeout = int(os.getenv("OPENFIELD_GUNICORN_TIMEOUT", "60"))
accesslog = "-"
errorlog = "-"

