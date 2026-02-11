import os
import re
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
INSTANCE_DIR = BASE_DIR / "instance"
INSTANCE_DIR.mkdir(exist_ok=True)


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            if not key or key in os.environ:
                continue
            val = val.strip().strip("'").strip('"')
            os.environ[key] = val
    except Exception:
        # Fail open if .env can't be parsed.
        return


_load_dotenv(BASE_DIR / ".env")


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def _env_bool(key: str, default: bool = False) -> bool:
    val = os.getenv(key)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def _env_int(key: str, default: int) -> int:
    val = os.getenv(key)
    if val is None or val == "":
        return default
    try:
        return int(val)
    except Exception:
        return default


def _resolve_path(value: str, fallback: Path) -> str:
    if not value:
        return str(fallback)
    p = Path(value)
    if not p.is_absolute():
        p = BASE_DIR / p
    return str(p)


APP_NAME = _env("OPENFIELD_APP_NAME", "OpenField Collect")
APP_VERSION = _env("OPENFIELD_APP_VERSION", "OpenField MVP v0.1")
APP_ENV = _env("OPENFIELD_ENV", "development").strip().lower()

_instance_db = INSTANCE_DIR / "openfield.db"
_default_db = BASE_DIR / "openfield.db"
DB_PATH = _env("OPENFIELD_DB_PATH", "")
if not DB_PATH:
    DB_PATH = str(_default_db if _default_db.exists() else _instance_db)

UPLOAD_DIR = _resolve_path(_env("OPENFIELD_UPLOAD_DIR", ""), BASE_DIR / "uploads")
EXPORT_DIR = _resolve_path(_env("OPENFIELD_EXPORT_DIR", ""), BASE_DIR / "exports")

ADMIN_KEY = _env("OPENFIELD_ADMIN_KEY", "").strip()
ENABLE_SERVER_DRAFTS = _env_bool("OPENFIELD_SERVER_DRAFTS", False)
DRAFTS_TABLE = _env("OPENFIELD_DRAFTS_TABLE", "survey_drafts")

# Platformization / multi-supervisor mode
PLATFORM_MODE = _env_bool("OPENFIELD_PLATFORM_MODE", False)
REQUIRE_SUPERVISOR_KEY = _env_bool("OPENFIELD_REQUIRE_SUPERVISOR_KEY", PLATFORM_MODE)
PROJECT_REQUIRED = _env_bool("OPENFIELD_PROJECT_REQUIRED", PLATFORM_MODE)
SUPERVISOR_KEY_PARAM = _env("OPENFIELD_SUPERVISOR_KEY_PARAM", "sk")
SUPERVISOR_KEY_COOKIE = _env("OPENFIELD_SUPERVISOR_KEY_COOKIE", "openfield_skey")

HOST = _env("OPENFIELD_HOST", "127.0.0.1")
PORT = _env_int("OPENFIELD_PORT", 5000)
DEBUG = _env_bool(
    "OPENFIELD_DEBUG",
    APP_ENV in ("dev", "development", "local"),
)

SECRET_KEY = _env("OPENFIELD_SECRET_KEY", "")

# Email (SMTP)
SMTP_HOST = _env("OPENFIELD_SMTP_HOST", "")
SMTP_PORT = _env_int("OPENFIELD_SMTP_PORT", 587)
SMTP_USER = _env("OPENFIELD_SMTP_USER", "")
SMTP_PASS = _env("OPENFIELD_SMTP_PASS", "")
SMTP_TLS = _env_bool("OPENFIELD_SMTP_TLS", True)
SMTP_FROM = _env("OPENFIELD_SMTP_FROM", "no-reply@openfield.local")

# Audio transcription (supports OPENFIELD_* and HURKFIELD_* aliases)
TRANSCRIBE_PROVIDER = _env(
    "OPENFIELD_TRANSCRIBE_PROVIDER",
    _env("HURKFIELD_TRANSCRIBE_PROVIDER", "openai"),
).strip().lower()
_transcribe_openfield_key = _env("OPENFIELD_TRANSCRIBE_OPENAI_KEY", "").strip()
_transcribe_hurkfield_key = _env("HURKFIELD_TRANSCRIBE_OPENAI_KEY", "").strip()
_transcribe_openai_key = _env("OPENAI_API_KEY", "").strip()
_transcribe_openfield_deepgram_key = _env("OPENFIELD_TRANSCRIBE_DEEPGRAM_KEY", "").strip()
_transcribe_hurkfield_deepgram_key = _env("HURKFIELD_TRANSCRIBE_DEEPGRAM_KEY", "").strip()
_transcribe_deepgram_key = _env("DEEPGRAM_API_KEY", "").strip()
_key_re = re.compile(r"sk-[A-Za-z0-9_-]{20,}$")

def _pick_transcribe_key() -> str:
    # Prefer a syntactically valid key, whichever namespace it comes from.
    for cand in (_transcribe_openfield_key, _transcribe_hurkfield_key, _transcribe_openai_key):
        c = cand.strip().rstrip(".;,")
        if c and _key_re.fullmatch(c):
            return c
    # Fallback to first non-empty candidate for debugging/error messaging.
    for cand in (_transcribe_openfield_key, _transcribe_hurkfield_key, _transcribe_openai_key):
        c = cand.strip().rstrip(".;,")
        if c:
            return c
    return ""

TRANSCRIBE_OPENAI_KEY = _pick_transcribe_key()
TRANSCRIBE_DEEPGRAM_KEY = (
    _transcribe_openfield_deepgram_key
    or _transcribe_hurkfield_deepgram_key
    or _transcribe_deepgram_key
).strip().rstrip(".;,")
TRANSCRIBE_MODEL = _env(
    "OPENFIELD_TRANSCRIBE_MODEL",
    _env("HURKFIELD_TRANSCRIBE_MODEL", "gpt-4o-mini-transcribe"),
).strip()
TRANSCRIBE_DEEPGRAM_MODEL = _env(
    "OPENFIELD_TRANSCRIBE_DEEPGRAM_MODEL",
    _env("HURKFIELD_TRANSCRIBE_DEEPGRAM_MODEL", "nova-2"),
).strip()
TRANSCRIBE_LANGUAGE = _env(
    "OPENFIELD_TRANSCRIBE_LANGUAGE",
    _env("HURKFIELD_TRANSCRIBE_LANGUAGE", ""),
).strip()
try:
    TRANSCRIBE_TIMEOUT = int(
        _env("OPENFIELD_TRANSCRIBE_TIMEOUT", _env("HURKFIELD_TRANSCRIBE_TIMEOUT", "120"))
    )
except Exception:
    TRANSCRIBE_TIMEOUT = 120
