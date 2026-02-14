# db.py — OpenField Collect (DB v2: Projects + Enumerators + Assignments + Code System)
# Full replacement

import os
import sqlite3
import re
import hashlib
from contextlib import contextmanager
from typing import List

try:
    from config import DB_PATH
except Exception:
    DB_PATH = os.environ.get("OPENFIELD_DB_PATH", "openfield.db")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    )
    return cur.fetchone() is not None


def _cols(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return [r["name"] for r in cur.fetchall()]


def _add_column_if_missing(conn: sqlite3.Connection, table: str, col_def_sql: str) -> None:
    """
    col_def_sql example: "project_id INTEGER"
    """
    col_name = col_def_sql.strip().split()[0]
    existing = _cols(conn, table)
    if col_name in existing:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def_sql}")


def _generate_project_tag(project_name: str) -> str:
    clean = (project_name or "").strip().upper()
    clean = re.sub(r"[^A-Z0-9\\s\\-]", " ", clean)
    clean = re.sub(r"\\s+", " ", clean).strip()
    words = [w for w in clean.split(" ") if len(w) >= 3 and w not in ("THE", "AND", "FOR", "WITH")]
    prefix = "".join([w[0] for w in words[:3]]) or "PRJ"
    digest = hashlib.sha256(clean.encode("utf-8")).hexdigest().upper()
    suffix = digest[:2]
    tag = f"{prefix}{suffix}"
    return re.sub(r"[^A-Z0-9]", "", tag)[:8] or "PRJ00"


def init_db() -> None:
    """
    Safe init:
    - Creates tables if missing
    - Adds new columns if missing
    - Adds indexes
    """
    with get_conn() as conn:
        cur = conn.cursor()

        # -----------------------------
        # Existing core tables (ensure)
        # -----------------------------
        if not _table_exists(conn, "facilities"):
            cur.execute(
                """
                CREATE TABLE facilities (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  name TEXT NOT NULL,
                  created_at TEXT
                )
                """
            )

        if not _table_exists(conn, "survey_templates"):
            cur.execute(
                """
                CREATE TABLE survey_templates (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  name TEXT NOT NULL,
                  description TEXT,
                  is_active INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT,
                  created_by TEXT,
                  updated_at TEXT,
                  deleted_at TEXT,
                  source TEXT,
                  assignment_mode TEXT,
                  template_version TEXT,
                  enable_consent INTEGER NOT NULL DEFAULT 0,
                  enable_attestation INTEGER NOT NULL DEFAULT 0,
                  is_sensitive INTEGER NOT NULL DEFAULT 0,
                  restricted_exports INTEGER NOT NULL DEFAULT 0,
                  redacted_fields TEXT,
                  project_id INTEGER,
                  -- share + flags
                  share_token TEXT,
                  require_enumerator_code INTEGER NOT NULL DEFAULT 0,
                  enable_gps INTEGER NOT NULL DEFAULT 0,
                  enable_coverage INTEGER NOT NULL DEFAULT 0,
                  coverage_scheme_id INTEGER,
                  collect_email INTEGER NOT NULL DEFAULT 0,
                  limit_one_response INTEGER NOT NULL DEFAULT 0,
                  allow_edit_response INTEGER NOT NULL DEFAULT 0,
                  show_summary_charts INTEGER NOT NULL DEFAULT 0,
                  confirmation_message TEXT
                )
                """
            )
        else:
            _add_column_if_missing(conn, "survey_templates", "created_by TEXT")
            _add_column_if_missing(conn, "survey_templates", "updated_at TEXT")
            _add_column_if_missing(conn, "survey_templates", "deleted_at TEXT")
            _add_column_if_missing(conn, "survey_templates", "source TEXT")
            _add_column_if_missing(conn, "survey_templates", "assignment_mode TEXT")
            _add_column_if_missing(conn, "survey_templates", "template_version TEXT")
            _add_column_if_missing(conn, "survey_templates", "enable_consent INTEGER NOT NULL DEFAULT 0")
            _add_column_if_missing(conn, "survey_templates", "enable_attestation INTEGER NOT NULL DEFAULT 0")
            _add_column_if_missing(conn, "survey_templates", "is_sensitive INTEGER NOT NULL DEFAULT 0")
            _add_column_if_missing(conn, "survey_templates", "restricted_exports INTEGER NOT NULL DEFAULT 0")
            _add_column_if_missing(conn, "survey_templates", "redacted_fields TEXT")
            _add_column_if_missing(conn, "survey_templates", "project_id INTEGER")
            _add_column_if_missing(conn, "survey_templates", "collect_email INTEGER NOT NULL DEFAULT 0")
            _add_column_if_missing(conn, "survey_templates", "limit_one_response INTEGER NOT NULL DEFAULT 0")
            _add_column_if_missing(conn, "survey_templates", "allow_edit_response INTEGER NOT NULL DEFAULT 0")
            _add_column_if_missing(conn, "survey_templates", "show_summary_charts INTEGER NOT NULL DEFAULT 0")
            _add_column_if_missing(conn, "survey_templates", "confirmation_message TEXT")

        if not _table_exists(conn, "template_questions"):
            cur.execute(
                """
                CREATE TABLE template_questions (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  template_id INTEGER NOT NULL,
                  question_text TEXT NOT NULL,
                  question_type TEXT NOT NULL DEFAULT 'TEXT',
                  display_order INTEGER NOT NULL DEFAULT 1,
                  is_required INTEGER NOT NULL DEFAULT 0,
                  help_text TEXT,
                  validation_json TEXT,
                  created_at TEXT,
                  FOREIGN KEY(template_id) REFERENCES survey_templates(id) ON DELETE CASCADE
                )
                """
            )
        else:
            _add_column_if_missing(conn, "template_questions", "help_text TEXT")
            _add_column_if_missing(conn, "template_questions", "validation_json TEXT")

        if not _table_exists(conn, "template_question_choices"):
            cur.execute(
                """
                CREATE TABLE template_question_choices (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  template_question_id INTEGER NOT NULL,
                  choice_text TEXT NOT NULL,
                  display_order INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT,
                  FOREIGN KEY(template_question_id) REFERENCES template_questions(id) ON DELETE CASCADE
                )
                """
            )

        if not _table_exists(conn, "surveys"):
            cur.execute(
                """
                CREATE TABLE surveys (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  facility_id INTEGER NOT NULL,
                  template_id INTEGER,
                  survey_type TEXT,
                  enumerator_name TEXT,
                  enumerator_code TEXT,
                  status TEXT NOT NULL DEFAULT 'DRAFT',
                  created_at TEXT,
                  completed_at TEXT,
                  respondent_email TEXT,
                  created_by TEXT,
                  source TEXT,
                  client_uuid TEXT,
                  client_created_at TEXT,
                  sync_source TEXT,
                  synced_at TEXT,
                  review_status TEXT,
                  review_reason TEXT,
                  reviewed_at TEXT,
                  reviewed_by TEXT,
                  -- GPS
                  gps_lat REAL,
                  gps_lng REAL,
                  gps_accuracy REAL,
                  gps_timestamp TEXT,
                  -- Coverage (optional)
                  coverage_node_id INTEGER,
                  coverage_node_name TEXT,
                  -- QA flags (optional)
                  qa_flags TEXT,
                  gps_missing_flag INTEGER NOT NULL DEFAULT 0,
                  duplicate_flag INTEGER NOT NULL DEFAULT 0,
                  -- Project linkage (optional)
                  project_id INTEGER,
                  enumerator_id INTEGER,
                  assignment_id INTEGER,
                  updated_at TEXT,
                  FOREIGN KEY(facility_id) REFERENCES facilities(id) ON DELETE CASCADE
                )
                """
            )
        else:
            _add_column_if_missing(conn, "surveys", "respondent_email TEXT")
            _add_column_if_missing(conn, "surveys", "created_by TEXT")
            _add_column_if_missing(conn, "surveys", "source TEXT")
            _add_column_if_missing(conn, "surveys", "client_uuid TEXT")
            _add_column_if_missing(conn, "surveys", "client_created_at TEXT")
            _add_column_if_missing(conn, "surveys", "sync_source TEXT")
            _add_column_if_missing(conn, "surveys", "synced_at TEXT")
            _add_column_if_missing(conn, "surveys", "review_status TEXT")
            _add_column_if_missing(conn, "surveys", "review_reason TEXT")
            _add_column_if_missing(conn, "surveys", "reviewed_at TEXT")
            _add_column_if_missing(conn, "surveys", "reviewed_by TEXT")
            _add_column_if_missing(conn, "surveys", "coverage_node_name TEXT")
            _add_column_if_missing(conn, "surveys", "qa_flags TEXT")
            _add_column_if_missing(conn, "surveys", "gps_missing_flag INTEGER NOT NULL DEFAULT 0")
            _add_column_if_missing(conn, "surveys", "duplicate_flag INTEGER NOT NULL DEFAULT 0")
            _add_column_if_missing(conn, "surveys", "updated_at TEXT")

        if not _table_exists(conn, "survey_answers"):
            cur.execute(
                """
                CREATE TABLE survey_answers (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  survey_id INTEGER NOT NULL,
                  template_question_id INTEGER,
                  question TEXT NOT NULL,
                  answer TEXT,
                  created_at TEXT,
                  -- QA support (optional)
                  answer_source TEXT,
                  confidence_level REAL,
                  is_missing INTEGER NOT NULL DEFAULT 0,
                  missing_reason TEXT,
                  FOREIGN KEY(survey_id) REFERENCES surveys(id) ON DELETE CASCADE
                )
                """
            )

        # -----------------------------
        # Qualitative interviews
        # -----------------------------
        if not _table_exists(conn, "qualitative_interviews"):
            cur.execute(
                """
                CREATE TABLE qualitative_interviews (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  project_id INTEGER NOT NULL,
                  enumerator_id INTEGER,
                  assignment_id INTEGER,
                  supervisor_id INTEGER,
                  interview_mode TEXT NOT NULL DEFAULT 'TEXT',
                  interview_text TEXT,
                  consent_obtained INTEGER,
                  consent_timestamp TEXT,
                  audio_recording_allowed INTEGER,
                  audio_confirmed INTEGER,
                  audio_file_url TEXT,
                  transcript_status TEXT NOT NULL DEFAULT 'NONE',
                  transcript_text TEXT,
                  transcript_approved_by INTEGER,
                  transcript_approved_at TEXT,
                  created_at TEXT,
                  updated_at TEXT,
                  FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
                )
                """
            )
        else:
            _add_column_if_missing(conn, "qualitative_interviews", "enumerator_id INTEGER")
            _add_column_if_missing(conn, "qualitative_interviews", "assignment_id INTEGER")
            _add_column_if_missing(conn, "qualitative_interviews", "supervisor_id INTEGER")
            _add_column_if_missing(conn, "qualitative_interviews", "interview_mode TEXT NOT NULL DEFAULT 'TEXT'")
            _add_column_if_missing(conn, "qualitative_interviews", "interview_text TEXT")
            _add_column_if_missing(conn, "qualitative_interviews", "consent_obtained INTEGER")
            _add_column_if_missing(conn, "qualitative_interviews", "consent_timestamp TEXT")
            _add_column_if_missing(conn, "qualitative_interviews", "audio_recording_allowed INTEGER")
            _add_column_if_missing(conn, "qualitative_interviews", "audio_confirmed INTEGER")
            _add_column_if_missing(conn, "qualitative_interviews", "audio_file_url TEXT")
            _add_column_if_missing(conn, "qualitative_interviews", "transcript_status TEXT NOT NULL DEFAULT 'NONE'")
            _add_column_if_missing(conn, "qualitative_interviews", "transcript_text TEXT")
            _add_column_if_missing(conn, "qualitative_interviews", "transcript_approved_by INTEGER")
            _add_column_if_missing(conn, "qualitative_interviews", "transcript_approved_at TEXT")
            _add_column_if_missing(conn, "qualitative_interviews", "created_at TEXT")
            _add_column_if_missing(conn, "qualitative_interviews", "updated_at TEXT")

        # -----------------------------
        # Coverage schemes + nodes
        # -----------------------------
        if not _table_exists(conn, "coverage_schemes"):
            cur.execute(
                """
                CREATE TABLE coverage_schemes (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  name TEXT NOT NULL,
                  description TEXT,
                  created_at TEXT
                )
                """
            )
        else:
            _add_column_if_missing(conn, "coverage_schemes", "description TEXT")

        if not _table_exists(conn, "coverage_nodes"):
            cur.execute(
                """
                CREATE TABLE coverage_nodes (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  scheme_id INTEGER NOT NULL,
                  name TEXT NOT NULL,
                  parent_id INTEGER,
                  level_index INTEGER NOT NULL DEFAULT 0,
                  gps_lat REAL,
                  gps_lng REAL,
                  gps_radius_m REAL,
                  created_at TEXT,
                  FOREIGN KEY(scheme_id) REFERENCES coverage_schemes(id) ON DELETE CASCADE
                )
                """
            )
        else:
            _add_column_if_missing(conn, "coverage_nodes", "level_index INTEGER NOT NULL DEFAULT 0")
            _add_column_if_missing(conn, "coverage_nodes", "gps_lat REAL")
            _add_column_if_missing(conn, "coverage_nodes", "gps_lng REAL")
            _add_column_if_missing(conn, "coverage_nodes", "gps_radius_m REAL")

        # -----------------------------
        # NEW: Projects + Enumerators + Assignments
        # -----------------------------
        if not _table_exists(conn, "projects"):
            cur.execute(
                """
                CREATE TABLE projects (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  name TEXT NOT NULL,
                  description TEXT,
                  template_id INTEGER,
                  project_tag TEXT NOT NULL,
                  is_active INTEGER NOT NULL DEFAULT 1,
                  status TEXT,
                  assignment_mode TEXT,
                  is_test_project INTEGER NOT NULL DEFAULT 0,
                  is_live_project INTEGER NOT NULL DEFAULT 0,
                  expected_submissions INTEGER,
                  expected_coverage INTEGER,
                  coverage_scheme_id INTEGER,
                  allow_unlisted_facilities INTEGER NOT NULL DEFAULT 0,
                  organization_id INTEGER,
                  owner_name TEXT,
                  created_by TEXT,
                  source TEXT,
                  updated_at TEXT,
                  created_at TEXT,
                  FOREIGN KEY(template_id) REFERENCES survey_templates(id)
                )
                """
            )
        else:
            _add_column_if_missing(conn, "projects", "template_id INTEGER")
            _add_column_if_missing(conn, "projects", "project_tag TEXT")
            _add_column_if_missing(conn, "projects", "is_active INTEGER NOT NULL DEFAULT 1")
            _add_column_if_missing(conn, "projects", "status TEXT")
            _add_column_if_missing(conn, "projects", "assignment_mode TEXT")
            _add_column_if_missing(conn, "projects", "is_test_project INTEGER NOT NULL DEFAULT 0")
            _add_column_if_missing(conn, "projects", "is_live_project INTEGER NOT NULL DEFAULT 0")
            _add_column_if_missing(conn, "projects", "expected_submissions INTEGER")
            _add_column_if_missing(conn, "projects", "expected_coverage INTEGER")
            _add_column_if_missing(conn, "projects", "coverage_scheme_id INTEGER")
            _add_column_if_missing(conn, "projects", "allow_unlisted_facilities INTEGER NOT NULL DEFAULT 0")
            _add_column_if_missing(conn, "projects", "organization_id INTEGER")
            _add_column_if_missing(conn, "projects", "owner_name TEXT")
            _add_column_if_missing(conn, "projects", "created_by TEXT")
            _add_column_if_missing(conn, "projects", "source TEXT")
            _add_column_if_missing(conn, "projects", "updated_at TEXT")

            # Backfill project_tag if missing/empty
            try:
                cur.execute("SELECT id, name, project_tag FROM projects")
                rows = cur.fetchall()
                for r in rows:
                    if not r["project_tag"]:
                        tag = _generate_project_tag(r["name"] or "Project")
                        cur.execute("UPDATE projects SET project_tag=? WHERE id=?", (tag, int(r["id"])))
            except Exception:
                pass

        # Backfill template project_id when only one project exists
        try:
            if _table_exists(conn, "survey_templates") and _table_exists(conn, "projects"):
                cur.execute("SELECT COUNT(*) AS c FROM projects")
                total_projects = int(cur.fetchone()["c"] or 0)
                if total_projects == 1:
                    cur.execute("SELECT id FROM projects LIMIT 1")
                    pr = cur.fetchone()
                    if pr:
                        cur.execute(
                            "UPDATE survey_templates SET project_id=? WHERE project_id IS NULL",
                            (int(pr["id"]),),
                        )
        except Exception:
            pass

        # -----------------------------
        # Organizations + Supervisors (platform mode)
        # -----------------------------
        if not _table_exists(conn, "organizations"):
            cur.execute(
                """
                CREATE TABLE organizations (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  name TEXT NOT NULL,
                  org_type TEXT,
                  country TEXT,
                  region TEXT,
                  sector TEXT,
                  size TEXT,
                  website TEXT,
                  domain TEXT,
                  logo_path TEXT,
                  address TEXT,
                  created_at TEXT,
                  updated_at TEXT
                )
                """
            )
        else:
            _add_column_if_missing(conn, "organizations", "org_type TEXT")
            _add_column_if_missing(conn, "organizations", "country TEXT")
            _add_column_if_missing(conn, "organizations", "region TEXT")
            _add_column_if_missing(conn, "organizations", "sector TEXT")
            _add_column_if_missing(conn, "organizations", "size TEXT")
            _add_column_if_missing(conn, "organizations", "website TEXT")
            _add_column_if_missing(conn, "organizations", "domain TEXT")
            _add_column_if_missing(conn, "organizations", "logo_path TEXT")
            _add_column_if_missing(conn, "organizations", "address TEXT")
            _add_column_if_missing(conn, "organizations", "updated_at TEXT")

        if not _table_exists(conn, "supervisors"):
            cur.execute(
                """
                CREATE TABLE supervisors (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  organization_id INTEGER,
                  full_name TEXT NOT NULL,
                  email TEXT,
                  phone TEXT,
                  access_key TEXT NOT NULL,
                  status TEXT NOT NULL DEFAULT 'ACTIVE',
                  created_at TEXT
                )
                """
            )

        if not _table_exists(conn, "users"):
            cur.execute(
                """
                CREATE TABLE users (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  organization_id INTEGER,
                  full_name TEXT NOT NULL,
                  email TEXT NOT NULL UNIQUE,
                  password_hash TEXT,
                  role TEXT NOT NULL DEFAULT 'OWNER',
                  title TEXT,
                  phone TEXT,
                  status TEXT NOT NULL DEFAULT 'ACTIVE',
                  email_verified INTEGER NOT NULL DEFAULT 0,
                  verified_at TEXT,
                  last_login_at TEXT,
                  created_at TEXT,
                  profile_image_path TEXT,
                  auth_provider TEXT NOT NULL DEFAULT 'local',
                  google_sub TEXT,
                  updated_at TEXT
                )
                """
            )

        # Keep users schema consistent for both fresh and existing databases.
        _add_column_if_missing(conn, "users", "organization_id INTEGER")
        _add_column_if_missing(conn, "users", "full_name TEXT")
        _add_column_if_missing(conn, "users", "email TEXT")
        _add_column_if_missing(conn, "users", "password_hash TEXT")
        _add_column_if_missing(conn, "users", "role TEXT NOT NULL DEFAULT 'OWNER'")
        _add_column_if_missing(conn, "users", "title TEXT")
        _add_column_if_missing(conn, "users", "phone TEXT")
        _add_column_if_missing(conn, "users", "status TEXT NOT NULL DEFAULT 'ACTIVE'")
        _add_column_if_missing(conn, "users", "email_verified INTEGER NOT NULL DEFAULT 0")
        _add_column_if_missing(conn, "users", "verified_at TEXT")
        _add_column_if_missing(conn, "users", "last_login_at TEXT")
        _add_column_if_missing(conn, "users", "created_at TEXT")
        _add_column_if_missing(conn, "users", "profile_image_path TEXT")
        _add_column_if_missing(conn, "users", "auth_provider TEXT NOT NULL DEFAULT 'local'")
        _add_column_if_missing(conn, "users", "google_sub TEXT")
        _add_column_if_missing(conn, "users", "updated_at TEXT")

        if not _table_exists(conn, "user_tokens"):
            cur.execute(
                """
                CREATE TABLE user_tokens (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER NOT NULL,
                  token TEXT NOT NULL,
                  token_type TEXT NOT NULL,
                  expires_at TEXT,
                  used_at TEXT,
                  created_at TEXT,
                  UNIQUE(token),
                  FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                )
                """
            )

        if not _table_exists(conn, "user_invites"):
            cur.execute(
                """
                CREATE TABLE user_invites (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  organization_id INTEGER NOT NULL,
                  email TEXT NOT NULL,
                  role TEXT NOT NULL DEFAULT 'SUPERVISOR',
                  token TEXT NOT NULL,
                  status TEXT NOT NULL DEFAULT 'PENDING',
                  expires_at TEXT,
                  created_by INTEGER,
                  created_at TEXT,
                  used_at TEXT,
                  UNIQUE(token)
                )
                """
            )

        if not _table_exists(conn, "sessions"):
            cur.execute(
                """
                CREATE TABLE sessions (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER NOT NULL,
                  session_token_hash TEXT NOT NULL,
                  device_label TEXT,
                  ip_address TEXT,
                  user_agent TEXT,
                  created_at TEXT,
                  last_seen_at TEXT,
                  revoked_at TEXT,
                  UNIQUE(session_token_hash),
                  FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                )
                """
            )
        else:
            _add_column_if_missing(conn, "sessions", "device_label TEXT")
            _add_column_if_missing(conn, "sessions", "ip_address TEXT")
            _add_column_if_missing(conn, "sessions", "user_agent TEXT")
            _add_column_if_missing(conn, "sessions", "created_at TEXT")
            _add_column_if_missing(conn, "sessions", "last_seen_at TEXT")
            _add_column_if_missing(conn, "sessions", "revoked_at TEXT")

        if not _table_exists(conn, "security_events"):
            cur.execute(
                """
                CREATE TABLE security_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  event_type TEXT NOT NULL,
                  ip_address TEXT,
                  user_agent TEXT,
                  created_at TEXT,
                  meta_json TEXT,
                  FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                )
                """
            )
        else:
            _add_column_if_missing(conn, "security_events", "user_id INTEGER")
            _add_column_if_missing(conn, "security_events", "event_type TEXT")
            _add_column_if_missing(conn, "security_events", "ip_address TEXT")
            _add_column_if_missing(conn, "security_events", "user_agent TEXT")
            _add_column_if_missing(conn, "security_events", "created_at TEXT")
            _add_column_if_missing(conn, "security_events", "meta_json TEXT")

        if not _table_exists(conn, "user_security_settings"):
            cur.execute(
                """
                CREATE TABLE user_security_settings (
                  user_id INTEGER PRIMARY KEY,
                  notify_new_login INTEGER NOT NULL DEFAULT 1,
                  notify_password_change INTEGER NOT NULL DEFAULT 1,
                  notify_oauth_changes INTEGER NOT NULL DEFAULT 1,
                  updated_at TEXT,
                  FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                )
                """
            )
        else:
            _add_column_if_missing(conn, "user_security_settings", "notify_new_login INTEGER NOT NULL DEFAULT 1")
            _add_column_if_missing(conn, "user_security_settings", "notify_password_change INTEGER NOT NULL DEFAULT 1")
            _add_column_if_missing(conn, "user_security_settings", "notify_oauth_changes INTEGER NOT NULL DEFAULT 1")
            _add_column_if_missing(conn, "user_security_settings", "updated_at TEXT")

        if not _table_exists(conn, "audit_logs"):
            cur.execute(
                """
                CREATE TABLE audit_logs (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  organization_id INTEGER,
                  actor_user_id INTEGER,
                  action TEXT NOT NULL,
                  target_type TEXT,
                  target_id INTEGER,
                  meta_json TEXT,
                  created_at TEXT
                )
                """
            )

        if not _table_exists(conn, "enumerators"):
            cur.execute(
                """
                CREATE TABLE enumerators (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  project_id INTEGER,
                  full_name TEXT,
                  name TEXT,
                  code TEXT,
                  phone TEXT,
                  email TEXT,
                  status TEXT,
                  is_active INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT
                )
                """
            )
        else:
            _add_column_if_missing(conn, "enumerators", "project_id INTEGER")
            _add_column_if_missing(conn, "enumerators", "full_name TEXT")
            _add_column_if_missing(conn, "enumerators", "name TEXT")
            _add_column_if_missing(conn, "enumerators", "code TEXT")
            _add_column_if_missing(conn, "enumerators", "phone TEXT")
            _add_column_if_missing(conn, "enumerators", "email TEXT")
            _add_column_if_missing(conn, "enumerators", "status TEXT")
            _add_column_if_missing(conn, "enumerators", "is_active INTEGER NOT NULL DEFAULT 1")

        if not _table_exists(conn, "enumerator_assignments"):
            cur.execute(
                """
                CREATE TABLE enumerator_assignments (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  project_id INTEGER NOT NULL,
                  enumerator_id INTEGER NOT NULL,
                  supervisor_id INTEGER,
                  coverage_node_id INTEGER,
                  template_id INTEGER,
                  scheme_id INTEGER,
                  coverage_label TEXT,                  -- optional label (legacy)
                  target_facilities_count INTEGER,      -- e.g., 8
                  code_serial INTEGER,                  -- sequential per project
                  code_full TEXT,                       -- e.g., LGSQ1A9-EN-0042-K7
                  is_active INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT,
                  UNIQUE(project_id, code_serial),
                  UNIQUE(project_id, enumerator_id),
                  UNIQUE(code_full),
                  FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE,
                  FOREIGN KEY(enumerator_id) REFERENCES enumerators(id) ON DELETE CASCADE
                )
                """
            )
        else:
            _add_column_if_missing(conn, "enumerator_assignments", "project_id INTEGER")
            _add_column_if_missing(conn, "enumerator_assignments", "enumerator_id INTEGER")
            _add_column_if_missing(conn, "enumerator_assignments", "supervisor_id INTEGER")
            _add_column_if_missing(conn, "enumerator_assignments", "coverage_node_id INTEGER")
            _add_column_if_missing(conn, "enumerator_assignments", "template_id INTEGER")
            _add_column_if_missing(conn, "enumerator_assignments", "scheme_id INTEGER")
            _add_column_if_missing(conn, "enumerator_assignments", "coverage_label TEXT")
            _add_column_if_missing(conn, "enumerator_assignments", "target_facilities_count INTEGER")
            _add_column_if_missing(conn, "enumerator_assignments", "code_serial INTEGER")
            _add_column_if_missing(conn, "enumerator_assignments", "code_full TEXT")
            _add_column_if_missing(conn, "enumerator_assignments", "is_active INTEGER NOT NULL DEFAULT 1")
            _add_column_if_missing(conn, "enumerator_assignments", "code_serial INTEGER")
            _add_column_if_missing(conn, "enumerator_assignments", "code_full TEXT")
            _add_column_if_missing(conn, "enumerator_assignments", "is_active INTEGER NOT NULL DEFAULT 1")
            _add_column_if_missing(conn, "enumerator_assignments", "created_at TEXT")

        if not _table_exists(conn, "assignment_facilities"):
            cur.execute(
                """
                CREATE TABLE assignment_facilities (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  assignment_id INTEGER NOT NULL,
                  facility_id INTEGER NOT NULL,
                  status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING/DONE
                  done_survey_id INTEGER,
                  created_at TEXT,
                  UNIQUE(assignment_id, facility_id),
                  FOREIGN KEY(assignment_id) REFERENCES enumerator_assignments(id) ON DELETE CASCADE,
                  FOREIGN KEY(facility_id) REFERENCES facilities(id) ON DELETE CASCADE
                )
                """
            )

        # NEW: assignment coverage nodes (multi-LGA support)
        if not _table_exists(conn, "assignment_coverage_nodes"):
            cur.execute(
                """
                CREATE TABLE assignment_coverage_nodes (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  assignment_id INTEGER NOT NULL,
                  coverage_node_id INTEGER NOT NULL,
                  created_at TEXT,
                  UNIQUE(assignment_id, coverage_node_id),
                  FOREIGN KEY(assignment_id) REFERENCES enumerator_assignments(id) ON DELETE CASCADE
                )
                """
            )

        # NEW: supervisor coverage nodes (multi-LGA support)
        if not _table_exists(conn, "supervisor_coverage_nodes"):
            cur.execute(
                """
                CREATE TABLE supervisor_coverage_nodes (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  supervisor_id INTEGER NOT NULL,
                  project_id INTEGER NOT NULL,
                  coverage_node_id INTEGER NOT NULL,
                  created_at TEXT,
                  UNIQUE(supervisor_id, project_id, coverage_node_id),
                  FOREIGN KEY(supervisor_id) REFERENCES supervisors(id) ON DELETE CASCADE,
                  FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
                )
                """
            )

        # -----------------------------
        # MIGRATIONS: surveys table gets project/enumerator linkage
        # -----------------------------
        # These columns allow us to bind submissions to:
        # - project
        # - enumerator identity
        # - assignment context (optional)
        _add_column_if_missing(conn, "surveys", "project_id INTEGER")
        _add_column_if_missing(conn, "surveys", "enumerator_id INTEGER")
        _add_column_if_missing(conn, "surveys", "assignment_id INTEGER")
        _add_column_if_missing(conn, "surveys", "consent_signature TEXT")
        _add_column_if_missing(conn, "surveys", "consent_signature_ts TEXT")
        _add_column_if_missing(conn, "surveys", "supervisor_id INTEGER")

        # If you later want to enforce via FK constraints, keep it as logical references for now.
        # SQLite can't easily add foreign keys via ALTER TABLE, so we store ids and handle in code.

        # -----------------------------
        # INDEXES (performance)
        # -----------------------------
        cur.execute("CREATE INDEX IF NOT EXISTS idx_facilities_name ON facilities(name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_surveys_status ON surveys(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_surveys_enum_name ON surveys(enumerator_name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_surveys_project ON surveys(project_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_surveys_coverage_node ON surveys(coverage_node_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_surveys_enum_id ON surveys(enumerator_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_surveys_supervisor ON surveys(supervisor_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_surveys_email ON surveys(respondent_email)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_surveys_client_uuid ON surveys(client_uuid)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_surveys_review_status ON surveys(review_status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_templates_project ON survey_templates(project_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_projects_org ON projects(organization_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_supervisors_org ON supervisors(organization_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_org_domain ON organizations(domain)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_org ON users(organization_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_google_sub ON users(google_sub)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_tokens_user ON user_tokens(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_tokens_type ON user_tokens(token_type)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_invites_org ON user_invites(organization_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_invites_email ON user_invites(email)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_invites_status ON user_invites(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_hash ON sessions(session_token_hash)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_revoked ON sessions(revoked_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sec_events_user ON security_events(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sec_events_type ON security_events(event_type)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_org ON audit_logs(organization_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_actor ON audit_logs(actor_user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_assign_project ON enumerator_assignments(project_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_assign_enum ON enumerator_assignments(enumerator_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_assign_supervisor ON enumerator_assignments(supervisor_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_assign_code ON enumerator_assignments(code_full)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_assign_fac ON assignment_facilities(assignment_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_interviews_project ON qualitative_interviews(project_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_interviews_enum ON qualitative_interviews(enumerator_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_assign_cov ON assignment_coverage_nodes(assignment_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sup_cov ON supervisor_coverage_nodes(supervisor_id)")

        conn.commit()
