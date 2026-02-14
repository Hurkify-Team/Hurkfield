# projects.py — Projects + Enumerator Code System (Project-aware codes)
import os
import re
import hmac
import hashlib
from datetime import datetime
from typing import Optional, Dict, Any, Tuple, List

from db import get_conn

# Secret used to generate checksum. Set this in your environment for real usage.
# Example (macOS): export OPENFIELD_CODE_SECRET="some-long-random-string"
CODE_SECRET = os.environ.get("OPENFIELD_CODE_SECRET", "openfield-dev-secret").encode("utf-8")


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _table_exists(conn, table: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    )
    return cur.fetchone() is not None


def _table_columns(conn, table: str) -> list[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return [r["name"] for r in cur.fetchall()]


def _normalize_words(name: str) -> str:
    n = (name or "").strip().upper()
    n = re.sub(r"[^A-Z0-9\s\-]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def generate_project_tag(project_name: str, project_id_hint: Optional[int] = None) -> str:
    """
    Deterministic-ish tag:
    - Uses first letters of up to 3 keywords (>=3 chars)
    - Adds a 2-char base32-ish hash suffix from project name (+ optional id hint)
    Example: "Lagos State Facility Assessment Q1 2026" -> "LSF-A9" (but we remove hyphen in final)
    We'll output: LSF A9 -> "LSFA9"
    """
    clean = _normalize_words(project_name)
    words = [w for w in clean.split(" ") if len(w) >= 3 and w not in ("THE", "AND", "FOR", "WITH")]
    prefix = "".join([w[0] for w in words[:3]]) or "PRJ"

    # hash suffix
    seed = f"{clean}|{project_id_hint or ''}".encode("utf-8")
    digest = hashlib.sha256(seed).hexdigest()
    # take 2 bytes -> 4 hex chars, map to alphanum
    raw = digest[:4].upper()
    # simple mapping: use hex directly (0-9A-F) but keep it short
    suffix = raw[:2]  # 2 chars

    tag = f"{prefix}{suffix}"
    tag = re.sub(r"[^A-Z0-9]", "", tag)[:8]
    return tag


def _checksum(project_id: int, enumerator_id: int, serial: int) -> str:
    msg = f"{project_id}|{enumerator_id}|{serial}".encode("utf-8")
    digest = hmac.new(CODE_SECRET, msg, hashlib.sha256).digest()
    # base32-ish without importing base64 for simplicity: use hex then pick 2 chars
    hx = digest.hex().upper()
    return hx[:2]  # 2 chars anti-typo + anti-guess


def format_code(project_tag: str, serial: int, check: str) -> str:
    return f"{project_tag}-EN-{serial:04d}-{check}"


def create_project(name: str, description: str = "", template_id: Optional[int] = None) -> int:
    """
    Creates a project with an auto project_tag.
    """
    n = (name or "").strip()
    if not n:
        raise ValueError("Project name is required.")

    # insert first to get id, then update tag with id hint if you want (we keep stable enough without)
    with get_conn() as conn:
        cur = conn.cursor()
        tag = generate_project_tag(n, None)
        cur.execute(
            """
            INSERT INTO projects (name, description, template_id, project_tag, is_active, created_at)
            VALUES (?, ?, ?, ?, 1, ?)
            """,
            (n, description.strip(), template_id, tag, now_iso()),
        )
        conn.commit()
        return int(cur.lastrowid)


def get_project_by_tag(tag: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM projects WHERE project_tag=? LIMIT 1", (tag,))
        return cur.fetchone()


def _next_serial(project_id: int) -> int:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT MAX(code_serial) AS mx FROM enumerator_assignments WHERE project_id=?",
            (int(project_id),),
        )
        r = cur.fetchone()
        mx = int(r["mx"] or 0)
        return mx + 1


def create_or_get_enumerator(full_name: str, phone: str = "", email: str = "") -> int:
    name = (full_name or "").strip()
    if not name:
        raise ValueError("Enumerator full_name is required.")

    with get_conn() as conn:
        cur = conn.cursor()
        # best-effort reuse by case-insensitive name match
        cur.execute("SELECT id FROM enumerators WHERE LOWER(full_name)=LOWER(?) LIMIT 1", (name,))
        r = cur.fetchone()
        if r:
            return int(r["id"])

        cur.execute(
            """
            INSERT INTO enumerators (full_name, phone, email, is_active, created_at)
            VALUES (?, ?, ?, 1, ?)
            """,
            (name, phone.strip() or None, email.strip() or None, now_iso()),
        )
        conn.commit()
        return int(cur.lastrowid)


def assign_enumerator_to_project(
    project_id: int,
    enumerator_id: int,
    coverage_label: str = "",
    target_facilities_count: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Creates assignment and code automatically.
    Returns {assignment_id, code_full, code_serial}
    """
    with get_conn() as conn:
        cur = conn.cursor()

        # fetch project tag
        cur.execute("SELECT project_tag FROM projects WHERE id=? LIMIT 1", (int(project_id),))
        pr = cur.fetchone()
        if not pr:
            raise ValueError("Project not found.")
        tag = (pr["project_tag"] or "").strip()
        if not tag:
            raise ValueError("Project tag missing.")

        # if already assigned, return existing
        cur.execute(
            """
            SELECT id, code_full, code_serial
            FROM enumerator_assignments
            WHERE project_id=? AND enumerator_id=?
            LIMIT 1
            """,
            (int(project_id), int(enumerator_id)),
        )
        existing = cur.fetchone()
        if existing:
            return {
                "assignment_id": int(existing["id"]),
                "code_full": existing["code_full"],
                "code_serial": int(existing["code_serial"]),
            }

        serial = _next_serial(int(project_id))
        check = _checksum(int(project_id), int(enumerator_id), int(serial))
        code_full = format_code(tag, serial, check)

        cur.execute(
            """
            INSERT INTO enumerator_assignments
              (project_id, enumerator_id, coverage_label, target_facilities_count,
               code_serial, code_full, is_active, created_at)
            VALUES (?, ?, ?, ?, ?, ?, 1, ?)
            """,
            (
                int(project_id),
                int(enumerator_id),
                (coverage_label.strip() or None),
                (int(target_facilities_count) if target_facilities_count is not None else None),
                int(serial),
                code_full,
                now_iso(),
            ),
        )
        conn.commit()
        return {
            "assignment_id": int(cur.lastrowid),
            "code_full": code_full,
            "code_serial": serial,
        }


def ensure_assignment_code(project_id: int, enumerator_id: int, assignment_id: int) -> Dict[str, Any]:
    """
    Ensures an existing assignment row has code_serial + code_full.
    Returns {assignment_id, code_full, code_serial}
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT code_serial, code_full FROM enumerator_assignments WHERE id=? LIMIT 1",
            (int(assignment_id),),
        )
        row = cur.fetchone()
        if row and row["code_full"]:
            return {
                "assignment_id": int(assignment_id),
                "code_full": row["code_full"],
                "code_serial": int(row["code_serial"] or 0),
            }

        try:
            cur.execute("SELECT project_tag, name FROM projects WHERE id=? LIMIT 1", (int(project_id),))
            pr = cur.fetchone()
        except Exception:
            pr = None
        if not pr:
            raise ValueError("Project not found.")
        tag = (pr["project_tag"] or "").strip() if "project_tag" in pr.keys() else ""
        if not tag:
            tag = generate_project_tag(pr["name"] if "name" in pr.keys() else "Project", project_id)
            try:
                cur.execute("UPDATE projects SET project_tag=? WHERE id=?", (tag, int(project_id)))
            except Exception:
                pass

        serial = _next_serial(int(project_id))
        check = _checksum(int(project_id), int(enumerator_id), int(serial))
        code_full = format_code(tag, serial, check)

        cur.execute(
            """
            UPDATE enumerator_assignments
            SET code_serial=?, code_full=?
            WHERE id=?
            """,
            (int(serial), code_full, int(assignment_id)),
        )
        conn.commit()
        return {
            "assignment_id": int(assignment_id),
            "code_full": code_full,
            "code_serial": int(serial),
        }


def ensure_assignment_code_template(template_id: int, enumerator_id: int, assignment_id: int) -> Dict[str, Any]:
    """
    Ensures an assignment has a code when no project exists (template-only mode).
    Uses the template name to generate a short tag.
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT code_serial, code_full FROM enumerator_assignments WHERE id=? LIMIT 1",
            (int(assignment_id),),
        )
        row = cur.fetchone()
        if row and row["code_full"]:
            return {
                "assignment_id": int(assignment_id),
                "code_full": row["code_full"],
                "code_serial": int(row["code_serial"] or 0),
            }

        cur.execute("SELECT name FROM survey_templates WHERE id=? LIMIT 1", (int(template_id),))
        tpl = cur.fetchone()
        if not tpl:
            raise ValueError("Template not found.")
        tag = generate_project_tag(tpl["name"] or "Template", int(template_id))

        cur.execute("SELECT project_id FROM enumerator_assignments WHERE id=? LIMIT 1", (int(assignment_id),))
        ar = cur.fetchone()
        internal_project_id = int(ar["project_id"] or 0) if ar else 0

        # serial per template within the internal project
        cur.execute(
            """
            SELECT MAX(code_serial) AS mx
            FROM enumerator_assignments
            WHERE template_id=? AND project_id=?
            """,
            (int(template_id), int(internal_project_id)),
        )
        r = cur.fetchone()
        serial = int(r["mx"] or 0) + 1

        check = _checksum(int(template_id), int(enumerator_id), int(serial))
        code_full = format_code(tag, serial, check)

        cur.execute(
            """
            UPDATE enumerator_assignments
            SET code_serial=?, code_full=?
            WHERE id=?
            """,
            (int(serial), code_full, int(assignment_id)),
        )
        conn.commit()
        return {
            "assignment_id": int(assignment_id),
            "code_full": code_full,
            "code_serial": int(serial),
        }


def get_template_only_project_id(organization_id: Optional[int]) -> Optional[int]:
    with get_conn() as conn:
        if not _table_exists(conn, "projects"):
            return None
        cols = _columns(conn, "projects")
        if "source" not in cols:
            return None
        cur = conn.cursor()
        if organization_id is not None and "organization_id" in cols:
            cur.execute(
                "SELECT id FROM projects WHERE organization_id=? AND source='system_template_only' LIMIT 1",
                (int(organization_id),),
            )
        else:
            cur.execute("SELECT id FROM projects WHERE source='system_template_only' LIMIT 1")
        row = cur.fetchone()
        return int(row["id"]) if row else None


def get_or_create_template_only_project(organization_id: Optional[int]) -> int:
    existing = get_template_only_project_id(organization_id)
    if existing:
        return int(existing)
    return create_project(
        "Template-only Workspace",
        "System internal project for template-only assignments.",
        source="system_template_only",
        assignment_mode="OPTIONAL",
        is_test_project=1,
        status="ACTIVE",
        organization_id=organization_id,
    )


def validate_enumerator_code(code: str) -> Dict[str, Any]:
    """
    Validates a code string and returns assignment context:
    {
      ok, error?,
      project_id, project_tag,
      enumerator_id, enumerator_name,
      assignment_id, coverage_label, target_facilities_count,
      code_serial
    }
    """
    c = (code or "").strip().upper()
    if not c:
        return {"ok": False, "error": "Code is required."}

    # Expected: TAG-EN-0001-CC
    parts = c.split("-")
    if len(parts) != 4:
        return {"ok": False, "error": "Invalid code format."}

    project_tag, role, serial_s, check = parts
    if role != "EN":
        return {"ok": False, "error": "Invalid role in code."}
    if not serial_s.isdigit():
        return {"ok": False, "error": "Invalid serial in code."}

    serial = int(serial_s)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM projects WHERE project_tag=? LIMIT 1", (project_tag,))
        proj = cur.fetchone()
        if not proj:
            return {"ok": False, "error": "Project not found for this code."}

        project_id = int(proj["id"])

        # Find assignment by project + serial
        cur.execute(
            """
            SELECT ea.*, e.full_name AS enumerator_name
            FROM enumerator_assignments ea
            JOIN enumerators e ON e.id = ea.enumerator_id
            WHERE ea.project_id=? AND ea.code_serial=?
            LIMIT 1
            """,
            (project_id, serial),
        )
        a = cur.fetchone()
        if not a:
            return {"ok": False, "error": "Assignment not found for this code."}
        if int(a["is_active"] or 1) != 1:
            return {"ok": False, "error": "This enumerator assignment is disabled."}

        enumerator_id = int(a["enumerator_id"])
        expected = _checksum(project_id, enumerator_id, serial)
        if expected != check:
            return {"ok": False, "error": "Invalid code (checksum mismatch)."}

        return {
            "ok": True,
            "project_id": project_id,
            "project_tag": project_tag,
            "enumerator_id": enumerator_id,
            "enumerator_name": a["enumerator_name"],
            "assignment_id": int(a["id"]),
            "coverage_label": a["coverage_label"],
            "target_facilities_count": a["target_facilities_count"],
            "code_serial": serial,
            "code_full": a["code_full"],
        }


# -----------------------------
# Compatibility helpers for app.py
# -----------------------------

def _table_exists(conn, name: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,))
    return cur.fetchone() is not None


def _columns(conn, table: str):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return [r["name"] for r in cur.fetchall()]


def _safe_int(x, default=None):
    try:
        return int(x)
    except Exception:
        return default


def list_projects(limit: int = 200, organization_id: Optional[int] = None, include_system: bool = False):
    if limit is None:
        limit = 200
    with get_conn() as conn:
        if not _table_exists(conn, "projects"):
            return []
        cols = _columns(conn, "projects")
        cur = conn.cursor()
        where = []
        params: List[Any] = []
        if organization_id is not None and "organization_id" in cols:
            where.append("organization_id=?")
            params.append(int(organization_id))
        if not include_system and "source" in cols:
            where.append("(source IS NULL OR source!='system_template_only')")
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        params.append(int(limit))
        cur.execute(
            f"SELECT * FROM projects {where_sql} ORDER BY id DESC LIMIT ?",
            tuple(params),
        )
        rows = cur.fetchall()
    out = []
    for r in rows:
        d = dict(r)
        status = d.get("status")
        if not status:
            is_active = int(d.get("is_active") or 0)
            status = "ACTIVE" if is_active == 1 else "ARCHIVED"
        d.setdefault("status", status)
        d.setdefault("owner_name", d.get("owner_name") or "")
        d.setdefault("created_by", d.get("created_by") or "")
        d.setdefault("updated_at", d.get("updated_at") or "")
        d.setdefault("source", d.get("source") or "")
        d.setdefault("assignment_mode", d.get("assignment_mode") or "OPTIONAL")
        d.setdefault("is_test_project", int(d.get("is_test_project") or 0))
        d.setdefault("is_live_project", int(d.get("is_live_project") or 0))
        d.setdefault("expected_submissions", d.get("expected_submissions"))
        d.setdefault("expected_coverage", d.get("expected_coverage"))
        d.setdefault("organization_id", d.get("organization_id"))
        d.setdefault("allow_unlisted_facilities", int(d.get("allow_unlisted_facilities") or 0))
        out.append(d)
    return out


def get_project(project_id: int):
    with get_conn() as conn:
        if not _table_exists(conn, "projects"):
            return None
        cur = conn.cursor()
        cur.execute("SELECT * FROM projects WHERE id=? LIMIT 1", (int(project_id),))
        row = cur.fetchone()
    if not row:
        return None
    d = dict(row)
    if not d.get("status"):
        is_active = int(d.get("is_active") or 0)
        d["status"] = "ACTIVE" if is_active == 1 else "ARCHIVED"
    d.setdefault("assignment_mode", d.get("assignment_mode") or "OPTIONAL")
    d.setdefault("allow_unlisted_facilities", int(d.get("allow_unlisted_facilities") or 0))
    return d


def update_project(
    project_id: int,
    name: Optional[str] = None,
    description: Optional[str] = None,
    assignment_mode: Optional[str] = None,
    is_test_project: Optional[int] = None,
    is_live_project: Optional[int] = None,
    status: Optional[str] = None,
    expected_submissions: Optional[int] = None,
    expected_coverage: Optional[int] = None,
    organization_id: Optional[int] = None,
    allow_unlisted_facilities: Optional[int] = None,
    coverage_scheme_id: Optional[int] = None,
) -> None:
    with get_conn() as conn:
        if not _table_exists(conn, "projects"):
            return
        cols = _columns(conn, "projects")
        fields = []
        values = []
        if name is not None and "name" in cols:
            fields.append("name=?")
            values.append((name or "").strip())
        if description is not None and "description" in cols:
            fields.append("description=?")
            values.append((description or "").strip())
        if assignment_mode is not None and "assignment_mode" in cols:
            fields.append("assignment_mode=?")
            values.append((assignment_mode or "OPTIONAL").strip().upper())
        if status is not None:
            if "status" in cols:
                fields.append("status=?")
                values.append((status or "DRAFT").strip().upper())
            elif "is_active" in cols:
                fields.append("is_active=?")
                values.append(1 if (status or "").strip().upper() == "ACTIVE" else 0)
        if is_test_project is not None and "is_test_project" in cols:
            fields.append("is_test_project=?")
            values.append(int(is_test_project))
        if is_live_project is not None and "is_live_project" in cols:
            fields.append("is_live_project=?")
            values.append(int(is_live_project))
        if expected_submissions is not None and "expected_submissions" in cols:
            fields.append("expected_submissions=?")
            values.append(int(expected_submissions))
        if expected_coverage is not None and "expected_coverage" in cols:
            fields.append("expected_coverage=?")
            values.append(int(expected_coverage))
        if coverage_scheme_id is not None and "coverage_scheme_id" in cols:
            fields.append("coverage_scheme_id=?")
            values.append(int(coverage_scheme_id))
        if organization_id is not None and "organization_id" in cols:
            fields.append("organization_id=?")
            values.append(int(organization_id))
        if allow_unlisted_facilities is not None and "allow_unlisted_facilities" in cols:
            fields.append("allow_unlisted_facilities=?")
            values.append(int(allow_unlisted_facilities))
        if not fields:
            return
        values.append(int(project_id))
        conn.execute(f"UPDATE projects SET {', '.join(fields)} WHERE id=?", tuple(values))
        conn.commit()


def create_project(
    name: str,
    description: str = "",
    owner_name: str = "",
    created_by: Optional[str] = None,
    source: str = "manual",
    assignment_mode: str = "OPTIONAL",
    is_test_project: int = 0,
    is_live_project: int = 0,
    status: str = "DRAFT",
    expected_submissions: Optional[int] = None,
    expected_coverage: Optional[int] = None,
    organization_id: Optional[int] = None,
    allow_unlisted_facilities: int = 0,
    template_id: Optional[int] = None,
) -> int:
    n = (name or "").strip()
    if not n:
        raise ValueError("Project name is required.")
    tag = generate_project_tag(n, None)
    with get_conn() as conn:
        if not _table_exists(conn, "projects"):
            return 0
        cols = _columns(conn, "projects")
        if organization_id is None and "organization_id" in cols:
            try:
                organization_id = get_default_organization_id()
            except Exception:
                organization_id = None
        fields = []
        values = []
        def _add(col, val):
            if col in cols:
                fields.append(col)
                values.append(val)
        _add("name", n)
        _add("description", (description or "").strip())
        _add("owner_name", (owner_name or "").strip())
        _add("created_by", (created_by or owner_name or "System").strip())
        _add("source", (source or "manual").strip().lower())
        _add("assignment_mode", (assignment_mode or "OPTIONAL").strip().upper())
        _add("is_test_project", int(is_test_project or 0))
        _add("is_live_project", int(is_live_project or 0))
        _add("status", (status or "DRAFT").strip().upper())
        _add("expected_submissions", int(expected_submissions) if expected_submissions is not None else None)
        _add("expected_coverage", int(expected_coverage) if expected_coverage is not None else None)
        _add("organization_id", int(organization_id) if organization_id is not None else None)
        _add("allow_unlisted_facilities", int(allow_unlisted_facilities or 0))
        _add("template_id", int(template_id) if template_id is not None else None)
        _add("project_tag", tag)
        _add("is_active", 1 if (status or "").strip().upper() == "ACTIVE" else 0)
        _add("created_at", now_iso())
        placeholders = ",".join(["?"] * len(fields))
        conn.execute(f"INSERT INTO projects ({', '.join(fields)}) VALUES ({placeholders})", tuple(values))
        conn.commit()
        cur = conn.cursor()
        cur.execute("SELECT last_insert_rowid() AS id")
        return int(cur.fetchone()["id"])


def soft_delete_project(project_id: int, deleted_by: Optional[str] = None) -> None:
    with get_conn() as conn:
        if not _table_exists(conn, "projects"):
            return
        cols = _columns(conn, "projects")
        if "deleted_at" in cols:
            conn.execute("UPDATE projects SET deleted_at=? WHERE id=?", (now_iso(), int(project_id)))
        if "status" in cols:
            conn.execute("UPDATE projects SET status='ARCHIVED' WHERE id=?", (int(project_id),))
        elif "is_active" in cols:
            conn.execute("UPDATE projects SET is_active=0 WHERE id=?", (int(project_id),))
        conn.commit()


def restore_project(project_id: int) -> None:
    with get_conn() as conn:
        if not _table_exists(conn, "projects"):
            return
        cols = _columns(conn, "projects")
        if "deleted_at" in cols:
            conn.execute("UPDATE projects SET deleted_at=NULL WHERE id=?", (int(project_id),))
        if "status" in cols:
            conn.execute("UPDATE projects SET status='ACTIVE' WHERE id=?", (int(project_id),))
        if "is_active" in cols:
            conn.execute("UPDATE projects SET is_active=1 WHERE id=?", (int(project_id),))
        conn.commit()


def hard_delete_project(project_id: int) -> None:
    pid = int(project_id)
    with get_conn() as conn:
        if not _table_exists(conn, "projects"):
            return
        cur = conn.cursor()

        # survey answers -> surveys
        if _table_exists(conn, "surveys"):
            cur.execute("SELECT id FROM surveys WHERE project_id=?", (pid,))
            survey_ids = [int(r["id"]) for r in cur.fetchall()]
            if survey_ids and _table_exists(conn, "survey_answers"):
                placeholders = ",".join(["?"] * len(survey_ids))
                cur.execute(f"DELETE FROM survey_answers WHERE survey_id IN ({placeholders})", tuple(survey_ids))
            cur.execute("DELETE FROM surveys WHERE project_id=?", (pid,))

        # templates -> questions -> choices
        if _table_exists(conn, "survey_templates"):
            cur.execute("SELECT id FROM survey_templates WHERE project_id=?", (pid,))
            tpl_ids = [int(r["id"]) for r in cur.fetchall()]
            if tpl_ids and _table_exists(conn, "template_questions"):
                placeholders = ",".join(["?"] * len(tpl_ids))
                cur.execute(f"SELECT id FROM template_questions WHERE template_id IN ({placeholders})", tuple(tpl_ids))
                qids = [int(r["id"]) for r in cur.fetchall()]
                if qids and _table_exists(conn, "template_question_choices"):
                    ph = ",".join(["?"] * len(qids))
                    cur.execute(f"DELETE FROM template_question_choices WHERE template_question_id IN ({ph})", tuple(qids))
                cur.execute(f"DELETE FROM template_questions WHERE template_id IN ({placeholders})", tuple(tpl_ids))
            cur.execute("DELETE FROM survey_templates WHERE project_id=?", (pid,))

        # assignments -> assignment facilities
        if _table_exists(conn, "enumerator_assignments"):
            cur.execute("SELECT id FROM enumerator_assignments WHERE project_id=?", (pid,))
            aids = [int(r["id"]) for r in cur.fetchall()]
            if aids and _table_exists(conn, "assignment_facilities"):
                ph = ",".join(["?"] * len(aids))
                cur.execute(f"DELETE FROM assignment_facilities WHERE assignment_id IN ({ph})", tuple(aids))
            cur.execute("DELETE FROM enumerator_assignments WHERE project_id=?", (pid,))

        # enumerators (project-scoped)
        if _table_exists(conn, "enumerators"):
            cols = _columns(conn, "enumerators")
            if "project_id" in cols:
                cur.execute("DELETE FROM enumerators WHERE project_id=?", (pid,))

        # finally delete project
        cur.execute("DELETE FROM projects WHERE id=?", (pid,))
        conn.commit()


def list_project_templates(project_id: int):
    with get_conn() as conn:
        if not _table_exists(conn, "survey_templates"):
            return []
        cols = _columns(conn, "survey_templates")
        if "project_id" not in cols:
            return []
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name FROM survey_templates WHERE project_id=? ORDER BY id DESC",
            (int(project_id),),
        )
        return [dict(r) for r in cur.fetchall()]


def project_metrics(project_id: int):
    pid = int(project_id)
    out = {"qa_alerts_count": 0}
    with get_conn() as conn:
        if not _table_exists(conn, "surveys"):
            return out
        cols = _columns(conn, "surveys")
        if "qa_flags" in cols:
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) AS c FROM surveys WHERE project_id=? AND qa_flags IS NOT NULL",
                (pid,),
            )
            out["qa_alerts_count"] = int(cur.fetchone()["c"] or 0)
    return out


def project_overview(project_id: int):
    pid = int(project_id)
    with get_conn() as conn:
        if not _table_exists(conn, "surveys"):
            return {
                "total_submissions": 0,
                "completed_submissions": 0,
                "draft_submissions": 0,
                "active_enumerators": 0,
                "expected_submissions": None,
                "project_created_at": None,
                "last_activity": None,
                "avg_completion_minutes": None,
                "median_completion_minutes": None,
                "outlier_count": 0,
            }
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM surveys WHERE project_id=?", (pid,))
        total = int(cur.fetchone()["c"] or 0)
        cur.execute("SELECT COUNT(*) AS c FROM surveys WHERE project_id=? AND status='COMPLETED'", (pid,))
        completed = int(cur.fetchone()["c"] or 0)
        cur.execute("SELECT COUNT(*) AS c FROM surveys WHERE project_id=? AND status!='COMPLETED'", (pid,))
        drafts = int(cur.fetchone()["c"] or 0)
        cur.execute(
            "SELECT COUNT(DISTINCT enumerator_name) AS c FROM surveys WHERE project_id=? AND enumerator_name IS NOT NULL",
            (pid,),
        )
        active_enum = int(cur.fetchone()["c"] or 0)
    project = get_project(pid) or {}
    return {
        "total_submissions": total,
        "completed_submissions": completed,
        "draft_submissions": drafts,
        "active_enumerators": active_enum,
        "expected_submissions": project.get("expected_submissions"),
        "project_created_at": project.get("created_at"),
        "last_activity": None,
        "avg_completion_minutes": None,
        "median_completion_minutes": None,
        "outlier_count": 0,
    }


def enumerator_performance(project_id: int, days: int = 7):
    pid = int(project_id)
    with get_conn() as conn:
        if not _table_exists(conn, "surveys"):
            return []
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              enumerator_name,
              COUNT(*) AS total_submissions,
              SUM(CASE WHEN status='COMPLETED' THEN 1 ELSE 0 END) AS completed_total,
              SUM(CASE WHEN status!='COMPLETED' THEN 1 ELSE 0 END) AS drafts_total,
              SUM(CASE WHEN date(created_at)=date('now','localtime') AND status='COMPLETED' THEN 1 ELSE 0 END) AS completed_today,
              SUM(CASE WHEN date(created_at)>=date('now','localtime','-{d} day') AND status='COMPLETED' THEN 1 ELSE 0 END) AS completed_recent,
              0 AS qa_flags,
              NULL AS avg_completion_minutes,
              0 AS gps_captured,
              0 AS completed_for_gps
            FROM surveys
            WHERE project_id=? AND enumerator_name IS NOT NULL AND enumerator_name<>'' 
            GROUP BY enumerator_name
            ORDER BY completed_total DESC, total_submissions DESC
            """.format(d=int(days)),
            (pid,),
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def submissions_timeline(project_id: int, days: int = 14):
    pid = int(project_id)
    with get_conn() as conn:
        if not _table_exists(conn, "surveys"):
            return []
        cur = conn.cursor()
        cur.execute(
            """
            SELECT date(created_at) AS day, COUNT(*) AS total,
                   SUM(CASE WHEN status='COMPLETED' THEN 1 ELSE 0 END) AS completed
            FROM surveys
            WHERE project_id=? AND date(created_at)>=date('now','localtime', ?)
            GROUP BY date(created_at)
            ORDER BY date(created_at) DESC
            """,
            (pid, f"-{int(days)} day"),
        )
        return [dict(r) for r in cur.fetchall()]


def researcher_profiles(project_id: int):
    pid = int(project_id)
    with get_conn() as conn:
        if not _table_exists(conn, "surveys"):
            return []
        s_cols = _table_columns(conn, "surveys")
        if "project_id" not in s_cols:
            return []

        qa_expr = "SUM(CASE WHEN COALESCE(qa_flags,'')<>'' THEN 1 ELSE 0 END) AS qa_flags" if "qa_flags" in s_cols else "0 AS qa_flags"
        avg_expr = (
            "AVG(CASE WHEN completed_at IS NOT NULL THEN (julianday(completed_at)-julianday(created_at))*1440 END) AS avg_completion_minutes"
            if "completed_at" in s_cols
            else "NULL AS avg_completion_minutes"
        )

        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
              enumerator_name AS display_name,
              COUNT(*) AS total_submissions,
              SUM(CASE WHEN status='COMPLETED' THEN 1 ELSE 0 END) AS completed_submissions,
              SUM(CASE WHEN status<>'COMPLETED' THEN 1 ELSE 0 END) AS drafts,
              {qa_expr},
              MIN(created_at) AS first_activity_at,
              MAX(created_at) AS last_activity_at,
              {avg_expr}
            FROM surveys
            WHERE project_id=? AND enumerator_name IS NOT NULL AND enumerator_name<>''
            GROUP BY enumerator_name
            ORDER BY completed_submissions DESC, total_submissions DESC
            """,
            (pid,),
        )
        rows = cur.fetchall()

        # Map enumerator name -> id if enumerators table exists
        enum_id_map = {}
        if _table_exists(conn, "enumerators"):
            e_cols = _table_columns(conn, "enumerators")
            name_col = "name" if "name" in e_cols else ("full_name" if "full_name" in e_cols else None)
            if name_col:
                cur.execute(
                    f"""
                    SELECT id, {name_col} AS name
                    FROM enumerators
                    WHERE project_id=? OR project_id IS NULL
                    """,
                    (pid,),
                )
                for r in cur.fetchall():
                    if r["name"]:
                        enum_id_map[str(r["name"]).strip().lower()] = int(r["id"])

        # Domains (templates used)
        domain_map = {}
        if "template_id" in s_cols and _table_exists(conn, "survey_templates"):
            cur.execute(
                """
                SELECT s.enumerator_name AS name, GROUP_CONCAT(DISTINCT t.name) AS templates
                FROM surveys s
                LEFT JOIN survey_templates t ON t.id = s.template_id
                WHERE s.project_id=? AND s.enumerator_name IS NOT NULL AND s.enumerator_name<>''
                GROUP BY s.enumerator_name
                """,
                (pid,),
            )
            for r in cur.fetchall():
                domain_map[str(r["name"]).strip().lower()] = [t.strip() for t in (r["templates"] or "").split(",") if t.strip()]

        # Regions (coverage nodes)
        region_map = {}
        if "coverage_node_id" in s_cols and _table_exists(conn, "coverage_nodes"):
            cur.execute(
                """
                SELECT s.enumerator_name AS name, GROUP_CONCAT(DISTINCT cn.name) AS regions
                FROM surveys s
                LEFT JOIN coverage_nodes cn ON cn.id = s.coverage_node_id
                WHERE s.project_id=? AND s.enumerator_name IS NOT NULL AND s.enumerator_name<>''
                GROUP BY s.enumerator_name
                """,
                (pid,),
            )
            for r in cur.fetchall():
                region_map[str(r["name"]).strip().lower()] = [t.strip() for t in (r["regions"] or "").split(",") if t.strip()]

    out = []
    for r in rows:
        name = (r["display_name"] or "").strip()
        total = int(r["total_submissions"] or 0)
        completed = int(r["completed_submissions"] or 0)
        drafts = int(r["drafts"] or 0)
        qa_flags = int(r["qa_flags"] or 0)
        avg_minutes = r["avg_completion_minutes"]

        completion_score = round((completed / max(1, total)) * 100.0, 1)
        quality_score = round(max(0.0, 100.0 - (qa_flags / max(1, completed)) * 100.0), 1)
        consistency_score = round(max(0.0, 100.0 - (drafts / max(1, total)) * 100.0), 1)
        experience_score = round(min(100.0, total * 5.0), 1)
        reliability_score = round(
            (quality_score * 0.35) + (consistency_score * 0.20) + (completion_score * 0.25) + (experience_score * 0.20),
            1,
        )

        key = name.lower()
        out.append(
            {
                "researcher_id": enum_id_map.get(key) or name,
                "display_name": name,
                "roles": ["Enumerator"],
                "completed_submissions": completed,
                "drafts": drafts,
                "qa_flags": qa_flags,
                "quality_score": quality_score,
                "consistency_score": consistency_score,
                "completion_score": completion_score,
                "experience_score": experience_score,
                "reliability_score": reliability_score,
                "avg_completion_minutes": (round(float(avg_minutes), 2) if avg_minutes is not None else None),
                "domains": domain_map.get(key, []),
                "regions": region_map.get(key, []),
                "first_activity_at": r["first_activity_at"],
                "last_activity_at": r["last_activity_at"],
            }
        )

    return out


def enumerator_activity_series(project_id: int, enumerator_name: str, days: int = 7):
    pid = int(project_id)
    name = (enumerator_name or "").strip()
    if not name:
        return []
    with get_conn() as conn:
        if not _table_exists(conn, "surveys"):
            return []
        s_cols = _table_columns(conn, "surveys")
        if "project_id" not in s_cols:
            return []
        qa_expr = "SUM(CASE WHEN COALESCE(qa_flags,'')<>'' THEN 1 ELSE 0 END) AS qa_flags" if "qa_flags" in s_cols else "0 AS qa_flags"
        avg_expr = (
            "AVG(CASE WHEN completed_at IS NOT NULL THEN (julianday(completed_at)-julianday(created_at))*1440 END) AS avg_minutes"
            if "completed_at" in s_cols
            else "NULL AS avg_minutes"
        )
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT date(created_at) AS day,
                   SUM(CASE WHEN status='COMPLETED' THEN 1 ELSE 0 END) AS completed,
                   SUM(CASE WHEN status<>'COMPLETED' THEN 1 ELSE 0 END) AS drafts,
                   {qa_expr},
                   {avg_expr}
            FROM surveys
            WHERE project_id=? AND LOWER(enumerator_name)=LOWER(?) AND date(created_at)>=date('now','localtime', ?)
            GROUP BY date(created_at)
            ORDER BY date(created_at) DESC
            """,
            (pid, name, f"-{int(days)} day"),
        )
        return [dict(r) for r in cur.fetchall()]


def enumerator_activity_series_range(project_id: int, enumerator_name: str, start_date: str, end_date: str):
    pid = int(project_id)
    name = (enumerator_name or "").strip()
    if not name:
        return []
    if not start_date or not end_date:
        return []
    with get_conn() as conn:
        if not _table_exists(conn, "surveys"):
            return []
        s_cols = _table_columns(conn, "surveys")
        if "project_id" not in s_cols:
            return []
        qa_expr = "SUM(CASE WHEN COALESCE(qa_flags,'')<>'' THEN 1 ELSE 0 END) AS qa_flags" if "qa_flags" in s_cols else "0 AS qa_flags"
        avg_expr = (
            "AVG(CASE WHEN completed_at IS NOT NULL THEN (julianday(completed_at)-julianday(created_at))*1440 END) AS avg_minutes"
            if "completed_at" in s_cols
            else "NULL AS avg_minutes"
        )
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT date(created_at) AS day,
                   SUM(CASE WHEN status='COMPLETED' THEN 1 ELSE 0 END) AS completed,
                   SUM(CASE WHEN status<>'COMPLETED' THEN 1 ELSE 0 END) AS drafts,
                   {qa_expr},
                   {avg_expr}
            FROM surveys
            WHERE project_id=? AND LOWER(enumerator_name)=LOWER(?) AND date(created_at) BETWEEN date(?) AND date(?)
            GROUP BY date(created_at)
            ORDER BY date(created_at) DESC
            """,
            (pid, name, start_date, end_date),
        )
        return [dict(r) for r in cur.fetchall()]


def _ensure_org_table(conn) -> None:
    if _table_exists(conn, "organizations"):
        return
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS organizations (
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


def list_organizations(limit: int = 200):
    with get_conn() as conn:
        _ensure_org_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT * FROM organizations ORDER BY id DESC LIMIT ?", (int(limit),))
        return [dict(r) for r in cur.fetchall()]


def create_organization(
    name: str,
    sector: str = "",
    org_type: str = "",
    country: str = "",
    region: str = "",
    size: str = "",
    website: str = "",
    domain: str = "",
    address: str = "",
) -> int:
    n = (name or "").strip()
    if not n:
        raise ValueError("Organization name is required.")
    with get_conn() as conn:
        _ensure_org_table(conn)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO organizations
              (name, org_type, country, region, sector, size, website, domain, address, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                n,
                (org_type or "").strip(),
                (country or "").strip(),
                (region or "").strip(),
                (sector or "").strip(),
                (size or "").strip(),
                (website or "").strip(),
                (domain or "").strip(),
                (address or "").strip(),
                now_iso(),
                now_iso(),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def get_organization(org_id: int):
    with get_conn() as conn:
        if not _table_exists(conn, "organizations"):
            return None
        cur = conn.cursor()
        cur.execute("SELECT * FROM organizations WHERE id=? LIMIT 1", (int(org_id),))
        row = cur.fetchone()
    return dict(row) if row else None


def get_default_organization_id() -> int:
    with get_conn() as conn:
        _ensure_org_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT id FROM organizations ORDER BY id ASC LIMIT 1")
        row = cur.fetchone()
        if row:
            return int(row["id"])
        cur.execute(
            "INSERT INTO organizations (name, sector, created_at) VALUES (?, ?, ?)",
            ("Default Organization", "General", now_iso()),
        )
        conn.commit()
        return int(cur.lastrowid)


def _ensure_supervisors_table(conn) -> None:
    if _table_exists(conn, "supervisors"):
        return
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS supervisors (
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


def list_supervisors(organization_id: Optional[int] = None, limit: int = 200):
    with get_conn() as conn:
        _ensure_supervisors_table(conn)
        cur = conn.cursor()
        if organization_id is not None:
            cur.execute(
                """
                SELECT id, organization_id, full_name, email, phone, access_key, status, created_at
                FROM supervisors
                WHERE organization_id=?
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(organization_id), int(limit)),
            )
        else:
            cur.execute(
                """
                SELECT id, organization_id, full_name, email, phone, access_key, status, created_at
                FROM supervisors
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(limit),),
            )
        return [dict(r) for r in cur.fetchall()]


def create_supervisor(
    full_name: str,
    organization_id: Optional[int] = None,
    email: str = "",
    phone: str = "",
    access_key: str = "",
    status: str = "ACTIVE",
) -> int:
    name = (full_name or "").strip()
    if not name:
        raise ValueError("Supervisor name is required.")
    key = (access_key or "").strip()
    if not key:
        raise ValueError("Supervisor access key is required.")
    if organization_id is None:
        try:
            organization_id = get_default_organization_id()
        except Exception:
            organization_id = None
    with get_conn() as conn:
        _ensure_supervisors_table(conn)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO supervisors (organization_id, full_name, email, phone, access_key, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(organization_id) if organization_id is not None else None,
                name,
                (email or "").strip() or None,
                (phone or "").strip() or None,
                key,
                (status or "ACTIVE").strip().upper(),
                now_iso(),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def update_supervisor(supervisor_id: int, **kwargs) -> None:
    with get_conn() as conn:
        _ensure_supervisors_table(conn)
        cols = _columns(conn, "supervisors")
        fields = []
        values = []
        for k, v in kwargs.items():
            if k in cols:
                fields.append(f"{k}=?")
                values.append(v)
        if not fields:
            return
        values.append(int(supervisor_id))
        conn.execute(f"UPDATE supervisors SET {', '.join(fields)} WHERE id=?", tuple(values))
        conn.commit()


def get_supervisor(supervisor_id: int):
    with get_conn() as conn:
        if not _table_exists(conn, "supervisors"):
            return None
        cur = conn.cursor()
        cur.execute("SELECT * FROM supervisors WHERE id=? LIMIT 1", (int(supervisor_id),))
        row = cur.fetchone()
    return dict(row) if row else None


def get_supervisor_by_key(access_key: str):
    key = (access_key or "").strip()
    if not key:
        return None
    with get_conn() as conn:
        if not _table_exists(conn, "supervisors"):
            return None
        cur = conn.cursor()
        cur.execute("SELECT * FROM supervisors WHERE access_key=? LIMIT 1", (key,))
        row = cur.fetchone()
    return dict(row) if row else None


def get_default_project_id(organization_id: Optional[int] = None) -> int:
    with get_conn() as conn:
        if not _table_exists(conn, "projects"):
            return 0
        cols = _columns(conn, "projects")
        cur = conn.cursor()
        where = []
        params: List[Any] = []
        if organization_id is not None and "organization_id" in cols:
            where.append("organization_id=?")
            params.append(int(organization_id))
        if "source" in cols:
            where.append("(source IS NULL OR source!='system_template_only')")
        if "deleted_at" in cols:
            where.append("deleted_at IS NULL")
        if "status" in cols:
            where.append("(status IS NULL OR status!='ARCHIVED')")
        elif "is_active" in cols:
            where.append("COALESCE(is_active,1)=1")
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        cur.execute(
            f"SELECT id FROM projects {where_sql} ORDER BY id ASC LIMIT 1",
            tuple(params),
        )
        row = cur.fetchone()
        if row:
            return int(row["id"])
    return create_project(
        "Default Project",
        "Auto-created starter project for this workspace.",
        source="system_default",
        assignment_mode="OPTIONAL",
        status="ACTIVE",
        template_id=None,
        organization_id=organization_id,
    )
