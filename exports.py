# exports.py — OpenField Collect
# CSV + JSON exports (facilities, surveys, answers)
# Safe across schema variants (template_id, project_id, QA columns, etc.)

from __future__ import annotations

import csv
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

from db import get_conn


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _table_columns(table: str) -> List[str]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        return [r["name"] for r in cur.fetchall()]


def _slug(text: str) -> str:
    t = "".join([c if c.isalnum() else "_" for c in (text or "").lower()])
    t = "_".join([p for p in t.split("_") if p])
    return t or "question"


def _unique(headers: List[str], name: str) -> str:
    if name not in headers:
        return name
    i = 2
    while f"{name}_{i}" in headers:
        i += 1
    return f"{name}_{i}"


def _parse_redacted_fields(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [v.strip().lower() for v in value.split(",") if v.strip()]


def _should_redact(question_text: str, template_question_id: Optional[int], tokens: List[str]) -> bool:
    if not tokens:
        return False
    qtext = (question_text or "").lower()
    qid = int(template_question_id) if template_question_id is not None else None
    for token in tokens:
        if token.startswith("q_") and qid is not None:
            try:
                if int(token[2:]) == qid:
                    return True
            except Exception:
                pass
        if token.isdigit() and qid is not None:
            if int(token) == qid:
                return True
        if token and token in qtext:
            return True
    return False


# -------------------------------------------------
# Facilities
# -------------------------------------------------

def export_facilities_csv(path: str, limit: int = 5000) -> Dict:
    """
    Exports facilities to CSV at `path`.
    Returns {path, rows}.
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, created_at
            FROM facilities
            ORDER BY id ASC
            LIMIT ?
            """,
            (int(limit),),
        )
        rows = cur.fetchall()

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "created_at"])
        for r in rows:
            w.writerow([r["id"], r["name"], r["created_at"]])

    return {"path": path, "rows": len(rows)}


def export_metadata_csv(path: str, project_id: Optional[int] = None) -> Dict:
    """
    Exports a metadata sheet for projects, templates, enumerators, and coverage schemes.
    """
    with get_conn() as conn:
        cur = conn.cursor()
        proj_where = ""
        params: List = []
        if project_id is not None:
            proj_where = "WHERE id=?"
            params.append(int(project_id))

        cur.execute(
            f"""
            SELECT id, name, description, status, owner_name, created_at, assignment_mode,
                   is_test_project, is_live_project
            FROM projects
            {proj_where}
            ORDER BY id ASC
            """,
            tuple(params),
        )
        projects = cur.fetchall()

        cur.execute(
            f"""
            SELECT id, project_id, name, template_version, updated_at, is_sensitive, restricted_exports, redacted_fields
            FROM survey_templates
            {"WHERE project_id=?" if project_id is not None else ""}
            ORDER BY id ASC
            """,
            tuple(params) if project_id is not None else (),
        )
        templates = cur.fetchall()

        cur.execute(
            f"""
            SELECT id, project_id, name, code, status, created_at
            FROM enumerators
            {"WHERE project_id=?" if project_id is not None else ""}
            ORDER BY id ASC
            """,
            tuple(params) if project_id is not None else (),
        )
        enumerators = cur.fetchall()

        cur.execute(
            """
            SELECT id, name, created_at
            FROM coverage_schemes
            ORDER BY id ASC
            """
        )
        schemes = cur.fetchall()

        cur.execute(
            """
            SELECT id, scheme_id, name, parent_id, created_at
            FROM coverage_nodes
            ORDER BY scheme_id ASC, id ASC
            """
        )
        nodes = cur.fetchall()

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["section", "key", "value", "extra1", "extra2", "extra3"])

        for p in projects:
            pid = p["id"]
            w.writerow(["project", f"{pid}:name", p["name"], "", "", ""])
            w.writerow(["project", f"{pid}:description", p["description"], "", "", ""])
            w.writerow(["project", f"{pid}:status", p["status"], "", "", ""])
            w.writerow(["project", f"{pid}:owner", p["owner_name"], "", "", ""])
            w.writerow(["project", f"{pid}:created_at", p["created_at"], "", "", ""])
            w.writerow(["project", f"{pid}:assignment_mode", p["assignment_mode"], "", "", ""])
            w.writerow(["project", f"{pid}:is_test_project", p["is_test_project"], "", "", ""])
            w.writerow(["project", f"{pid}:is_live_project", p["is_live_project"], "", "", ""])

        for t in templates:
            w.writerow(
                [
                    "template",
                    t["id"],
                    t["name"],
                    t["template_version"],
                    "sensitive" if int(t["is_sensitive"] or 0) == 1 else "normal",
                    "restricted" if int(t["restricted_exports"] or 0) == 1 else "open",
                ]
            )
            if t["redacted_fields"]:
                w.writerow(
                    ["template_redactions", t["id"], t["redacted_fields"], "", "", ""]
                )

        for e in enumerators:
            w.writerow(
                [
                    "enumerator",
                    e["id"],
                    e["name"],
                    e["code"],
                    e["status"],
                    e["created_at"],
                ]
            )

        for s in schemes:
            w.writerow(["coverage_scheme", s["id"], s["name"], s["created_at"], "", ""])

        for n in nodes:
            w.writerow(
                [
                    "coverage_node",
                    n["id"],
                    n["name"],
                    n["scheme_id"],
                    n["parent_id"],
                    n["created_at"],
                ]
            )

    return {"path": path, "rows": len(projects) + len(templates) + len(enumerators)}


# -------------------------------------------------
# Surveys + Answers (CSV)
# -------------------------------------------------

def export_surveys_answers_csv(
    path: str,
    limit_surveys: int = 2000,
    project_id: Optional[int] = None,
    allow_restricted: bool = False,
) -> Dict:
    """
    Flat CSV: one row per answer, with survey header fields repeated.
    Optional: filter by project_id if column exists.
    """
    surveys_cols = _table_columns("surveys")
    has_client_uuid = "client_uuid" in surveys_cols
    has_client_created_at = "client_created_at" in surveys_cols
    has_sync_source = "sync_source" in surveys_cols
    has_synced_at = "synced_at" in surveys_cols
    answers_cols = _table_columns("survey_answers")

    has_project = "project_id" in surveys_cols
    has_template_id = "template_id" in surveys_cols
    has_enum_code = "enumerator_code" in surveys_cols
    has_client_uuid = "client_uuid" in surveys_cols
    has_client_created_at = "client_created_at" in surveys_cols
    has_sync_source = "sync_source" in surveys_cols
    has_synced_at = "synced_at" in surveys_cols
    has_client_uuid = "client_uuid" in surveys_cols
    has_client_created_at = "client_created_at" in surveys_cols
    has_sync_source = "sync_source" in surveys_cols
    has_synced_at = "synced_at" in surveys_cols

    # Optional QA fields
    has_answer_source = "answer_source" in answers_cols
    has_confidence = "confidence_level" in answers_cols
    has_is_missing = "is_missing" in answers_cols
    has_missing_reason = "missing_reason" in answers_cols
    has_template_question_id = "template_question_id" in answers_cols

    where = []
    params: List = []
    if project_id is not None and has_project:
        where.append("s.project_id=?")
        params.append(int(project_id))
    if "deleted_at" in surveys_cols:
        where.append("s.deleted_at IS NULL")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
              s.id AS survey_id,
              f.name AS facility_name,
              s.facility_id,
              {"s.template_id," if has_template_id else "NULL AS template_id,"}
              t.assignment_mode AS template_assignment_mode,
              p.assignment_mode AS project_assignment_mode,
              s.survey_type,
              s.enumerator_name,
              {"s.enumerator_code," if has_enum_code else "NULL AS enumerator_code,"}
              s.status,
              s.created_at,
              s.completed_at,
              {"s.client_uuid," if has_client_uuid else "NULL AS client_uuid,"}
              {"s.client_created_at," if has_client_created_at else "NULL AS client_created_at,"}
              {"s.sync_source," if has_sync_source else "NULL AS sync_source,"}
              {"s.synced_at," if has_synced_at else "NULL AS synced_at,"}
              s.consent_obtained,
              s.consent_timestamp,
              s.attestation_text,
              s.attestation_timestamp,
              s.gps_lat, s.gps_lng, s.gps_accuracy, s.gps_timestamp,
              a.id AS answer_id,
              {"a.template_question_id," if has_template_question_id else "NULL AS template_question_id,"}
              a.question,
              a.answer,
              a.created_at AS answer_created_at
              {", a.answer_source" if has_answer_source else ", NULL AS answer_source"}
              {", a.confidence_level" if has_confidence else ", NULL AS confidence_level"}
              {", a.is_missing" if has_is_missing else ", 0 AS is_missing"}
              {", a.missing_reason" if has_missing_reason else ", NULL AS missing_reason"}
            FROM surveys s
            JOIN facilities f ON f.id = s.facility_id
            LEFT JOIN survey_answers a ON a.survey_id = s.id
            LEFT JOIN survey_templates t ON t.id = s.template_id
            LEFT JOIN projects p ON p.id = s.project_id
            {where_sql}
            ORDER BY s.id DESC, a.id ASC
            LIMIT ?
            """,
            (*params, int(limit_surveys) * 500),
        )
        rows = cur.fetchall()

    template_settings: Dict[int, Dict[str, Any]] = {}
    template_ids = [int(r["template_id"]) for r in rows if r["template_id"] is not None]
    if template_ids:
        placeholders = ",".join(["?"] * len(template_ids))
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                f"""
                SELECT id, restricted_exports, redacted_fields
                FROM survey_templates
                WHERE id IN ({placeholders})
                """,
                tuple(template_ids),
            )
            for r in cur.fetchall():
                template_settings[int(r["id"])] = {
                    "restricted_exports": int(r["restricted_exports"] or 0),
                    "redacted_fields": _parse_redacted_fields(r["redacted_fields"]),
                }

    header = [
        "survey_id",
        "facility_id",
        "facility_name",
        "template_id",
        "template_assignment_mode",
        "project_assignment_mode",
        "survey_type",
        "enumerator_name",
        "enumerator_code",
        "status",
        "created_at",
        "completed_at",
        "client_uuid",
        "client_created_at",
        "sync_source",
        "synced_at",
        "consent_obtained",
        "consent_timestamp",
        "attestation_text",
        "attestation_timestamp",
        "gps_lat",
        "gps_lng",
        "gps_accuracy",
        "gps_timestamp",
        "answer_id",
        "template_question_id",
        "question",
        "answer",
        "answer_created_at",
        "answer_source",
        "confidence_level",
        "is_missing",
        "missing_reason",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            template_id = int(r["template_id"]) if r["template_id"] is not None else None
            settings = template_settings.get(template_id, {})
            restricted = int(settings.get("restricted_exports") or 0) == 1
            tokens = settings.get("redacted_fields") or []
            answer_value = r["answer"]
            if not allow_restricted and (restricted or _should_redact(r["question"], r["template_question_id"], tokens)):
                answer_value = "REDACTED"

            w.writerow(
                [
                    r["survey_id"],
                    r["facility_id"],
                    r["facility_name"],
                    r["template_id"],
                    r["template_assignment_mode"],
                    r["project_assignment_mode"],
                    r["survey_type"],
                    r["enumerator_name"],
                    r["enumerator_code"],
                    r["status"],
                    r["created_at"],
                    r["completed_at"],
                    r["client_uuid"],
                    r["client_created_at"],
                    r["sync_source"],
                    r["synced_at"],
                    r["consent_obtained"],
                    r["consent_timestamp"],
                    r["attestation_text"],
                    r["attestation_timestamp"],
                    r["gps_lat"],
                    r["gps_lng"],
                    r["gps_accuracy"],
                    r["gps_timestamp"],
                    r["answer_id"],
                    r["template_question_id"],
                    r["question"],
                    answer_value,
                    r["answer_created_at"],
                    r["answer_source"],
                    r["confidence_level"],
                    r["is_missing"],
                    r["missing_reason"],
                ]
            )

    return {"path": path, "rows": len(rows)}


# -------------------------------------------------
# Surveys + Answers (JSON)
# -------------------------------------------------

def export_surveys_answers_json(
    path: str,
    limit_surveys: int = 500,
    project_id: Optional[int] = None,
    allow_restricted: bool = False,
) -> Dict:
    """
    Nested JSON: surveys[] each contains answers[].
    Optional: filter by project_id if column exists.
    """
    surveys_cols = _table_columns("surveys")
    has_project = "project_id" in surveys_cols
    has_template_id = "template_id" in surveys_cols
    has_enum_code = "enumerator_code" in surveys_cols

    where = []
    params: List = []
    if project_id is not None and has_project:
        where.append("s.project_id=?")
        params.append(int(project_id))
    if "deleted_at" in surveys_cols:
        where.append("s.deleted_at IS NULL")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
              s.id AS survey_id,
              s.facility_id,
              f.name AS facility_name,
              {"s.template_id AS template_id," if has_template_id else "NULL AS template_id,"}
              t.assignment_mode AS template_assignment_mode,
              p.assignment_mode AS project_assignment_mode,
              s.survey_type,
              s.enumerator_name,
              {"s.enumerator_code AS enumerator_code," if has_enum_code else "NULL AS enumerator_code,"}
              s.status,
              s.created_at,
              s.completed_at,
              {"s.client_uuid," if has_client_uuid else "NULL AS client_uuid,"}
              {"s.client_created_at," if has_client_created_at else "NULL AS client_created_at,"}
              {"s.sync_source," if has_sync_source else "NULL AS sync_source,"}
              {"s.synced_at," if has_synced_at else "NULL AS synced_at,"}
              s.consent_obtained,
              s.consent_timestamp,
              s.attestation_text,
              s.attestation_timestamp,
              s.gps_lat, s.gps_lng, s.gps_accuracy, s.gps_timestamp
            FROM surveys s
            JOIN facilities f ON f.id = s.facility_id
            LEFT JOIN survey_templates t ON t.id = s.template_id
            LEFT JOIN projects p ON p.id = s.project_id
            {where_sql}
            ORDER BY s.id DESC
            LIMIT ?
            """,
            (*params, int(limit_surveys)),
        )
        surveys = cur.fetchall()

        template_ids = [int(s["template_id"]) for s in surveys if s["template_id"] is not None]
        template_settings: Dict[int, Dict[str, Any]] = {}
        if template_ids:
            placeholders = ",".join(["?"] * len(template_ids))
            cur.execute(
                f"""
                SELECT id, restricted_exports, redacted_fields
                FROM survey_templates
                WHERE id IN ({placeholders})
                """,
                tuple(template_ids),
            )
            for r in cur.fetchall():
                template_settings[int(r["id"])] = {
                    "restricted_exports": int(r["restricted_exports"] or 0),
                    "redacted_fields": _parse_redacted_fields(r["redacted_fields"]),
                }

        out = []
        for s in surveys:
            cur.execute(
                """
                SELECT
                  id,
                  template_question_id,
                  question,
                  answer,
                  created_at,
                  answer_source,
                  confidence_level,
                  is_missing,
                  missing_reason
                FROM survey_answers
                WHERE survey_id=?
                ORDER BY id ASC
                """,
                (int(s["survey_id"]),),
            )
            ans = cur.fetchall()

            template_id = int(s["template_id"]) if s["template_id"] is not None else None
            settings = template_settings.get(template_id, {})
            restricted = int(settings.get("restricted_exports") or 0) == 1
            tokens = settings.get("redacted_fields") or []
            answers_out = []
            for a in ans:
                answer_value = a["answer"]
                if not allow_restricted and (restricted or _should_redact(a["question"], a["template_question_id"], tokens)):
                    answer_value = "REDACTED"
                answers_out.append(
                    {
                        "answer_id": a["id"],
                        "template_question_id": a["template_question_id"],
                        "question": a["question"],
                        "answer": answer_value,
                        "created_at": a["created_at"],
                        "answer_source": a["answer_source"],
                        "confidence_level": a["confidence_level"],
                        "is_missing": a["is_missing"],
                        "missing_reason": a["missing_reason"],
                    }
                )

            out.append(
                {
                    "survey": {
                        "survey_id": s["survey_id"],
                        "facility_id": s["facility_id"],
                        "facility_name": s["facility_name"],
                        "template_id": s["template_id"],
                        "template_assignment_mode": s["template_assignment_mode"],
                        "project_assignment_mode": s["project_assignment_mode"],
                        "survey_type": s["survey_type"],
                        "enumerator_name": s["enumerator_name"],
                        "enumerator_code": s["enumerator_code"],
                        "status": s["status"],
                        "created_at": s["created_at"],
                        "completed_at": s["completed_at"],
                        "client_uuid": s["client_uuid"],
                        "client_created_at": s["client_created_at"],
                        "sync_source": s["sync_source"],
                        "synced_at": s["synced_at"],
                        "consent_obtained": s["consent_obtained"],
                        "consent_timestamp": s["consent_timestamp"],
                        "attestation_text": s["attestation_text"],
                        "attestation_timestamp": s["attestation_timestamp"],
                        "gps": {
                            "lat": s["gps_lat"],
                            "lng": s["gps_lng"],
                            "accuracy": s["gps_accuracy"],
                            "timestamp": s["gps_timestamp"],
                        },
                    },
                    "answers": answers_out,
                }
            )

    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    return {"path": path, "surveys": len(out)}


def export_one_survey_json(path: str, survey_id: int, allow_restricted: bool = False) -> Dict:
    """
    Exports exactly one survey (nested) to JSON.
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              s.*,
              f.name AS facility_name,
              t.assignment_mode AS template_assignment_mode,
              p.assignment_mode AS project_assignment_mode,
              t.restricted_exports AS restricted_exports,
              t.redacted_fields AS redacted_fields
            FROM surveys s
            JOIN facilities f ON f.id = s.facility_id
            LEFT JOIN survey_templates t ON t.id = s.template_id
            LEFT JOIN projects p ON p.id = s.project_id
            WHERE s.id=?
            LIMIT 1
            """,
            (int(survey_id),),
        )
        s = cur.fetchone()
        if not s:
            raise ValueError("Survey not found")

        cur.execute(
            """
            SELECT *
            FROM survey_answers
            WHERE survey_id=?
            ORDER BY id ASC
            """,
            (int(survey_id),),
        )
        ans = cur.fetchall()

    survey_data = dict(s)
    survey_data["template_assignment_mode"] = s["template_assignment_mode"]
    survey_data["project_assignment_mode"] = s["project_assignment_mode"]
    restricted = int(s.get("restricted_exports") or 0) == 1 if "restricted_exports" in s.keys() else False
    tokens = _parse_redacted_fields(s.get("redacted_fields") if "redacted_fields" in s.keys() else None)
    answers_out = []
    for a in ans:
        a_dict = dict(a)
        if not allow_restricted and (restricted or _should_redact(a_dict.get("question", ""), a_dict.get("template_question_id"), tokens)):
            a_dict["answer"] = "REDACTED"
        answers_out.append(a_dict)
    out = {
        "survey": survey_data,
        "answers": answers_out,
        "exported_at": _now(),
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    return {"path": path, "survey_id": int(survey_id), "answers": len(ans)}


def export_surveys_flat_csv(
    path: str,
    project_id: Optional[int] = None,
    template_id: Optional[int] = None,
    enumerator_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    allow_restricted: bool = False,
) -> Dict:
    """
    One row = one survey. Questions are flattened to columns using question text.
    """
    surveys_cols = _table_columns("surveys")
    where = []
    params: List = []
    if project_id is not None and "project_id" in surveys_cols:
        where.append("s.project_id=?")
        params.append(int(project_id))
    if template_id is not None and "template_id" in surveys_cols:
        where.append("s.template_id=?")
        params.append(int(template_id))
    if enumerator_name:
        where.append("LOWER(s.enumerator_name) LIKE LOWER(?)")
        params.append(f"%{enumerator_name}%")
    if start_date:
        where.append("date(s.created_at) >= date(?)")
        params.append(start_date)
    if end_date:
        where.append("date(s.created_at) <= date(?)")
        params.append(end_date)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
              s.id AS survey_id,
              s.project_id,
              s.template_id,
              s.facility_id,
              f.name AS facility_name,
              s.enumerator_name,
              s.enumerator_code,
              s.status,
              s.created_at,
              s.completed_at,
              {"s.client_uuid," if has_client_uuid else "NULL AS client_uuid,"}
              {"s.client_created_at," if has_client_created_at else "NULL AS client_created_at,"}
              {"s.sync_source," if has_sync_source else "NULL AS sync_source,"}
              {"s.synced_at," if has_synced_at else "NULL AS synced_at,"}
              s.consent_obtained,
              s.consent_timestamp,
              s.attestation_text,
              s.attestation_timestamp,
              s.gps_lat, s.gps_lng, s.gps_accuracy, s.gps_timestamp,
              s.coverage_node_name,
              s.qa_flags
            FROM surveys s
            JOIN facilities f ON f.id = s.facility_id
            {where_sql}
            ORDER BY s.id ASC
            """,
            tuple(params),
        )
        surveys = cur.fetchall()

        if not surveys:
            return {"path": path, "rows": 0}

        template_ids = [int(s["template_id"]) for s in surveys if s["template_id"] is not None]
        template_settings: Dict[int, Dict[str, Any]] = {}
        if template_ids:
            placeholders = ",".join(["?"] * len(template_ids))
            cur.execute(
                f"""
                SELECT id, restricted_exports, redacted_fields
                FROM survey_templates
                WHERE id IN ({placeholders})
                """,
                tuple(template_ids),
            )
            for r in cur.fetchall():
                template_settings[int(r["id"])] = {
                    "restricted_exports": int(r["restricted_exports"] or 0),
                    "redacted_fields": _parse_redacted_fields(r["redacted_fields"]),
                }

        survey_ids = [int(s["survey_id"]) for s in surveys]
        placeholders = ",".join(["?"] * len(survey_ids))
        cur.execute(
            f"""
            SELECT
              survey_id,
              template_question_id,
              question,
              answer
            FROM survey_answers
            WHERE survey_id IN ({placeholders})
            ORDER BY survey_id ASC, id ASC
            """,
            tuple(survey_ids),
        )
        answers = cur.fetchall()

    # Build question header map
    question_headers: Dict[int, str] = {}
    headers: List[str] = [
        "survey_id",
        "project_id",
        "template_id",
        "facility_id",
        "facility_name",
        "enumerator_name",
        "enumerator_code",
        "status",
        "created_at",
        "completed_at",
        "client_uuid",
        "client_created_at",
        "sync_source",
        "synced_at",
        "consent_obtained",
        "consent_timestamp",
        "attestation_text",
        "attestation_timestamp",
        "gps_lat",
        "gps_lng",
        "gps_accuracy",
        "gps_timestamp",
        "coverage_node_name",
        "qa_flags",
    ]

    for a in answers:
        qid = a["template_question_id"] if a["template_question_id"] is not None else None
        qtext = a["question"] or ""
        base = _slug(qtext)
        if qid is not None and int(qid) in question_headers:
            continue
        key = _unique(headers, base)
        headers.append(key)
        if qid is not None:
            question_headers[int(qid)] = key

    # Build rows per survey
    by_survey: Dict[int, Dict[str, str]] = {}
    for s in surveys:
        sid = int(s["survey_id"])
        by_survey[sid] = {
            "survey_id": sid,
            "project_id": s["project_id"],
            "template_id": s["template_id"],
            "facility_id": s["facility_id"],
            "facility_name": s["facility_name"],
            "enumerator_name": s["enumerator_name"],
            "enumerator_code": s["enumerator_code"],
            "status": s["status"],
            "created_at": s["created_at"],
            "completed_at": s["completed_at"],
            "client_uuid": s["client_uuid"],
            "client_created_at": s["client_created_at"],
            "sync_source": s["sync_source"],
            "synced_at": s["synced_at"],
            "consent_obtained": s["consent_obtained"],
            "consent_timestamp": s["consent_timestamp"],
            "attestation_text": s["attestation_text"],
            "attestation_timestamp": s["attestation_timestamp"],
            "gps_lat": s["gps_lat"],
            "gps_lng": s["gps_lng"],
            "gps_accuracy": s["gps_accuracy"],
            "gps_timestamp": s["gps_timestamp"],
            "coverage_node_name": s["coverage_node_name"],
            "qa_flags": s["qa_flags"],
        }

    for a in answers:
        sid = int(a["survey_id"])
        qid = a["template_question_id"] if a["template_question_id"] is not None else None
        qtext = a["question"] or ""
        key = question_headers.get(int(qid)) if qid is not None else _unique(headers, _slug(qtext))
        if key not in headers:
            headers.append(key)
        if sid in by_survey:
            template_id = by_survey[sid].get("template_id")
            settings = template_settings.get(int(template_id)) if template_id else {}
            restricted = int(settings.get("restricted_exports") or 0) == 1
            tokens = settings.get("redacted_fields") or []
            answer_value = a["answer"] if a["answer"] is not None else "NA"
            if not allow_restricted and (restricted or _should_redact(qtext, qid, tokens)):
                answer_value = "REDACTED"
            by_survey[sid][key] = answer_value

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for s in surveys:
            row = by_survey.get(int(s["survey_id"]), {})
            w.writerow([row.get(h, "NA") if row.get(h, None) not in (None, "") else "NA" for h in headers])

    return {"path": path, "rows": len(surveys)}


def export_surveys_flat_json(
    path: str,
    project_id: Optional[int] = None,
    template_id: Optional[int] = None,
    enumerator_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    allow_restricted: bool = False,
) -> Dict:
    csv_path = path.replace(".json", ".csv")
    export_surveys_flat_csv(
        csv_path,
        project_id=project_id,
        template_id=template_id,
        enumerator_name=enumerator_name,
        start_date=start_date,
        end_date=end_date,
        allow_restricted=allow_restricted,
    )
    # Re-read CSV to JSON rows for consistency
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [dict(r) for r in reader]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    return {"path": path, "rows": len(rows)}
