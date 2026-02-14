# supervision.py — OpenField Collect
# Full replacement. Provides stable survey listing, survey detail + QA, and QA alerts dashboard.

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

from db import get_conn


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_int(x, default=0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _safe_float(x, default=None):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def _haversine_m(lat1, lng1, lat2, lng2) -> Optional[float]:
    try:
        from math import radians, sin, cos, asin, sqrt
        r = 6371000.0
        dlat = radians(float(lat2) - float(lat1))
        dlng = radians(float(lng2) - float(lng1))
        a = sin(dlat / 2) ** 2 + cos(radians(float(lat1))) * cos(radians(float(lat2))) * sin(dlng / 2) ** 2
        c = 2 * asin(sqrt(a))
        return r * c
    except Exception:
        return None


@dataclass
class QASummary:
    # lightweight QA summary
    total_answers: int = 0
    missing_required_count: int = 0
    missing_required_questions: List[str] = None
    empty_answer_count: int = 0
    low_confidence_count: int = 0
    gps_missing: bool = False
    gps_present: bool = False
    has_suspicious_values: bool = False
    flags: List[str] = None
    severity: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["missing_required_questions"] = self.missing_required_questions or []
        d["flags"] = self.flags or []
        return d


@dataclass
class QAAlert:
    survey_id: int
    facility_name: str
    enumerator_name: str
    flags: List[str]
    severity: float


# -------------------------
# Internal schema helpers
# -------------------------

def _table_columns(table_name: str) -> List[str]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table_name})")
        return [r["name"] for r in cur.fetchall()]


def _table_exists(table_name: str) -> bool:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table_name,),
        )
        return cur.fetchone() is not None


def _surveys_has(col: str) -> bool:
    return col in _table_columns("surveys")


def _answers_has(col: str) -> bool:
    return col in _table_columns("survey_answers")


def _templates_has(col: str) -> bool:
    return col in _table_columns("survey_templates")


def _supervisor_coverage_nodes(supervisor_id: int, project_id: Optional[int]) -> List[int]:
    if not _table_exists("supervisor_coverage_nodes"):
        return []
    with get_conn() as conn:
        cur = conn.cursor()
        if project_id:
            cur.execute(
                """
                SELECT coverage_node_id
                FROM supervisor_coverage_nodes
                WHERE supervisor_id=? AND project_id=?
                """,
                (int(supervisor_id), int(project_id)),
            )
        else:
            cur.execute(
                "SELECT coverage_node_id FROM supervisor_coverage_nodes WHERE supervisor_id=?",
                (int(supervisor_id),),
            )
        return [int(r["coverage_node_id"]) for r in cur.fetchall() if r["coverage_node_id"]]


# -------------------------
# Filters + listing
# -------------------------

def filter_surveys(
    status: str = "",
    enumerator: str = "",
    template_id: str = "",
    facility_id: str = "",
    project_id: str = "",
    supervisor_id: str = "",
    limit: int = 50,
    date_from: str = "",
    date_to: str = "",
) -> List[Tuple[int, str, Optional[int], str, str, str, str]]:
    """
    Returns list rows:
    (survey_id, facility_name, template_id, survey_type, enumerator_name, status, created_at)
    """
    where = []
    params = []

    if status:
        where.append("s.status = ?")
        params.append(status.strip().upper())

    if enumerator:
        where.append("LOWER(s.enumerator_name) LIKE LOWER(?)")
        params.append(f"%{enumerator.strip()}%")

    if template_id:
        where.append("s.template_id = ?")
        params.append(_safe_int(template_id))

    if facility_id:
        where.append("s.facility_id = ?")
        params.append(_safe_int(facility_id))

    if project_id:
        where.append("s.project_id = ?")
        params.append(_safe_int(project_id))

    # Supervisor scope filter (owner scope + optional field area scope)
    if supervisor_id:
        sup_id_int = _safe_int(supervisor_id)
        sup_scope_clauses = []
        if _surveys_has("supervisor_id"):
            sup_scope_clauses.append("s.supervisor_id=?")
            params.append(sup_id_int)
        if _surveys_has("assignment_id") and _table_exists("enumerator_assignments"):
            ea_cols = _table_columns("enumerator_assignments")
            if "supervisor_id" in ea_cols:
                sup_scope_clauses.append(
                    """
                    s.assignment_id IN (
                      SELECT id
                      FROM enumerator_assignments
                      WHERE supervisor_id=?
                    )
                    """
                )
                params.append(sup_id_int)
        if sup_scope_clauses:
            where.append("(" + " OR ".join(sup_scope_clauses) + ")")
        else:
            where.append("1=0")

        cov_nodes = _supervisor_coverage_nodes(sup_id_int, _safe_int(project_id) if project_id else None)
        if cov_nodes:
            placeholders = ",".join(["?"] * len(cov_nodes))
            cov_clauses = []
            if _surveys_has("coverage_node_id"):
                cov_clauses.append(f"s.coverage_node_id IN ({placeholders})")
            if _surveys_has("assignment_id") and _table_exists("assignment_coverage_nodes"):
                cov_clauses.append(
                    f"""s.assignment_id IN (
                        SELECT assignment_id
                        FROM assignment_coverage_nodes
                        WHERE coverage_node_id IN ({placeholders})
                    )"""
                )
            if cov_clauses:
                where.append("(" + " OR ".join(cov_clauses) + ")")
                # add params for each clause using placeholders
                params.extend(cov_nodes)
                if len(cov_clauses) > 1:
                    params.extend(cov_nodes)

    if date_from:
        where.append("date(s.created_at) >= date(?)")
        params.append(date_from)
    if date_to:
        where.append("date(s.created_at) <= date(?)")
        params.append(date_to)

    if _surveys_has("deleted_at"):
        where.append("s.deleted_at IS NULL")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # surveys.template_id might not exist in very old DBs.
    tpl_expr = "s.template_id" if _surveys_has("template_id") else "NULL as template_id"

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
              s.id AS survey_id,
              COALESCE(f.name, '') AS facility_name,
              {tpl_expr} AS template_id,
              COALESCE(s.survey_type, '') AS survey_type,
              COALESCE(s.enumerator_name, '') AS enumerator_name,
              COALESCE(s.status, '') AS status,
              COALESCE(s.created_at, '') AS created_at
            FROM surveys s
            LEFT JOIN facilities f ON f.id = s.facility_id
            {where_sql}
            ORDER BY s.id DESC
            LIMIT ?
            """,
            tuple(params + [int(limit)]),
        )
        rows = cur.fetchall()

    out = []
    for r in rows:
        out.append(
            (
                int(r["survey_id"]),
                r["facility_name"],
                (int(r["template_id"]) if r["template_id"] is not None else None),
                r["survey_type"],
                r["enumerator_name"],
                r["status"],
                r["created_at"],
            )
        )
    return out


# -------------------------
# Survey detail
# -------------------------

def get_survey_details(
    survey_id: int,
) -> Tuple[
    Optional[Tuple[Any, ...]],
    List[Tuple[Any, ...]],
    QASummary,
]:
    """
    Header tuple format expected by app.py:
    (survey_id, facility_id, facility_name, template_id, survey_type, enumerator_name,
     status, created_at, gps_lat, gps_lng, gps_accuracy, gps_timestamp, coverage_node_id, coverage_node_name)

    Answers tuple format expected by app.py:
    (answer_id, template_question_id, question, answer, answer_source, confidence_level, is_missing, missing_reason)
    """

    sid = int(survey_id)

    # Determine columns availability
    has_template_id = _surveys_has("template_id")
    has_gps_lat = _surveys_has("gps_lat")
    has_gps_lng = _surveys_has("gps_lng")
    has_gps_accuracy = _surveys_has("gps_accuracy")
    has_gps_timestamp = _surveys_has("gps_timestamp")
    has_coverage_node_id = _surveys_has("coverage_node_id")
    has_qa_flags = _surveys_has("qa_flags")
    has_gps_missing_flag = _surveys_has("gps_missing_flag")
    has_duplicate_flag = _surveys_has("duplicate_flag")

    has_answer_source = _answers_has("answer_source")
    has_confidence = _answers_has("confidence_level")
    has_is_missing = _answers_has("is_missing")
    has_missing_reason = _answers_has("missing_reason")
    has_tqid = _answers_has("template_question_id")

    tpl_expr = "s.template_id" if has_template_id else "NULL"
    gps_lat_expr = "s.gps_lat" if has_gps_lat else "NULL"
    gps_lng_expr = "s.gps_lng" if has_gps_lng else "NULL"
    gps_acc_expr = "s.gps_accuracy" if has_gps_accuracy else "NULL"
    gps_ts_expr = "s.gps_timestamp" if has_gps_timestamp else "NULL"
    cov_expr = "s.coverage_node_id" if has_coverage_node_id else "NULL"
    qa_flags_expr = "s.qa_flags" if has_qa_flags else "NULL"
    gps_missing_expr = "s.gps_missing_flag" if has_gps_missing_flag else "0"
    dup_expr = "s.duplicate_flag" if has_duplicate_flag else "0"

    where = ["s.id=?"]
    params = [sid]
    if _surveys_has("deleted_at"):
        where.append("s.deleted_at IS NULL")
    where_sql = " AND ".join(where)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
              s.id AS survey_id,
              s.facility_id AS facility_id,
              COALESCE(f.name, '') AS facility_name,
              {tpl_expr} AS template_id,
              COALESCE(s.survey_type, '') AS survey_type,
              COALESCE(s.enumerator_name, '') AS enumerator_name,
              COALESCE(s.status, '') AS status,
              COALESCE(s.created_at, '') AS created_at,
              {gps_lat_expr} AS gps_lat,
              {gps_lng_expr} AS gps_lng,
              {gps_acc_expr} AS gps_accuracy,
              {gps_ts_expr} AS gps_timestamp,
              {cov_expr} AS coverage_node_id,
              {qa_flags_expr} AS qa_flags,
              {gps_missing_expr} AS gps_missing_flag,
              {dup_expr} AS duplicate_flag
            FROM surveys s
            LEFT JOIN facilities f ON f.id = s.facility_id
            WHERE {where_sql}
            LIMIT 1
            """,
            tuple(params),
        )
        srow = cur.fetchone()

    if not srow:
        return None, [], QASummary(missing_required_questions=[], flags=[])

    srow_d = dict(srow)

    template_id = srow_d["template_id"]
    coverage_node_id = srow_d["coverage_node_id"]

    # coverage_node_name is optional (we may not have coverage tables yet)
    coverage_node_name = ""
    if coverage_node_id:
        # Safe: only if tables exist
        cols_cov = _table_columns("coverage_nodes") if "coverage_nodes" in _list_tables() else []
        if cols_cov:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT name FROM coverage_nodes WHERE id=? LIMIT 1", (int(coverage_node_id),))
                rr = cur.fetchone()
                if rr:
                    coverage_node_name = rr["name"]

    header = (
        int(srow_d["survey_id"]),
        int(srow_d["facility_id"]),
        srow_d["facility_name"],
        (int(template_id) if template_id is not None else None),
        srow_d["survey_type"],
        srow_d["enumerator_name"],
        srow_d["status"],
        srow_d["created_at"],
        srow_d["gps_lat"],
        srow_d["gps_lng"],
        srow_d["gps_accuracy"],
        srow_d["gps_timestamp"],
        (int(coverage_node_id) if coverage_node_id is not None else None),
        coverage_node_name,
    )

    # Load answers
    tqid_expr = "a.template_question_id" if has_tqid else "NULL"
    asrc_expr = "a.answer_source" if has_answer_source else "NULL"
    conf_expr = "a.confidence_level" if has_confidence else "NULL"
    miss_expr = "a.is_missing" if has_is_missing else "0"
    mrea_expr = "a.missing_reason" if has_missing_reason else "NULL"

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
              a.id AS answer_id,
              {tqid_expr} AS template_question_id,
              COALESCE(a.question, '') AS question,
              COALESCE(a.answer, '') AS answer,
              {asrc_expr} AS answer_source,
              {conf_expr} AS confidence_level,
              {miss_expr} AS is_missing,
              {mrea_expr} AS missing_reason
            FROM survey_answers a
            WHERE a.survey_id=?
            ORDER BY a.id ASC
            """,
            (sid,),
        )
        arows = cur.fetchall()

    answers = []
    for r in arows:
        answers.append(
            (
                int(r["answer_id"]),
                (int(r["template_question_id"]) if r["template_question_id"] is not None else None),
                r["question"],
                r["answer"],
                r["answer_source"],
                r["confidence_level"],
                int(r["is_missing"] or 0),
                r["missing_reason"],
            )
        )

    qa = _compute_qa(
        template_id=template_id,
        gps_lat=srow_d["gps_lat"],
        gps_lng=srow_d["gps_lng"],
        coverage_node_id=srow_d.get("coverage_node_id"),
        answers=answers,
    )

    merged_flags: List[str] = list(qa.flags or [])
    if srow_d.get("qa_flags"):
        merged_flags.extend([f for f in (srow_d.get("qa_flags") or "").split(",") if f])
    if int(srow_d.get("gps_missing_flag") or 0) == 1:
        merged_flags.append("GPS_MISSING")
    if int(srow_d.get("duplicate_flag") or 0) == 1:
        merged_flags.append("DUPLICATE_FACILITY_DAY")

    # Preserve order while de-duplicating.
    seen = set()
    normalized_flags: List[str] = []
    for f in merged_flags:
        key = (f or "").strip().upper()
        if not key or key in seen:
            continue
        seen.add(key)
        normalized_flags.append(key)
    qa.flags = normalized_flags

    severity_boost = 0.0
    if "GPS_OUTSIDE_FIELD_AREA" in seen or "GPS_OUTSIDE_COVERAGE" in seen:
        severity_boost += 0.25
    if "FIELD_AREA_CLUSTER_SPIKE" in seen:
        severity_boost += 0.20
    if "UNLISTED_FACILITY_USED" in seen:
        severity_boost += 0.12
    if "DUPLICATE_FACILITY_DAY" in seen:
        severity_boost += 0.15
    if severity_boost > 0:
        qa.severity = round(min(1.0, float(qa.severity or 0) + severity_boost), 4)

    return header, answers, qa


def _list_tables() -> List[str]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [r["name"] for r in cur.fetchall()]


# -------------------------
# QA logic (lightweight but useful)
# -------------------------

def _compute_qa(
    template_id: Optional[int],
    gps_lat,
    gps_lng,
    coverage_node_id: Optional[int],
    answers: List[Tuple[Any, ...]],
) -> QASummary:
    flags: List[str] = []
    missing_required: List[str] = []

    total_answers = len(answers)
    empty_answer_count = sum(1 for a in answers if not (a[3] or "").strip())
    low_confidence_count = 0

    gps_present = bool(gps_lat is not None and gps_lng is not None)
    gps_missing = not gps_present
    gps_outside = False
    gps_distance_m: Optional[float] = None

    # Optional: validate GPS against coverage node geofence (if node has coordinates)
    if gps_present and coverage_node_id and _table_exists("coverage_nodes"):
        cols = _table_columns("coverage_nodes")
        if "gps_lat" in cols and "gps_lng" in cols:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT gps_lat, gps_lng, gps_radius_m FROM coverage_nodes WHERE id=? LIMIT 1",
                    (int(coverage_node_id),),
                )
                r = cur.fetchone()
            if r and r["gps_lat"] is not None and r["gps_lng"] is not None:
                gps_distance_m = _haversine_m(gps_lat, gps_lng, r["gps_lat"], r["gps_lng"])
                radius = float(r["gps_radius_m"]) if r["gps_radius_m"] is not None else 5000.0
                if gps_distance_m is not None and gps_distance_m > radius:
                    gps_outside = True

    # Missing required (only if template_id exists and template_questions table exists)
    if template_id is not None and "template_questions" in _list_tables():
        tq_cols = _table_columns("template_questions")
        has_is_required = "is_required" in tq_cols

        if has_is_required:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT id, question_text
                    FROM template_questions
                    WHERE template_id=? AND is_required=1
                    """,
                    (int(template_id),),
                )
                required = cur.fetchall()

            answered_qids = set()
            for a in answers:
                tqid = a[1]
                if tqid is not None and (a[3] or "").strip():
                    answered_qids.add(int(tqid))

            for rq in required:
                if int(rq["id"]) not in answered_qids:
                    missing_required.append(rq["question_text"])

    # Low confidence (if confidence scores exist)
    for a in answers:
        conf = a[5]
        if conf is None:
            continue
        try:
            if float(conf) < 0.5:
                low_confidence_count += 1
        except Exception:
            continue

    # Simple suspicious checks
    suspicious = False
    for a in answers:
        q = (a[2] or "").lower()
        val = (a[3] or "").strip().lower()

        # obvious placeholders
        if val in ("n/a", "na", "none", "nil", "test", "xxx"):
            suspicious = True
            break

        # extremely short for longtext-like questions
        if "challenge" in q and len(val) <= 2:
            suspicious = True
            break

    if missing_required:
        flags.append("MISSING_REQUIRED")
    if empty_answer_count > 0:
        flags.append("EMPTY_ANSWERS")
    if low_confidence_count > 0:
        flags.append("LOW_CONFIDENCE")
    if gps_missing:
        flags.append("GPS_MISSING")
    if gps_outside:
        flags.append("GPS_OUTSIDE_COVERAGE")
    if suspicious:
        flags.append("SUSPICIOUS_VALUES")

    # severity scoring (simple weighted)
    severity = 0.0
    severity += min(1.0, len(missing_required) / 5.0) * 0.45
    severity += min(1.0, empty_answer_count / 10.0) * 0.20
    severity += (0.20 if gps_missing else 0.0)
    severity += (0.15 if gps_outside else 0.0)
    severity += min(1.0, low_confidence_count / 6.0) * 0.15
    severity += (0.15 if suspicious else 0.0)

    return QASummary(
        total_answers=total_answers,
        missing_required_count=len(missing_required),
        missing_required_questions=missing_required,
        empty_answer_count=empty_answer_count,
        low_confidence_count=low_confidence_count,
        gps_missing=gps_missing,
        gps_present=gps_present,
        has_suspicious_values=suspicious,
        flags=flags,
        severity=round(severity, 4),
    )


# -------------------------
# QA Alerts Dashboard
# -------------------------

def qa_alerts_dashboard(
    limit: int = 50,
    project_id: str = "",
    template_id: Optional[int] = None,
    date_from: str = "",
    date_to: str = "",
    supervisor_id: str = "",
) -> List[QAAlert]:
    return qa_alerts_dashboard_filtered(
        limit=limit,
        project_id=project_id,
        template_id=template_id,
        date_from=date_from,
        date_to=date_to,
        supervisor_id=supervisor_id,
    )


def qa_alerts_dashboard_filtered(
    limit: int = 50,
    project_id: str = "",
    template_id: Optional[int] = None,
    date_from: str = "",
    date_to: str = "",
    severity_min: Optional[float] = None,
    flag: str = "",
    enumerator: str = "",
    supervisor_id: str = "",
) -> List[QAAlert]:
    """
    Looks at latest surveys and returns alerts where severity is notable.
    Optional filters: severity_min, flag (exact), enumerator contains.
    """
    rows = filter_surveys(
        limit=max(200, int(limit) * 4),
        project_id=project_id,
        template_id=str(template_id) if template_id is not None else "",
        date_from=date_from,
        date_to=date_to,
        supervisor_id=supervisor_id,
    )
    alerts: List[QAAlert] = []

    for (sid, facility_name, tplid, survey_type, enum, st, created_at) in rows:
        header, answers, qa = get_survey_details(int(sid))
        if not header:
            continue

        # Alert when severity is notable or when field-area control flags are present.
        qa_flags_upper = {(f or "").strip().upper() for f in (qa.flags or []) if f}
        critical_scope_flags = {
            "GPS_OUTSIDE_FIELD_AREA",
            "GPS_OUTSIDE_COVERAGE",
            "FIELD_AREA_CLUSTER_SPIKE",
            "UNLISTED_FACILITY_USED",
            "DUPLICATE_FACILITY_DAY",
        }
        if qa.severity >= 0.30 or ("MISSING_REQUIRED" in qa_flags_upper) or bool(qa_flags_upper.intersection(critical_scope_flags)):
            alerts.append(
                QAAlert(
                    survey_id=int(sid),
                    facility_name=facility_name,
                    enumerator_name=enum,
                    flags=qa.flags or [],
                    severity=float(qa.severity),
                )
            )

        if len(alerts) >= int(limit) * 2:
            break

    # highest severity first
    alerts.sort(key=lambda a: a.severity, reverse=True)

    if enumerator:
        enum_q = enumerator.strip().lower()
        alerts = [a for a in alerts if enum_q in (a.enumerator_name or "").lower()]
    if flag:
        flag_q = flag.strip().lower()
        alerts = [a for a in alerts if any(flag_q == (f or "").lower() for f in a.flags or [])]
    if severity_min is not None:
        alerts = [a for a in alerts if float(a.severity or 0) >= severity_min]

    return alerts[: int(limit)]


def analytics_overview(
    project_id: int,
    template_id: Optional[int] = None,
    date_from: str = "",
    date_to: str = "",
) -> Dict[str, Any]:
    pid = int(project_id)
    overview = {
        "expected_submissions": None,
        "completed_submissions": 0,
        "draft_submissions": 0,
        "last_activity": None,
        "project_created_at": None,
        "avg_completion_minutes": None,
        "median_completion_minutes": None,
        "outlier_count": 0,
    }
    with get_conn() as conn:
        cur = conn.cursor()
        if "expected_submissions" in _table_columns("projects"):
            cur.execute("SELECT expected_submissions, created_at FROM projects WHERE id=?", (pid,))
            row = cur.fetchone()
            if row:
                overview["expected_submissions"] = row["expected_submissions"]
                overview["project_created_at"] = row["created_at"]
        else:
            cur.execute("SELECT created_at FROM projects WHERE id=?", (pid,))
            row = cur.fetchone()
            if row:
                overview["project_created_at"] = row["created_at"]

        where = ["project_id=?"]
        params: List[Any] = [pid]
        if template_id is not None and _surveys_has("template_id"):
            where.append("template_id=?")
            params.append(int(template_id))
        if date_from:
            where.append("date(created_at) >= date(?)")
            params.append(date_from)
        if date_to:
            where.append("date(created_at) <= date(?)")
            params.append(date_to)
        if _surveys_has("deleted_at"):
            where.append("deleted_at IS NULL")

        where_sql = " AND ".join(where)

        cur.execute(
            f"""
            SELECT status, created_at, completed_at
            FROM surveys
            WHERE {where_sql}
            """,
            tuple(params),
        )
        rows = cur.fetchall()

    durations: List[float] = []
    last_activity = None
    for r in rows:
        status = (r["status"] or "").upper()
        if status == "COMPLETED":
            overview["completed_submissions"] += 1
        else:
            overview["draft_submissions"] += 1
        ts = r["completed_at"] or r["created_at"]
        if ts and (last_activity is None or str(ts) > str(last_activity)):
            last_activity = ts
        if r["created_at"] and r["completed_at"]:
            try:
                start = datetime.fromisoformat(str(r["created_at"]).replace("Z", "+00:00"))
                end = datetime.fromisoformat(str(r["completed_at"]).replace("Z", "+00:00"))
                minutes = (end - start).total_seconds() / 60.0
                if minutes >= 0:
                    durations.append(minutes)
            except Exception:
                pass

    overview["last_activity"] = last_activity
    if durations:
        durations.sort()
        overview["avg_completion_minutes"] = sum(durations) / len(durations)
        mid = len(durations) // 2
        overview["median_completion_minutes"] = (
            durations[mid] if len(durations) % 2 else (durations[mid - 1] + durations[mid]) / 2
        )
        med = overview["median_completion_minutes"] or 0
        overview["outlier_count"] = len([d for d in durations if d > (med * 2)]) if med else 0
    return overview


# Compatibility with analytics package naming
def analytics_kpis(template_id: Optional[int] = None, project_id: Optional[int] = None) -> Dict[str, Any]:
    """
    KPI strip values for Analytics Overview.
    If project_id is provided, uses project-scoped analytics_overview.
    If only template_id is provided, falls back to template-scoped counts.
    """
    if project_id is not None:
        ov = analytics_overview(int(project_id))
        expected = ov.get("expected_submissions")
        completed = int(ov.get("completed_submissions") or 0)
        drafts = int(ov.get("draft_submissions") or 0)
        completion_rate = (completed / expected * 100.0) if expected else None
        return {
            "expected": expected,
            "completed": completed,
            "drafts": drafts,
            "completion_rate": completion_rate,
            "last_activity": ov.get("last_activity") or "",
            "total_surveys": completed + drafts,
        }

    # Template-scoped fallback (legacy)
    cols_surveys = _table_columns("surveys")
    has_template_id = "template_id" in cols_surveys
    where = []
    params: List[Any] = []
    if template_id is not None and has_template_id:
        where.append("template_id = ?")
        params.append(int(template_id))
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) AS c FROM surveys{where_sql}", tuple(params))
        total = int(cur.fetchone()["c"] or 0)
        cur.execute(
            f"SELECT COUNT(*) AS c FROM surveys{where_sql} AND status='COMPLETED'"
            if where_sql
            else "SELECT COUNT(*) AS c FROM surveys WHERE status='COMPLETED'"
        )
        completed = int(cur.fetchone()["c"] or 0)
        cur.execute(
            f"SELECT COUNT(*) AS c FROM surveys{where_sql} AND status='DRAFT'"
            if where_sql
            else "SELECT COUNT(*) AS c FROM surveys WHERE status='DRAFT'"
        )
        drafts = int(cur.fetchone()["c"] or 0)
        cur.execute(
            f"""
            SELECT MAX(COALESCE(completed_at, created_at)) AS last_activity
            FROM surveys
            {where_sql}
            """,
            tuple(params),
        )
        last_activity = cur.fetchone()["last_activity"]
    return {
        "expected": None,
        "completed": completed,
        "drafts": drafts,
        "completion_rate": None,
        "last_activity": last_activity or "",
        "total_surveys": total,
    }


def submissions_timeline(
    project_id: int,
    days: int = 14,
    template_id: Optional[int] = None,
    date_from: str = "",
    date_to: str = "",
) -> List[Dict[str, Any]]:
    pid = int(project_id)
    where = ["project_id=?"]
    params: List[Any] = [pid]
    if template_id is not None and _surveys_has("template_id"):
        where.append("template_id=?")
        params.append(int(template_id))
    if date_from:
        where.append("date(created_at) >= date(?)")
        params.append(date_from)
    if date_to:
        where.append("date(created_at) <= date(?)")
        params.append(date_to)
    if not date_from and not date_to:
        where.append(f"date(created_at) >= date('now','localtime','-{int(days)} day')")
    if _surveys_has("deleted_at"):
        where.append("deleted_at IS NULL")
    where_sql = " AND ".join(where)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
              date(created_at) AS day,
              COUNT(*) AS total,
              SUM(CASE WHEN status='COMPLETED' THEN 1 ELSE 0 END) AS completed
            FROM surveys
            WHERE {where_sql}
            GROUP BY date(created_at)
            ORDER BY date(created_at) DESC
            """,
            tuple(params),
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def submissions_timeseries(days: int = 14, template_id: Optional[int] = None, project_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Compatibility for analytics package naming.
    Returns daily submission counts (completed + drafts_started).
    """
    if project_id is not None:
        rows = submissions_timeline(int(project_id), days=days)
        return [
            {"date": r.get("day") or "", "completed": int(r.get("completed") or 0), "drafts_started": int(r.get("total") or 0) - int(r.get("completed") or 0)}
            for r in rows
        ]
    cols_surveys = _table_columns("surveys")
    has_template_id = "template_id" in cols_surveys
    where = []
    params: List[Any] = []
    if template_id is not None and has_template_id:
        where.append("template_id = ?")
        params.append(int(template_id))
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT
          date(created_at) AS d,
          SUM(CASE WHEN status='DRAFT' THEN 1 ELSE 0 END) AS drafts_started,
          SUM(CASE WHEN status='COMPLETED' THEN 1 ELSE 0 END) AS completed
        FROM surveys
        {where_sql}
        GROUP BY date(created_at)
        ORDER BY d DESC
        LIMIT ?
    """
    params2 = list(params) + [int(days)]
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, tuple(params2))
        rows = cur.fetchall()
    out = []
    for r in reversed(rows):
        out.append(
            {
                "date": r["d"] or "",
                "drafts_started": int(r["drafts_started"] or 0),
                "completed": int(r["completed"] or 0),
            }
        )
    return out


def enumerator_performance(
    project_id: int,
    days: int = 7,
    template_id: Optional[int] = None,
    date_from: str = "",
    date_to: str = "",
) -> List[Dict[str, Any]]:
    if not _surveys_has("project_id"):
        return []

    pid = int(project_id)
    has_gps_missing_flag = _surveys_has("gps_missing_flag")
    has_duplicate_flag = _surveys_has("duplicate_flag")
    has_gps_lat = _surveys_has("gps_lat")

    qa_parts: List[str] = []
    if has_gps_missing_flag:
        qa_parts.append("COALESCE(gps_missing_flag,0)=1")
    if has_duplicate_flag:
        qa_parts.append("COALESCE(duplicate_flag,0)=1")
    qa_expr = " OR ".join(qa_parts) if qa_parts else "0=1"
    gps_ok_expr = "COALESCE(gps_missing_flag,0)=0" if has_gps_missing_flag else "1=1"
    gps_lat_expr = "gps_lat IS NOT NULL" if has_gps_lat else "0=1"

    where = ["project_id=?", "enumerator_name IS NOT NULL", "enumerator_name<>''"]
    params: List[Any] = [pid]
    if template_id is not None and _surveys_has("template_id"):
        where.append("template_id=?")
        params.append(int(template_id))
    if date_from:
        where.append("date(created_at) >= date(?)")
        params.append(date_from)
    if date_to:
        where.append("date(created_at) <= date(?)")
        params.append(date_to)
    if _surveys_has("deleted_at"):
        where.append("deleted_at IS NULL")
    where_sql = " AND ".join(where)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
              enumerator_name,
              COUNT(*) AS total_submissions,
              SUM(CASE WHEN status='COMPLETED' THEN 1 ELSE 0 END) AS completed_total,
              SUM(CASE WHEN status!='COMPLETED' THEN 1 ELSE 0 END) AS drafts_total,
              SUM(CASE WHEN date(created_at)=date('now','localtime') AND status='COMPLETED' THEN 1 ELSE 0 END) AS completed_today,
              SUM(CASE WHEN date(created_at)>=date('now','localtime','-{int(days)} day') AND status='COMPLETED' THEN 1 ELSE 0 END) AS completed_recent,
              SUM(CASE WHEN ({qa_expr}) THEN 1 ELSE 0 END) AS qa_flags,
              AVG(CASE WHEN status='COMPLETED' AND completed_at IS NOT NULL AND created_at IS NOT NULL
                THEN (julianday(completed_at) - julianday(created_at)) * 1440.0 ELSE NULL END) AS avg_completion_minutes,
              SUM(CASE WHEN status='COMPLETED' AND ({gps_ok_expr}) AND ({gps_lat_expr}) THEN 1 ELSE 0 END) AS gps_captured,
              SUM(CASE WHEN status='COMPLETED' THEN 1 ELSE 0 END) AS completed_for_gps
            FROM surveys
            WHERE {where_sql}
            GROUP BY enumerator_name
            ORDER BY completed_total DESC, total_submissions DESC
            """,
            tuple(params),
        )
        rows = cur.fetchall()
    out = []
    for r in rows:
        d = dict(r)
        gps_total = int(d.get("completed_for_gps") or 0)
        gps_captured = int(d.get("gps_captured") or 0)
        d["gps_capture_rate"] = (gps_captured / gps_total) if gps_total else None
        out.append(d)
    return out


# =========================================================
# Researcher / Enumerator Profile (Derived, no new tables)
# =========================================================
@dataclass
class ResearcherProfile:
    enumerator_name: str
    total_surveys: int
    completed: int
    drafts: int
    unique_facilities: int
    templates_used: int
    avg_completion_seconds: Optional[float]
    gps_capture_pct: Optional[float]
    last_activity: Optional[str]
    qa_alerts_count: int
    recent_alerts: List[Dict[str, Any]]


def get_researcher_profile(enumerator_name: str, alerts_limit: int = 10) -> ResearcherProfile:
    """
    Build a derived profile for a given enumerator name.
    No new tables required.
    """
    name = (enumerator_name or "").strip()
    if not name:
        raise ValueError("enumerator_name is required")

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              COUNT(*) AS total_surveys,
              SUM(CASE WHEN status='COMPLETED' THEN 1 ELSE 0 END) AS completed,
              SUM(CASE WHEN status='DRAFT' THEN 1 ELSE 0 END) AS drafts,
              COUNT(DISTINCT facility_id) AS unique_facilities,
              COUNT(DISTINCT template_id) AS templates_used,
              MAX(COALESCE(completed_at, created_at)) AS last_activity
            FROM surveys
            WHERE LOWER(enumerator_name) = LOWER(?)
            """,
            (name,),
        )
        row = cur.fetchone()

        total_surveys = int(row["total_surveys"] or 0)
        completed = int(row["completed"] or 0)
        drafts = int(row["drafts"] or 0)
        unique_facilities = int(row["unique_facilities"] or 0)
        templates_used = int(row["templates_used"] or 0)
        last_activity = row["last_activity"] if row else None

        cur.execute(
            """
            SELECT
              AVG((julianday(completed_at) - julianday(created_at)) * 86400.0) AS avg_seconds
            FROM surveys
            WHERE LOWER(enumerator_name)=LOWER(?)
              AND status='COMPLETED'
              AND created_at IS NOT NULL
              AND completed_at IS NOT NULL
            """,
            (name,),
        )
        r2 = cur.fetchone()
        avg_seconds = r2["avg_seconds"] if r2 else None
        avg_seconds = float(avg_seconds) if avg_seconds is not None else None

        cur.execute(
            """
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN gps_lat IS NOT NULL AND gps_lng IS NOT NULL THEN 1 ELSE 0 END) AS with_gps
            FROM surveys
            WHERE LOWER(enumerator_name)=LOWER(?)
            """,
            (name,),
        )
        r3 = cur.fetchone()
        total_for_gps = int(r3["total"] or 0)
        with_gps = int(r3["with_gps"] or 0)

    gps_capture_pct = None
    if total_for_gps > 0:
        gps_capture_pct = (with_gps / total_for_gps) * 100.0

    qa_alerts = qa_alerts_dashboard(limit=500)
    mine = [a for a in qa_alerts if (a.enumerator_name or "").strip().lower() == name.lower()]
    qa_count = len(mine)

    recent_alerts: List[Dict[str, Any]] = []
    for a in mine[: max(1, int(alerts_limit))]:
        recent_alerts.append(
            {
                "survey_id": a.survey_id,
                "facility_name": a.facility_name,
                "flags": a.flags,
                "severity": float(a.severity),
            }
        )

    return ResearcherProfile(
        enumerator_name=name,
        total_surveys=total_surveys,
        completed=completed,
        drafts=drafts,
        unique_facilities=unique_facilities,
        templates_used=templates_used,
        avg_completion_seconds=avg_seconds,
        gps_capture_pct=gps_capture_pct,
        last_activity=last_activity,
        qa_alerts_count=qa_count,
        recent_alerts=recent_alerts,
    )


def list_researchers(limit: int = 200) -> List[str]:
    """
    Returns distinct enumerator names (cleaned) for quick selection in UI.
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT enumerator_name
            FROM surveys
            WHERE enumerator_name IS NOT NULL AND TRIM(enumerator_name) <> ''
            ORDER BY enumerator_name COLLATE NOCASE
            LIMIT ?
            """,
            (int(limit),),
        )
        rows = cur.fetchall()
    return [str(r["enumerator_name"]).strip() for r in rows if r and r["enumerator_name"]]
