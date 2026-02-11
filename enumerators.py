# enumerators.py — OpenField Collect
# Enumerator registry + assignments

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from db import get_conn


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _assignment_cols() -> List[str]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(enumerator_assignments)")
        return [r["name"] for r in cur.fetchall()]


def _enum_cols() -> List[str]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(enumerators)")
        return [r["name"] for r in cur.fetchall()]


def create_enumerator(
    project_id: Optional[int],
    name: str,
    code: str = "",
    phone: str = "",
    email: str = "",
    status: str = "ACTIVE",
) -> int:
    name = (name or "").strip()
    if not name:
        raise ValueError("Enumerator name is required.")
    cols = _enum_cols()
    fields = []
    values: List[Any] = []
    if "project_id" in cols:
        fields.append("project_id")
        values.append(int(project_id) if project_id is not None else None)
    if "name" in cols:
        fields.append("name")
        values.append(name)
    if "full_name" in cols:
        fields.append("full_name")
        values.append(name)
    if "code" in cols:
        fields.append("code")
        clean_code = (code or "").strip()
        values.append(clean_code if clean_code else None)
    if "phone" in cols:
        fields.append("phone")
        values.append((phone or "").strip())
    if "email" in cols:
        fields.append("email")
        values.append((email or "").strip())
    if "status" in cols:
        fields.append("status")
        values.append((status or "ACTIVE").strip().upper())
    if "is_active" in cols:
        fields.append("is_active")
        values.append(1 if (status or "ACTIVE").strip().upper() == "ACTIVE" else 0)
    if "created_at" in cols:
        fields.append("created_at")
        values.append(_now())
    if not fields:
        raise RuntimeError("Enumerators table missing expected columns.")
    placeholders = ",".join(["?"] * len(fields))
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"INSERT INTO enumerators ({', '.join(fields)}) VALUES ({placeholders})",
            tuple(values),
        )
        conn.commit()
        return int(cur.lastrowid)


def list_enumerators(project_id: Optional[int] = None, limit: int = 200) -> List[Dict[str, Any]]:
    cols = _enum_cols()
    where = ""
    params: List[Any] = []
    if project_id is not None:
        if "project_id" in cols:
            where = "WHERE project_id=? OR project_id IS NULL OR id IN (SELECT enumerator_id FROM enumerator_assignments WHERE project_id=?)"
            params.append(int(project_id))
            params.append(int(project_id))
        else:
            where = "WHERE id IN (SELECT enumerator_id FROM enumerator_assignments WHERE project_id=?)"
            params.append(int(project_id))
    params.append(int(limit))
    name_expr = "name" if "name" in cols else ("full_name AS name" if "full_name" in cols else "'' AS name")
    code_expr = "code" if "code" in cols else "'' AS code"
    project_expr = "project_id" if "project_id" in cols else "NULL AS project_id"
    status_expr = "status" if "status" in cols else ("CASE WHEN is_active=1 THEN 'ACTIVE' ELSE 'ARCHIVED' END AS status" if "is_active" in cols else "'ACTIVE' AS status")
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT id, {project_expr} AS project_id, {name_expr} AS name, {code_expr} AS code,
                   phone, email, {status_expr} AS status, created_at
            FROM enumerators
            {where}
            ORDER BY id DESC
            LIMIT ?
            """,
            tuple(params),
        )
        return [dict(r) for r in cur.fetchall()]


def get_enumerator(enumerator_id: int) -> Optional[Dict[str, Any]]:
    cols = _enum_cols()
    name_expr = "name" if "name" in cols else ("full_name AS name" if "full_name" in cols else "'' AS name")
    code_expr = "code" if "code" in cols else "'' AS code"
    project_expr = "project_id" if "project_id" in cols else "NULL AS project_id"
    status_expr = "status" if "status" in cols else ("CASE WHEN is_active=1 THEN 'ACTIVE' ELSE 'ARCHIVED' END AS status" if "is_active" in cols else "'ACTIVE' AS status")
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, {project_expr} AS project_id, {name_expr} AS name, {code_expr} AS code,
                   phone, email, {status_expr} AS status, created_at
            FROM enumerators
            WHERE id=?
            LIMIT 1
            """.format(project_expr=project_expr, name_expr=name_expr, code_expr=code_expr, status_expr=status_expr),
            (int(enumerator_id),),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def get_enumerator_by_code(project_id: Optional[int], code: str) -> Optional[Dict[str, Any]]:
    code = (code or "").strip()
    if not code:
        return None
    cols = _enum_cols()
    if "code" not in cols:
        return None
    where = "LOWER(code)=LOWER(?)"
    params: List[Any] = [code]
    if project_id is not None and "project_id" in cols:
        where += " AND project_id=?"
        params.append(int(project_id))
    name_expr = "name" if "name" in cols else ("full_name AS name" if "full_name" in cols else "'' AS name")
    code_expr = "code"
    project_expr = "project_id" if "project_id" in cols else "NULL AS project_id"
    status_expr = "status" if "status" in cols else ("CASE WHEN is_active=1 THEN 'ACTIVE' ELSE 'ARCHIVED' END AS status" if "is_active" in cols else "'ACTIVE' AS status")
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT id, {project_expr} AS project_id, {name_expr} AS name, {code_expr} AS code,
                   phone, email, {status_expr} AS status, created_at
            FROM enumerators
            WHERE {where}
            LIMIT 1
            """,
            tuple(params),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def update_enumerator(
    enumerator_id: int,
    name: Optional[str] = None,
    code: Optional[str] = None,
    phone: Optional[str] = None,
    email: Optional[str] = None,
    status: Optional[str] = None,
) -> None:
    cols = _enum_cols()
    fields = []
    values: List[Any] = []
    if name is not None:
        if "name" in cols:
            fields.append("name=?")
            values.append((name or "").strip())
        if "full_name" in cols:
            fields.append("full_name=?")
            values.append((name or "").strip())
    if code is not None:
        if "code" in cols:
            fields.append("code=?")
            clean_code = (code or "").strip()
            values.append(clean_code if clean_code else None)
    if phone is not None:
        if "phone" in cols:
            fields.append("phone=?")
            values.append((phone or "").strip())
    if email is not None:
        if "email" in cols:
            fields.append("email=?")
            values.append((email or "").strip())
    if status is not None:
        if "status" in cols:
            fields.append("status=?")
            values.append((status or "ACTIVE").strip().upper())
        if "is_active" in cols:
            fields.append("is_active=?")
            values.append(1 if (status or "ACTIVE").strip().upper() == "ACTIVE" else 0)
    if not fields:
        return
    values.append(int(enumerator_id))
    with get_conn() as conn:
        conn.execute(
            f"UPDATE enumerators SET {', '.join(fields)} WHERE id=?",
            tuple(values),
        )
        conn.commit()


def archive_enumerator(enumerator_id: int) -> None:
    update_enumerator(enumerator_id, status="ARCHIVED")


def activate_enumerator(enumerator_id: int) -> None:
    update_enumerator(enumerator_id, status="ACTIVE")


def assign_enumerator(
    project_id: Optional[int],
    enumerator_id: int,
    coverage_node_id: Optional[int] = None,
    template_id: Optional[int] = None,
    target_facilities_count: Optional[int] = None,
    scheme_id: Optional[int] = None,
    supervisor_id: Optional[int] = None,
) -> int:
    cols = _assignment_cols()
    fields = ["project_id", "enumerator_id", "coverage_node_id", "template_id", "target_facilities_count", "created_at"]
    values = [
        (int(project_id) if project_id is not None else None),
        int(enumerator_id),
        (int(coverage_node_id) if coverage_node_id is not None else None),
        (int(template_id) if template_id is not None else None),
        (int(target_facilities_count) if target_facilities_count is not None else None),
        _now(),
    ]
    if "supervisor_id" in cols:
        fields.insert(2, "supervisor_id")
        values.insert(2, (int(supervisor_id) if supervisor_id is not None else None))
    if "scheme_id" in cols:
        if scheme_id is None:
            with get_conn() as conn:
                cur = conn.cursor()
                try:
                    cur.execute("SELECT id FROM coverage_schemes ORDER BY id ASC LIMIT 1")
                    row = cur.fetchone()
                    if row:
                        scheme_id = int(row["id"])
                    else:
                        cur.execute(
                            "INSERT INTO coverage_schemes (name, description, created_at) VALUES (?, ?, ?)",
                            ("Default Coverage", "Auto-created for assignments", _now()),
                        )
                        conn.commit()
                        scheme_id = int(cur.lastrowid)
                except Exception:
                    scheme_id = 1
        fields.insert(3, "scheme_id")
        values.insert(3, int(scheme_id))
    if "coverage_node_id" in cols:
        if coverage_node_id is None:
            try:
                idx = fields.index("coverage_node_id")
                values[idx] = 0
            except Exception:
                pass
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            INSERT INTO enumerator_assignments
              ({', '.join(fields)})
            VALUES ({', '.join(['?'] * len(fields))})
            """,
            tuple(values),
        )
        conn.commit()
        return int(cur.lastrowid)


def list_assignments(
    project_id: Optional[int] = None,
    enumerator_id: Optional[int] = None,
    supervisor_id: Optional[int] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    cols = _assignment_cols()
    active_expr = "is_active" if "is_active" in cols else "1 AS is_active"
    supervisor_expr = "supervisor_id" if "supervisor_id" in cols else "NULL AS supervisor_id"
    where = []
    params: List[Any] = []
    if project_id is not None:
        where.append("(project_id=? OR project_id IS NULL)")
        params.append(int(project_id))
    if enumerator_id is not None:
        where.append("enumerator_id=?")
        params.append(int(enumerator_id))
    if supervisor_id is not None and "supervisor_id" in cols:
        where.append("supervisor_id=?")
        params.append(int(supervisor_id))
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    params.append(int(limit))
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT id, project_id, enumerator_id, {supervisor_expr} AS supervisor_id, coverage_node_id, template_id, target_facilities_count, {active_expr} AS is_active, created_at
            FROM enumerator_assignments
            {where_sql}
            ORDER BY id DESC
            LIMIT ?
            """,
            tuple(params),
        )
        return [dict(r) for r in cur.fetchall()]


def get_assignment(assignment_id: int) -> Optional[Dict[str, Any]]:
    cols = _assignment_cols()
    active_expr = "is_active" if "is_active" in cols else "1 AS is_active"
    supervisor_expr = "supervisor_id" if "supervisor_id" in cols else "NULL AS supervisor_id"
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, project_id, enumerator_id, {supervisor_expr} AS supervisor_id, coverage_node_id, template_id, target_facilities_count, {active_expr} AS is_active, created_at
            FROM enumerator_assignments
            WHERE id=?
            LIMIT 1
            """.format(active_expr=active_expr, supervisor_expr=supervisor_expr),
            (int(assignment_id),),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def get_assignment_for_enumerator(
    project_id: Optional[int],
    enumerator_id: int,
    template_id: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    cols = _assignment_cols()
    active_expr = "is_active" if "is_active" in cols else "1 AS is_active"
    supervisor_expr = "supervisor_id" if "supervisor_id" in cols else "NULL AS supervisor_id"
    where = ["enumerator_id=?"]
    params: List[Any] = [int(enumerator_id)]
    if project_id is not None:
        where.append("project_id=?")
        params.append(int(project_id))

    order_clause = "id DESC"
    if template_id is not None:
        order_clause = "CASE WHEN template_id=? THEN 0 WHEN template_id IS NULL THEN 1 ELSE 2 END, id DESC"
        params.insert(0, int(template_id))
        where_sql = " AND ".join(where)
        sql = f"""
            SELECT id, project_id, enumerator_id, {supervisor_expr} AS supervisor_id, coverage_node_id, template_id, target_facilities_count, {active_expr} AS is_active, created_at
            FROM enumerator_assignments
            WHERE {where_sql}
            ORDER BY {order_clause}
            LIMIT 1
        """
    else:
        where_sql = " AND ".join(where)
        sql = f"""
            SELECT id, project_id, enumerator_id, {supervisor_expr} AS supervisor_id, coverage_node_id, template_id, target_facilities_count, {active_expr} AS is_active, created_at
            FROM enumerator_assignments
            WHERE {where_sql}
            ORDER BY {order_clause}
            LIMIT 1
        """

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        row = cur.fetchone()
    return dict(row) if row else None


def update_assignment_target(assignment_id: int, target_facilities_count: Optional[int]) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE enumerator_assignments SET target_facilities_count=? WHERE id=?",
            (int(target_facilities_count) if target_facilities_count is not None else None, int(assignment_id)),
        )
        conn.commit()


def list_assignment_facilities(assignment_id: int) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT af.id, af.assignment_id, af.facility_id, af.status, af.done_survey_id, af.created_at,
                   f.name AS facility_name
            FROM assignment_facilities af
            LEFT JOIN facilities f ON f.id = af.facility_id
            WHERE af.assignment_id=?
            ORDER BY af.id ASC
            """,
            (int(assignment_id),),
        )
        return [dict(r) for r in cur.fetchall()]


def add_assignment_facility(assignment_id: int, facility_id: int) -> int:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO assignment_facilities (assignment_id, facility_id, status, created_at)
            VALUES (?, ?, 'PENDING', ?)
            """,
            (int(assignment_id), int(facility_id), _now()),
        )
        conn.commit()
        return int(cur.lastrowid)


def delete_assignment_facility(assignment_facility_id: int) -> None:
    with get_conn() as conn:
        conn.execute(
            "DELETE FROM assignment_facilities WHERE id=?",
            (int(assignment_facility_id),),
        )
        conn.commit()


def mark_assignment_facility_done(assignment_id: int, facility_id: int, survey_id: int) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE assignment_facilities
            SET status='DONE', done_survey_id=?
            WHERE assignment_id=? AND facility_id=?
            """,
            (int(survey_id), int(assignment_id), int(facility_id)),
        )
        conn.commit()


def delete_assignment(assignment_id: int) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM enumerator_assignments WHERE id=?", (int(assignment_id),))
        conn.commit()


def delete_enumerator(enumerator_id: int) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM enumerators WHERE id=?", (int(enumerator_id),))
        conn.commit()
