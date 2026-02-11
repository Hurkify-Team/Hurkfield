# coverage.py — OpenField Collect
# Coverage schemes and nodes (Country -> State -> LGA -> ...)

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from db import get_conn


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def create_scheme(name: str, description: str = "") -> int:
    name = (name or "").strip()
    if not name:
        raise ValueError("Scheme name is required.")
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO coverage_schemes (name, description, created_at) VALUES (?, ?, ?)",
            (name, (description or "").strip() or None, _now()),
        )
        conn.commit()
        return int(cur.lastrowid)


def list_schemes(limit: int = 200) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name, description, created_at FROM coverage_schemes ORDER BY id DESC LIMIT ?",
            (int(limit),),
        )
        return [dict(r) for r in cur.fetchall()]


def get_scheme(scheme_id: int) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name, description, created_at FROM coverage_schemes WHERE id=? LIMIT 1",
            (int(scheme_id),),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def create_node(
    scheme_id: int,
    name: str,
    parent_id: Optional[int] = None,
    gps_lat: Optional[float] = None,
    gps_lng: Optional[float] = None,
    gps_radius_m: Optional[float] = None,
) -> int:
    name = (name or "").strip()
    if not name:
        raise ValueError("Node name is required.")
    level_index = 0
    if parent_id is not None:
        try:
            p = get_node(int(parent_id))
            level_index = int(p.get("level_index") or 0) + 1 if p else 1
        except Exception:
            level_index = 1
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO coverage_nodes (scheme_id, name, parent_id, level_index, gps_lat, gps_lng, gps_radius_m, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (int(scheme_id), name, parent_id, int(level_index), gps_lat, gps_lng, gps_radius_m, _now()),
        )
        conn.commit()
        return int(cur.lastrowid)


def list_nodes(
    scheme_id: int,
    parent_id: Optional[int] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    where = "WHERE scheme_id=?"
    params = [int(scheme_id)]
    if parent_id is not None:
        where += " AND parent_id=?"
        params.append(int(parent_id))
    params.append(int(limit))
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT id, scheme_id, name, parent_id, level_index, gps_lat, gps_lng, gps_radius_m, created_at
            FROM coverage_nodes
            {where}
            ORDER BY id ASC
            LIMIT ?
            """,
            tuple(params),
        )
        return [dict(r) for r in cur.fetchall()]


def get_node(node_id: int) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, scheme_id, name, parent_id, level_index, gps_lat, gps_lng, gps_radius_m, created_at
            FROM coverage_nodes
            WHERE id=?
            LIMIT 1
            """,
            (int(node_id),),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def update_node(
    node_id: int,
    name: Optional[str] = None,
    parent_id: Optional[int] = None,
    gps_lat: Optional[float] = None,
    gps_lng: Optional[float] = None,
    gps_radius_m: Optional[float] = None,
) -> None:
    fields = []
    values = []
    if name is not None:
        fields.append("name=?")
        values.append((name or "").strip())
    if parent_id is not None:
        fields.append("parent_id=?")
        values.append(int(parent_id))
        try:
            p = get_node(int(parent_id))
            level_index = int(p.get("level_index") or 0) + 1 if p else 1
            fields.append("level_index=?")
            values.append(int(level_index))
        except Exception:
            pass
    if gps_lat is not None:
        fields.append("gps_lat=?")
        values.append(float(gps_lat))
    if gps_lng is not None:
        fields.append("gps_lng=?")
        values.append(float(gps_lng))
    if gps_radius_m is not None:
        fields.append("gps_radius_m=?")
        values.append(float(gps_radius_m))
    if not fields:
        return
    values.append(int(node_id))
    with get_conn() as conn:
        conn.execute(
            f"UPDATE coverage_nodes SET {', '.join(fields)} WHERE id=?",
            tuple(values),
        )
        conn.commit()


def delete_node(node_id: int) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM coverage_nodes WHERE id=?", (int(node_id),))
        conn.commit()
