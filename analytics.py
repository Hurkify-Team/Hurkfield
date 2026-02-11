# analytics.py — OpenField Collect
# Analytics rendering helpers (project analytics)

from __future__ import annotations

from datetime import datetime
from typing import Mapping, Optional, Dict, Any, List
from urllib.parse import urlencode

from db import get_conn
import projects as prj
import supervision as sup
import templates as tpl
import coverage as cov
from flask import url_for


def _clean_date(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    try:
        return datetime.fromisoformat(raw).date().isoformat()
    except Exception:
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date().isoformat()
        except Exception:
            return ""


def _qs(params: Dict[str, Any], admin_key: str = "") -> str:
    cleaned: Dict[str, Any] = {k: v for k, v in params.items() if v not in (None, "", [])}
    if admin_key:
        cleaned["key"] = admin_key
    if not cleaned:
        return ""
    return "?" + urlencode(cleaned)


def render_project_analytics(project: Dict[str, Any], admin_key: str, args: Mapping[str, str]) -> str:
    project_id = int(project.get("id") or 0)
    if not project_id:
        return "<div class='card'><h2>Project not found</h2></div>"

    key_q = f"?key={admin_key}" if admin_key else ""

    tab = (args.get("tab") or "overview").strip().lower()
    filter_key = (args.get("filter") or "").strip().lower()
    template_raw = (args.get("template_id") or "").strip()
    date_from = _clean_date(args.get("date_from") or "")
    date_to = _clean_date(args.get("date_to") or "")

    template_rows = tpl.list_templates(300, project_id=project_id)
    template_ids = {int(t[0]) for t in template_rows}
    template_id = int(template_raw) if template_raw.isdigit() and int(template_raw) in template_ids else None

    template_label = "All templates"
    template_options = ["<option value=''>All templates</option>"]
    for t in template_rows:
        tid = int(t[0])
        name = (t[1] or f"Template {tid}").strip()
        selected = "selected" if template_id == tid else ""
        if template_id == tid:
            template_label = name
        template_options.append(f"<option value='{tid}' {selected}>{name}</option>")
    template_options_html = "".join(template_options)

    overview = sup.analytics_overview(
        int(project_id), template_id=template_id, date_from=date_from, date_to=date_to
    )
    enum_perf = sup.enumerator_performance(
        int(project_id), days=7, template_id=template_id, date_from=date_from, date_to=date_to
    )
    timeline = sup.submissions_timeline(
        int(project_id), days=30, template_id=template_id, date_from=date_from, date_to=date_to
    )
    sev_q = (args.get("severity") or "").strip().lower()
    flag_q = (args.get("flag") or "").strip()
    enum_q = (args.get("enumerator") or "").strip()
    sev_min = None
    if sev_q in ("high", "critical"):
        sev_min = 0.7
    elif sev_q in ("medium", "mid"):
        sev_min = 0.4
    elif sev_q in ("low",):
        sev_min = 0.3
    qa_alerts = sup.qa_alerts_dashboard_filtered(
        limit=200,
        project_id=str(project_id),
        template_id=template_id,
        date_from=date_from,
        date_to=date_to,
        severity_min=sev_min,
        flag=flag_q,
        enumerator=enum_q,
    )
    qa_all = sup.qa_alerts_dashboard(
        limit=200, project_id=str(project_id), template_id=template_id, date_from=date_from, date_to=date_to
    )
    flag_options = sorted({f for a in qa_all for f in (a.flags or []) if f})
    enum_options = sorted({(e.get("enumerator_name") or "").strip() for e in enum_perf if e.get("enumerator_name")})
    enum_option_items = ["<option value=''>All</option>"]
    for name in enum_options:
        selected = "selected" if enum_q == name else ""
        enum_option_items.append(f"<option value='{name}' {selected}>{name}</option>")
    enum_options_html = "".join(enum_option_items)
    org_name = None
    if project.get("organization_id"):
        org = prj.get_organization(int(project.get("organization_id")))
        org_name = org.get("name") if org else None

    try:
        projects = prj.list_projects(200)
    except Exception:
        projects = []

    project_options = []
    for p in projects:
        pid = int(p.get("id") or 0)
        if not pid:
            continue
        status = (p.get("status") or "ACTIVE").upper()
        status_text = " [Archived]" if status == "ARCHIVED" else (" [Draft]" if status == "DRAFT" else "")
        selected = "selected" if pid == project_id else ""
        project_options.append(f"<option value='{pid}' {selected}>{p.get('name')}{status_text}</option>")

    def _fmt_dt(value):
        if not value:
            return "—"
        try:
            raw = str(value).replace("Z", "+00:00")
            dt = datetime.fromisoformat(raw)
            return dt.strftime("%b %d, %Y · %H:%M")
        except Exception:
            return str(value)

    rollup_rows: List[str] = []
    show_rollup = tab == "overview" and len(projects) > 1 and not template_id
    rollup_note = "Filtered by date range" if (date_from or date_to) else "All time"
    if show_rollup:
        for p in projects:
            pid = int(p.get("id") or 0)
            if not pid:
                continue
            ov = sup.analytics_overview(pid, template_id=None, date_from=date_from, date_to=date_to)
            completed_p = int(ov.get("completed_submissions") or 0)
            drafts_p = int(ov.get("draft_submissions") or 0)
            total_p = completed_p + drafts_p
            expected_p = ov.get("expected_submissions")
            rate_p = (completed_p / expected_p * 100.0) if expected_p else None
            rollup_href = f"/ui/projects/{pid}/analytics" + _qs(
                {"tab": "overview", "date_from": date_from, "date_to": date_to}, admin_key
            )
            rollup_rate = f"{rate_p:.1f}%" if rate_p is not None else "—"
            rollup_rows.append(
                f"""
                <tr>
                  <td><a href="{rollup_href}">{p.get('name')}</a></td>
                  <td>{(p.get('status') or 'ACTIVE').title()}</td>
                  <td>{total_p}</td>
                  <td>{completed_p}</td>
                  <td>{drafts_p}</td>
                  <td>{rollup_rate}</td>
                  <td>{_fmt_dt(ov.get('last_activity'))}</td>
                </tr>
                """
            )

    def _sparkline_dual(values_a, values_b, width=640, height=200):
        if not values_a and not values_b:
            return "<div class='muted'>No data yet</div>"
        all_vals = [v for v in (values_a or []) + (values_b or []) if v is not None]
        if not all_vals:
            return "<div class='muted'>No data yet</div>"
        min_v = min(all_vals)
        max_v = max(all_vals)
        span = max_v - min_v if max_v != min_v else 1
        step = (width - 16) / max(1, max(len(values_a), len(values_b)) - 1)

        def _points(values):
            pts = []
            for i, v in enumerate(values):
                x = 8 + i * step
                y = 8 + (height - 16) * (1 - ((v - min_v) / span))
                pts.append(f"{x:.2f},{y:.2f}")
            return " ".join(pts)

        pts_a = _points(values_a or [])
        pts_b = _points(values_b or [])
        return f"""
        <svg width="100%" height="{height}" viewBox="0 0 {width} {height}" preserveAspectRatio="none">
          <defs>
            <linearGradient id="gridFade" x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stop-color="#7c3aed" stop-opacity="0.25"/>
              <stop offset="100%" stop-color="#7c3aed" stop-opacity="0"/>
            </linearGradient>
            <linearGradient id="lineGlow" x1="0" x2="1">
              <stop offset="0%" stop-color="#a855f7"/>
              <stop offset="100%" stop-color="#7c3aed"/>
            </linearGradient>
          </defs>
          <rect x="0" y="0" width="{width}" height="{height}" rx="16" fill="rgba(14,8,28,.75)" />
          <g stroke="rgba(168,85,247,.18)" stroke-width="1">
            {''.join([f'<line x1="8" y1="{8+i*32}" x2="{width-8}" y2="{8+i*32}" />' for i in range(1,5)])}
          </g>
          <polyline fill="url(#gridFade)" stroke="none" points="{pts_a} {width-8},{height-8} 8,{height-8}" />
          <polyline fill="none" stroke="url(#lineGlow)" stroke-width="3" points="{pts_a}" />
          <polyline fill="none" stroke="rgba(34,211,238,.9)" stroke-width="2" points="{pts_b}" />
        </svg>
        """

    tl_sorted = list(reversed(timeline))
    total_vals = [int(t.get("total") or 0) for t in tl_sorted]
    completed_vals = [int(t.get("completed") or 0) for t in tl_sorted]

    expected = overview.get("expected_submissions")
    completed = int(overview.get("completed_submissions") or 0)
    drafts = int(overview.get("draft_submissions") or 0)
    completion_rate = (completed / expected * 100.0) if expected else None
    last_activity = overview.get("last_activity")
    project_start = overview.get("project_created_at")
    avg_minutes = overview.get("avg_completion_minutes")
    median_minutes = overview.get("median_completion_minutes")
    outlier_count = overview.get("outlier_count") or 0
    if date_from or date_to:
        date_label = f"{date_from or 'Start'} -> {date_to or 'Now'}"
    else:
        date_label = f"{_fmt_dt(project_start)} -> {_fmt_dt(last_activity)}"

    def _fmt_minutes(value):
        if value is None:
            return "—"
        mins = float(value)
        if mins < 60:
            return f"{mins:.1f} min"
        hours = mins / 60.0
        return f"{hours:.1f} hrs"

    def _severity_color(sev: float) -> str:
        if sev >= 0.7:
            return "#dc2626"
        if sev >= 0.4:
            return "#f59e0b"
        return "#16a34a"

    def _calendar_cells():
        try:
            import calendar
            today = datetime.now()
            year = today.year
            month = today.month
            first_wday, days_in_month = calendar.monthrange(year, month)
            # calendar.monthrange: Monday=0, Sunday=6; we want Sunday first
            start = (first_wday + 1) % 7
            activity_days = set()
            for t in timeline:
                day = t.get("day") or ""
                try:
                    dt_day = datetime.fromisoformat(str(day))
                    if dt_day.year == year and dt_day.month == month:
                        activity_days.add(dt_day.day)
                except Exception:
                    continue
            if not activity_days and last_activity:
                try:
                    dt_last = datetime.fromisoformat(str(last_activity).replace("Z", "+00:00"))
                    if dt_last.year == year and dt_last.month == month:
                        activity_days.add(dt_last.day)
                except Exception:
                    pass
            if not activity_days:
                activity_days.add(today.day)
            cells = []
            labels = ["S", "M", "T", "W", "T", "F", "S"]
            for lbl in labels:
                cells.append(f"<div class='cal-day cal-head'>{lbl}</div>")
            for _ in range(start):
                cells.append("<div class='cal-day cal-empty'></div>")
            for d in range(1, days_in_month + 1):
                hit = " cal-hit" if d in activity_days else ""
                cells.append(f"<div class='cal-day{hit}'>{d}</div>")
            return "".join(cells)
        except Exception:
            return "".join([f"<div class='cal-day'>{d}</div>" for d in range(1, 31)])

    enum_rows = []
    enum_perf_sorted = []
    for e in enum_perf:
        total_val = int(e.get("total_submissions") or 0)
        qa_flags = int(e.get("qa_flags") or 0)
        qa_risk = qa_flags / max(1, total_val)
        e["qa_risk"] = qa_risk
        enum_perf_sorted.append(e)

    enum_perf_sorted.sort(key=lambda r: r.get("qa_risk", 0), reverse=True)

    if filter_key == "inactive":
        enum_perf_sorted = [e for e in enum_perf_sorted if int(e.get("completed_recent") or 0) == 0]
    elif filter_key == "consistent":
        enum_perf_sorted = [e for e in enum_perf_sorted if int(e.get("qa_flags") or 0) == 0 and int(e.get("drafts_total") or 0) == 0]
    elif filter_key == "high-risk":
        enum_perf_sorted = [e for e in enum_perf_sorted if (e.get("qa_risk") or 0) >= 0.2 or int(e.get("qa_flags") or 0) >= 2]

    for e in enum_perf_sorted:
        total_val = int(e.get("total_submissions") or 0)
        completed_val = int(e.get("completed_total") or 0)
        drafts_val = int(e.get("drafts_total") or 0)
        qa_flags = int(e.get("qa_flags") or 0)
        avg_minutes_row = e.get("avg_completion_minutes")
        gps_rate = e.get("gps_capture_rate")
        gps_pct = f"{int(gps_rate * 100)}%" if gps_rate is not None else "—"
        enum_rows.append(
            f"""
            <tr onclick="window.location.href='/ui/projects/{project_id}/researchers?name={e.get('enumerator_name') or ''}{key_q}'" style="cursor:pointer">
              <td>{e.get('enumerator_name') or '—'}</td>
              <td>{total_val}</td>
              <td>{completed_val}</td>
              <td>{drafts_val}</td>
              <td>{_fmt_minutes(avg_minutes_row)}</td>
              <td>{qa_flags}</td>
              <td>{gps_pct}</td>
                </tr>
                """
            )
    rollup_html = ""
    if show_rollup:
        rollup_html = f"""
        <div class="card" style="margin-top:16px">
          <div class="row" style="justify-content:space-between; align-items:center;">
            <h3 style="margin-top:0">Project rollup</h3>
            <div class="muted" style="font-size:12px">All projects · {rollup_note}</div>
          </div>
          <table class="table" style="margin-top:10px">
            <thead>
              <tr>
                <th>Project</th>
                <th style="width:120px">Status</th>
                <th style="width:120px">Total</th>
                <th style="width:120px">Completed</th>
                <th style="width:120px">Drafts</th>
                <th style="width:120px">Completion %</th>
                <th style="width:180px">Last activity</th>
              </tr>
            </thead>
            <tbody>
              {("".join(rollup_rows) if rollup_rows else "<tr><td colspan='7' class='muted' style='padding:18px'>No project activity yet.</td></tr>")}
            </tbody>
          </table>
        </div>
        """

    timeline_rows = []
    for t in timeline:
        timeline_rows.append(
            f"""
            <tr>
              <td>{t.get('day') or ''}</td>
              <td>{int(t.get('total') or 0)}</td>
              <td>{int(t.get('completed') or 0)}</td>
            </tr>
            """
        )

    qa_rows = []
    for a in qa_alerts:
        sev = float(a.severity or 0)
        sev_color = _severity_color(sev)
        qa_rows.append(
            f"""
            <tr>
              <td><span class="template-id">#{a.survey_id}</span></td>
              <td>{a.facility_name or '—'}</td>
              <td>{a.enumerator_name or '—'}</td>
              <td>{", ".join(a.flags or []) or "—"}</td>
              <td><span style="color:{sev_color}; font-weight:800">{sev:.2f}</span></td>
              <td><a class="btn btn-sm" href="/ui/surveys/{a.survey_id}{key_q}">View survey</a></td>
            </tr>
            """
        )

    scheme_id = None
    if template_id:
        cfg = tpl.get_template_config(int(template_id))
        if int(cfg.get("enable_coverage") or 0) == 1 and cfg.get("coverage_scheme_id"):
            scheme_id = int(cfg.get("coverage_scheme_id"))
    else:
        template_rows = tpl.list_templates(300, project_id=project_id)
        for t in template_rows:
            cfg = tpl.get_template_config(int(t[0]))
            if int(cfg.get("enable_coverage") or 0) == 1 and cfg.get("coverage_scheme_id"):
                scheme_id = int(cfg.get("coverage_scheme_id"))
                break
    coverage_nodes = cov.list_nodes(int(scheme_id), limit=5000) if scheme_id else []
    coverage_total = len([n for n in coverage_nodes if n.get("parent_id") is not None or n.get("name")])
    coverage_done = 0
    missing_nodes = []
    if scheme_id:
        where = ["project_id=?", "coverage_node_id IS NOT NULL", "status='COMPLETED'"]
        params = [int(project_id)]
        if template_id and sup._surveys_has("template_id"):
            where.append("template_id=?")
            params.append(int(template_id))
        if date_from:
            where.append("date(created_at) >= date(?)")
            params.append(date_from)
        if date_to:
            where.append("date(created_at) <= date(?)")
            params.append(date_to)
        if sup._surveys_has("deleted_at"):
            where.append("deleted_at IS NULL")
        where_sql = " AND ".join(where)
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                f"""
                SELECT DISTINCT coverage_node_id FROM surveys
                WHERE {where_sql}
                """,
                tuple(params),
            )
            covered_ids = {int(r["coverage_node_id"]) for r in cur.fetchall() if r["coverage_node_id"] is not None}
        coverage_done = len(covered_ids)
        missing_nodes = [n for n in coverage_nodes if int(n.get("id")) not in covered_ids]

    expected_coverage = project.get("expected_coverage")
    coverage_target = int(expected_coverage) if expected_coverage is not None else coverage_total
    coverage_pct = int((coverage_done / coverage_target) * 100) if coverage_target else 0

    tab_overview_class = "btn btn-sm btn-primary" if tab == "overview" else "btn btn-sm"
    tab_enum_class = "btn btn-sm btn-primary" if tab == "enumerators" else "btn btn-sm"
    tab_qa_class = "btn btn-sm btn-primary" if tab == "quality" else "btn btn-sm"
    tab_cov_class = "btn btn-sm btn-primary" if tab == "coverage" else "btn btn-sm"

    base_params = {
        "template_id": template_id,
        "date_from": date_from,
        "date_to": date_to,
    }
    tab_overview_href = url_for("ui_project_analytics", project_id=project_id) + _qs(
        {**base_params, "tab": "overview"}, admin_key
    )
    tab_enum_href = url_for("ui_project_analytics", project_id=project_id) + _qs(
        {**base_params, "tab": "enumerators", "filter": filter_key}, admin_key
    )
    tab_qa_href = url_for("ui_project_analytics", project_id=project_id) + _qs(
        {**base_params, "tab": "quality", "severity": sev_q, "flag": flag_q, "enumerator": enum_q}, admin_key
    )
    tab_cov_href = url_for("ui_project_analytics", project_id=project_id) + _qs(
        {**base_params, "tab": "coverage"}, admin_key
    )

    export_qs = _qs({"project_id": project_id}, admin_key)
    filter_clear_href = url_for("ui_project_analytics", project_id=project_id) + _qs({"tab": tab}, admin_key)
    enum_filter_high = url_for("ui_project_analytics", project_id=project_id) + _qs(
        {**base_params, "tab": "enumerators", "filter": "high-risk"}, admin_key
    )
    enum_filter_inactive = url_for("ui_project_analytics", project_id=project_id) + _qs(
        {**base_params, "tab": "enumerators", "filter": "inactive"}, admin_key
    )
    enum_filter_consistent = url_for("ui_project_analytics", project_id=project_id) + _qs(
        {**base_params, "tab": "enumerators", "filter": "consistent"}, admin_key
    )
    enum_filter_clear = url_for("ui_project_analytics", project_id=project_id) + _qs(
        {**base_params, "tab": "enumerators"}, admin_key
    )

    qa_clear_href = url_for("ui_project_analytics", project_id=project_id) + _qs(
        {**base_params, "tab": "quality"}, admin_key
    )

    html = f"""
    <style>
      body{{
        background:#f6f4ff;
        color:#1f2937;
      }}
      .muted{{color:#6b7280}}
      .nav-actions{{display:none}}
      .ana-shell{{
        display:grid;
        grid-template-columns: 220px 1fr;
        gap:18px;
        align-items:stretch;
      }}
      .ana-sidebar{{
        background:#ffffff;
        border:1px solid rgba(124,58,237,.18);
        border-radius:20px;
        padding:18px 14px;
        box-shadow:0 16px 40px rgba(15,18,34,.08);
        min-height:86vh;
      }}
      .ana-brand{{
        font-weight:800;
        color:#7c3aed;
        font-size:20px;
        margin-bottom:14px;
      }}
      .ana-nav{{
        display:flex;
        flex-direction:column;
        gap:8px;
        margin-top:8px;
      }}
      .ana-nav a{{
        padding:10px 12px;
        border-radius:12px;
        color:#4b5563;
        background:rgba(124,58,237,.06);
        border:1px solid transparent;
      }}
      .ana-nav a.active{{
        border-color:rgba(124,58,237,.35);
        background:rgba(124,58,237,.14);
        color:#4c1d95;
      }}
      .ana-content{{
        display:flex;
        flex-direction:column;
        gap:16px;
      }}
      .ana-topbar{{
        display:flex;
        align-items:center;
        justify-content:space-between;
        gap:16px;
        background:#ffffff;
        border:1px solid rgba(124,58,237,.18);
        border-radius:20px;
        padding:14px 18px;
        box-shadow:0 16px 40px rgba(15,18,34,.08);
      }}
      .ana-search{{
        flex:1;
        display:flex;
        align-items:center;
        gap:10px;
        background:rgba(124,58,237,.08);
        border:1px solid rgba(124,58,237,.2);
        border-radius:999px;
        padding:8px 14px;
        max-width:420px;
      }}
      .ana-search input{{
        background:transparent;
        border:none;
        color:#111827;
        outline:none;
        width:100%;
      }}
      .ana-project{{
        display:flex;
        flex-direction:column;
        gap:6px;
        min-width:200px;
      }}
      .ana-select{{
        padding:8px 12px;
        border-radius:12px;
        border:1px solid rgba(124,58,237,.2);
        background:rgba(124,58,237,.06);
        color:#1f2937;
      }}
      .ana-breadcrumb{{
        color:#7c3aed;
        font-size:12px;
        letter-spacing:.2em;
        text-transform:uppercase;
      }}
      .ana-title{{
        font-size:28px;
        font-weight:800;
        letter-spacing:-0.02em;
      }}
      .ana-hero{{
        background:linear-gradient(135deg, rgba(124,58,237,.12), rgba(168,85,247,.12));
        border:1px solid rgba(124,58,237,.25);
        border-radius:24px;
        padding:20px 22px;
        box-shadow:0 16px 40px rgba(15,18,34,.08);
      }}
      .ana-filters{{
        display:flex;
        flex-wrap:wrap;
        gap:12px;
        margin-top:14px;
        align-items:end;
      }}
      .ana-filters label{{
        font-size:12px;
        color:#6b7280;
        display:block;
        margin-bottom:6px;
      }}
      .ana-filters select,
      .ana-filters input{{
        padding:8px 10px;
        border-radius:10px;
        border:1px solid rgba(124,58,237,.25);
        background:rgba(124,58,237,.06);
        color:inherit;
      }}
      .filter-actions{{
        display:flex;
        gap:8px;
        align-items:end;
      }}
      .chip{{
        display:inline-flex;
        align-items:center;
        gap:6px;
        padding:6px 12px;
        border-radius:999px;
        font-size:11px;
        background:rgba(124,58,237,.16);
        color:#4c1d95;
        border:1px solid rgba(124,58,237,.35);
        letter-spacing:.08em;
        text-transform:uppercase;
      }}
      .ana-grid{{
        display:grid;
        grid-template-columns: repeat(5, minmax(150px, 1fr));
        gap:14px;
      }}
      .chart-wrap{{
        padding:14px;
        border-radius:16px;
        background:linear-gradient(180deg, rgba(124,58,237,.08), rgba(124,58,237,.03));
        border:1px solid rgba(124,58,237,.18);
      }}
      .widget-grid{{
        display:grid;
        grid-template-columns: 2fr 1.2fr;
        gap:16px;
        margin-top:16px;
      }}
      .activity-grid{{
        display:grid;
        grid-template-columns: 1.2fr .8fr;
        gap:16px;
        margin-top:16px;
      }}
      .activity-item{{
        display:flex;
        justify-content:space-between;
        font-size:12px;
        padding:8px 0;
        border-bottom:1px dashed rgba(124,58,237,.18);
      }}
      .calendar{{
        display:grid;
        grid-template-columns: repeat(7, 1fr);
        gap:6px;
        margin-top:10px;
        font-size:12px;
      }}
      .cal-day{{
        text-align:center;
        padding:6px 0;
        border-radius:8px;
        background:rgba(124,58,237,.06);
      }}
      .cal-head{{font-weight:700; background:transparent; color:#7c3aed}}
      .cal-empty{{background:transparent}}
      .cal-hit{{background:rgba(124,58,237,.35); color:#3b146b; font-weight:800; box-shadow:0 0 12px rgba(124,58,237,.35)}}
      .mini-bars{{
        display:grid;
        gap:8px;
      }}
      .mini-bar{{
        height:10px;
        border-radius:999px;
        background:linear-gradient(90deg, #7c3aed, #22d3ee);
        opacity:.85;
      }}
      @media(max-width:1100px){{
        .ana-grid{{grid-template-columns:1fr 1fr}}
        .widget-grid{{grid-template-columns:1fr}}
      }}
      .ana-card{{
        border:1px solid rgba(124,58,237,.18);
        background:#ffffff;
        border-radius:18px;
        padding:16px;
        box-shadow:0 16px 40px rgba(15,18,34,.08);
      }}
      .ana-kpi{{
        display:flex;
        align-items:center;
        justify-content:space-between;
        gap:10px;
      }}
      .ana-kpi .label{{font-size:12px; color:#6b7280;}}
      .ana-kpi .value{{
        font-size:18px;
        font-weight:900;
        color:#4c1d95;
        text-align:right;
        white-space:normal;
        word-break:break-word;
        max-width:140px;
        line-height:1.1;
        font-variant-numeric:tabular-nums;
      }}
      .ring-grid{{
        display:grid;
        grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
        gap:16px;
        margin-top:16px;
      }}
      .ring-card{{
        background:#ffffff;
        border:1px solid rgba(124,58,237,.18);
        border-radius:18px;
        padding:16px;
        box-shadow:0 16px 40px rgba(15,18,34,.08);
        display:flex;
        flex-direction:column;
        align-items:center;
        gap:10px;
      }}
      .ring{{
        --val:0;
        --ring:#7c3aed;
        width:86px;
        height:86px;
        border-radius:50%;
        background:conic-gradient(var(--ring) calc(var(--val)*1%), rgba(124,58,237,.12) 0);
        display:grid;
        place-items:center;
        position:relative;
        box-shadow:0 0 18px rgba(124,58,237,.25);
      }}
      .ring::before{{
        content:"";
        width:64px;
        height:64px;
        border-radius:50%;
        background:#ffffff;
        border:1px solid rgba(124,58,237,.18);
      }}
      .ring span{{
        position:absolute;
        font-size:16px;
        font-weight:800;
        color:#4c1d95;
      }}
      .tab-row .btn{{font-size:12px}}
      .qa-dot{{width:10px; height:10px; border-radius:999px; display:inline-block}}
      .card{{
        background:#ffffff;
        border:1px solid rgba(124,58,237,.18);
        box-shadow:0 16px 40px rgba(15,18,34,.08);
        color:#111827;
      }}
      .table th, .table td{{
        border-color:rgba(124,58,237,.18);
      }}
      .table th{{color:#4c1d95}}
      .table td{{color:#1f2937}}
      .btn{{
        background:rgba(124,58,237,.12);
        color:#4c1d95;
        border-color:rgba(124,58,237,.35);
      }}
      .btn:hover{{
        border-color:#7c3aed;
        box-shadow:0 10px 24px rgba(124,58,237,.2);
      }}
      .btn-primary{{
        background:linear-gradient(135deg, #7c3aed, #a855f7);
        border:none;
        color:#fff;
        box-shadow:0 12px 30px rgba(124,58,237,.35);
      }}
      .btn-primary:hover{{
        box-shadow:0 16px 40px rgba(124,58,237,.45);
      }}
      h1, h3{{color:#2b1c46}}
      html[data-theme="dark"] body{{
        background:
          radial-gradient(900px 380px at 10% -10%, rgba(124,58,237,.35), transparent 60%),
          radial-gradient(700px 300px at 90% 20%, rgba(168,85,247,.28), transparent 55%),
          #0b0616;
        color:#ece9ff;
      }}
      html[data-theme="dark"] .muted{{color:#b7b2d6}}
      html[data-theme="dark"] .ana-sidebar{{
        background:linear-gradient(180deg, #150824, #0e071d);
        border:1px solid rgba(139,92,246,.22);
        box-shadow:0 18px 50px rgba(0,0,0,.45);
      }}
      html[data-theme="dark"] .ana-brand{{color:#b988ff}}
      html[data-theme="dark"] .ana-nav a{{color:#dcd6ff; background:rgba(139,92,246,.06)}}
      html[data-theme="dark"] .ana-nav a.active{{border-color:rgba(139,92,246,.35); background:rgba(139,92,246,.16); color:#f3ecff}}
      html[data-theme="dark"] .ana-topbar{{background:linear-gradient(135deg, rgba(30,14,58,.92), rgba(20,10,40,.95)); border:1px solid rgba(139,92,246,.24); box-shadow:0 18px 50px rgba(0,0,0,.45)}}
      html[data-theme="dark"] .ana-search{{background:rgba(139,92,246,.1); border:1px solid rgba(139,92,246,.28)}}
      html[data-theme="dark"] .ana-search input{{color:#e9e3ff}}
      html[data-theme="dark"] .ana-select{{background:rgba(139,92,246,.12); border-color:rgba(139,92,246,.3); color:#e9e3ff}}
      html[data-theme="dark"] .ana-filters select,
      html[data-theme="dark"] .ana-filters input{{background:rgba(139,92,246,.12); border-color:rgba(139,92,246,.3); color:#e9e3ff}}
      html[data-theme="dark"] .ana-breadcrumb{{color:#cfc7ee}}
      html[data-theme="dark"] .ana-hero{{background:linear-gradient(135deg, rgba(28,16,52,.92), rgba(52,24,92,.88)); border:1px solid rgba(139,92,246,.28); box-shadow:0 18px 50px rgba(0,0,0,.45)}}
      html[data-theme="dark"] .chip{{background:rgba(139,92,246,.18); color:#f3ecff; border:1px solid rgba(139,92,246,.35)}}
      html[data-theme="dark"] .ana-card{{border:1px solid rgba(139,92,246,.22); background:linear-gradient(180deg, rgba(20,12,38,.92), rgba(12,8,26,.96)); box-shadow:0 14px 40px rgba(0,0,0,.4)}}
      html[data-theme="dark"] .ana-kpi .label{{color:#b7b2d6}}
      html[data-theme="dark"] .ana-kpi .value{{color:#f5f0ff}}
      html[data-theme="dark"] .ring-card{{
        background:linear-gradient(180deg, rgba(18,10,34,.92), rgba(10,6,22,.98));
        border:1px solid rgba(139,92,246,.22);
        box-shadow:0 14px 40px rgba(0,0,0,.4);
      }}
      html[data-theme="dark"] .ring{{
        background:conic-gradient(#a855f7 calc(var(--val)*1%), rgba(255,255,255,.06) 0);
        box-shadow:
          0 0 14px rgba(168,85,247,.55),
          0 0 28px rgba(124,58,237,.55),
          0 0 42px rgba(168,85,247,.35);
        filter:drop-shadow(0 0 10px rgba(168,85,247,.35));
      }}
      html[data-theme="dark"] .ring::before{{
        background:#0c071a;
        border:1px solid rgba(139,92,246,.25);
      }}
      html[data-theme="dark"] .ring span{{color:#f3ecff}}
      html[data-theme="dark"] .card{{background:linear-gradient(180deg, rgba(18,10,34,.92), rgba(10,6,22,.98)); border:1px solid rgba(139,92,246,.22); box-shadow:0 14px 40px rgba(0,0,0,.4); color:#e7e2ff}}
      html[data-theme="dark"] .table th, html[data-theme="dark"] .table td{{border-color:rgba(139,92,246,.18)}}
      html[data-theme="dark"] .table th{{color:#dcd6ff}}
      html[data-theme="dark"] .table td{{color:#e6e0ff}}
      html[data-theme="dark"] .btn{{background:rgba(139,92,246,.14); color:#efeaff; border-color:rgba(139,92,246,.35)}}
      html[data-theme="dark"] .btn:hover{{border-color:#a855f7; box-shadow:0 10px 24px rgba(168,85,247,.25)}}
      html[data-theme="dark"] h1, html[data-theme="dark"] h3{{color:#f6f1ff}}
      html[data-theme="dark"] .cal-head{{color:#dcd6ff}}
      html[data-theme="dark"] .cal-day{{background:rgba(139,92,246,.08)}}
      html[data-theme="dark"] .cal-empty{{background:transparent}}
      html[data-theme="dark"] .cal-hit{{background:rgba(124,58,237,.45); color:#f3ecff; box-shadow:0 0 12px rgba(124,58,237,.5)}}
      @media(max-width:1100px){{
        .ana-shell{{grid-template-columns:1fr}}
        .ana-sidebar{{min-height:auto}}
        .activity-grid{{grid-template-columns:1fr}}
      }}
    </style>

    <div class="ana-shell">
      <aside class="ana-sidebar">
        <div class="ana-brand">Dashboard</div>
        <div class="ana-nav">
          <a href="/{key_q}">Home</a>
          <a class="active" href="/ui/analytics{key_q}">Analytics</a>
          <a href="/ui/analytics/enumerators{key_q}">Enumerators</a>
          <a href="/ui/analytics/qa{key_q}">QA</a>
          <a href="/ui/analytics/coverage{key_q}">Coverage</a>
          <a href="/ui/projects{key_q}">Projects</a>
          <a href="/ui/templates{key_q}">Templates</a>
          <a href="/ui/surveys{key_q}">Submissions</a>
          <a href="/ui/exports{key_q}">Exports</a>
          <a href="/ui/adoption{key_q}">Adoption</a>
        </div>
        <div class="muted" style="margin-top:24px; font-size:12px; letter-spacing:.14em;">PROJECTS</div>
        <div class="ana-nav" style="margin-top:8px">
          <a href="{url_for('ui_project_detail', project_id=project_id)}{key_q}">{project.get('name')}</a>
          <a href="/ui{key_q}">All projects</a>
        </div>
        <div class="ana-nav" style="margin-top:16px">
          <button class="btn btn-sm" id="themeToggleAlt" type="button">Toggle theme</button>
        </div>
      </aside>
      <div class="ana-content">
        <div class="ana-topbar">
          <div class="ana-search">
            <span style="opacity:.7">Search</span>
            <input type="text" placeholder="Search analytics" />
          </div>
          <div class="ana-project">
            <div class="muted" style="font-size:11px; text-transform:uppercase; letter-spacing:.18em;">Project</div>
            <select id="projectSelect" class="ana-select">
              {''.join(project_options) if project_options else "<option value=''>No projects</option>"}
            </select>
          </div>
          <div class="row" style="gap:10px">
            <a class="btn btn-sm" href="/ui/exports/surveys.csv{export_qs}">CSV</a>
            <a class="btn btn-sm" href="/ui/exports/surveys.json{export_qs}">JSON</a>
            <a class="btn btn-sm" href="/ui/exports/metadata.csv{export_qs}">Audit</a>
          </div>
        </div>

        <div class="ana-breadcrumb">Dashboard / Home</div>

        <div class="ana-hero">
          <div class="row" style="justify-content:space-between; align-items:center;">
            <div>
              <div class="chip">Analytics</div>
              <h1 class="h1 ana-title" style="margin-top:8px">Project — {project.get('name')}</h1>
          <div class="muted">Status: {(project.get('status') or 'ACTIVE').title()} · Date range: {date_label}</div>
              <div class="muted" style="margin-top:6px">Template: {template_label} · Organization: {org_name or '—'}</div>
            </div>
            <div class="row" style="gap:10px">
              <a class="btn" href="{url_for('ui_project_detail', project_id=project_id)}{key_q}">Back to project</a>
            </div>
          </div>
          <div class="row tab-row" style="margin-top:16px; gap:8px; flex-wrap:wrap;">
            <a class="{tab_overview_class}" href="{tab_overview_href}">Overview</a>
            <a class="{tab_enum_class}" href="{tab_enum_href}">Enumerators</a>
            <a class="{tab_qa_class}" href="{tab_qa_href}">Data Quality</a>
            <a class="{tab_cov_class}" href="{tab_cov_href}">Coverage</a>
          </div>
          <form method="GET" action="{url_for('ui_project_analytics', project_id=project_id)}" class="ana-filters">
            <input type="hidden" name="tab" value="{tab}" />
            {f"<input type='hidden' name='key' value='{admin_key}' />" if admin_key else ""}
            {f"<input type='hidden' name='filter' value='{filter_key}' />" if filter_key else ""}
            {f"<input type='hidden' name='severity' value='{sev_q}' />" if sev_q else ""}
            {f"<input type='hidden' name='flag' value='{flag_q}' />" if flag_q else ""}
            {f"<input type='hidden' name='enumerator' value='{enum_q}' />" if enum_q else ""}
            <div>
              <label>Template</label>
              <select name="template_id">
                {template_options_html}
              </select>
            </div>
            <div>
              <label>From</label>
              <input type="date" name="date_from" value="{date_from}" />
            </div>
            <div>
              <label>To</label>
              <input type="date" name="date_to" value="{date_to}" />
            </div>
            <div class="filter-actions">
              <button class="btn btn-sm" type="submit">Apply</button>
              <a class="btn btn-sm" href="{filter_clear_href}">Clear</a>
            </div>
          </form>
        </div>

    {f'''
    {rollup_html}
    <div class="ana-grid">
      <div class="ana-card">
        <div class="ana-kpi"><div class="label">Expected submissions</div><div class="value">{expected if expected is not None else "—"}</div></div>
      </div>
      <div class="ana-card">
        <div class="ana-kpi"><div class="label">Completed</div><div class="value">{completed}</div></div>
      </div>
      <div class="ana-card">
        <div class="ana-kpi"><div class="label">Drafts pending</div><div class="value">{drafts}</div></div>
      </div>
      <div class="ana-card">
        <div class="ana-kpi"><div class="label">Completion %</div><div class="value">{f"{completion_rate:.1f}%" if completion_rate is not None else "—"}</div></div>
      </div>
      <div class="ana-card">
        <div class="ana-kpi"><div class="label">Last activity</div><div class="value">{_fmt_dt(last_activity)}</div></div>
      </div>
    </div>

    <div class="ring-grid">
      <div class="ring-card">
        <div class="ring" style="--val:{int(completion_rate) if completion_rate is not None else 0}; --ring:#7c3aed;">
          <span>{f"{completion_rate:.0f}%" if completion_rate is not None else "—"}</span>
        </div>
        <div class="label">Completion rate</div>
      </div>
      <div class="ring-card">
        <div class="ring" style="--val:{int((drafts / expected * 100) if expected else 0)}; --ring:#a855f7;">
          <span>{f"{int((drafts / expected * 100))}%" if expected else "—"}</span>
        </div>
        <div class="label">Drafts ratio</div>
      </div>
      <div class="ring-card">
        <div class="ring" style="--val:{coverage_pct}; --ring:#22d3ee;">
          <span>{coverage_pct}%</span>
        </div>
        <div class="label">Coverage</div>
      </div>
    </div>

    <div class="card" style="margin-top:16px">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <h3 style="margin-top:0">Submission flow</h3>
        <div class="muted">Avg: {_fmt_minutes(avg_minutes)} · Median: {_fmt_minutes(median_minutes)} · Outliers: {outlier_count}</div>
      </div>
      <div class="widget-grid">
        <div>
        <div class="muted" style="font-size:12px">Submissions per day (Completed vs Total)</div>
        <div class="chart-wrap">
          {_sparkline_dual(completed_vals, total_vals)}
        </div>
        </div>
        <div class="card" style="padding:14px">
          <div class="muted" style="font-size:12px">Performance mix</div>
          <div class="mini-bars" style="margin-top:10px">
            <div class="mini-bar" style="width:{min(100, int((completed / max(1, expected or completed or 1))*100))}%"></div>
            <div class="mini-bar" style="width:{min(100, int((drafts / max(1, expected or drafts or 1))*100))}%; background:linear-gradient(90deg, #f472b6, #a855f7);"></div>
            <div class="mini-bar" style="width:{min(100, int(coverage_pct))}%; background:linear-gradient(90deg, #22d3ee, #10b981);"></div>
          </div>
          <div class="muted" style="font-size:12px; margin-top:10px">Completed · Drafts · Coverage</div>
        </div>
      </div>
      <table class="table" style="margin-top:12px">
        <thead>
          <tr>
            <th>Date</th>
            <th style="width:140px">Total</th>
            <th style="width:160px">Completed</th>
          </tr>
        </thead>
        <tbody>
          {("".join(timeline_rows) if timeline_rows else "<tr><td colspan='3' class='muted' style='padding:18px'>No submissions yet.</td></tr>")}
        </tbody>
      </table>
    </div>

    <div class="activity-grid">
      <div class="card" style="padding:16px">
        <h3 style="margin-top:0">Recent activity</h3>
        <div class="activity-item"><span>New submissions today</span><b>{completed}</b></div>
        <div class="activity-item"><span>Drafts pending</span><b>{drafts}</b></div>
        <div class="activity-item"><span>QA alerts</span><b>{len(qa_alerts)}</b></div>
        <div class="activity-item" style="border-bottom:none"><span>Active enumerators</span><b>{len(enum_perf_sorted)}</b></div>
      </div>
      <div class="card" style="padding:16px">
        <h3 style="margin-top:0">Calendar</h3>
        <div class="muted" style="font-size:12px">This month</div>
        <div class="calendar">
          {_calendar_cells()}
        </div>
      </div>
    </div>
    ''' if tab == "overview" else ""}

    {f'''
    <div class="card" style="margin-top:16px">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <h3 style="margin-top:0">Enumerator performance</h3>
        <div class="row" style="gap:8px">
          <a class="btn btn-sm" href="{enum_filter_high}">High-risk</a>
          <a class="btn btn-sm" href="{enum_filter_inactive}">Inactive</a>
          <a class="btn btn-sm" href="{enum_filter_consistent}">Consistent</a>
          <a class="btn btn-sm" href="{enum_filter_clear}">Clear</a>
        </div>
      </div>
      <div class="muted" style="margin-bottom:8px">Sorted by QA risk (highest first). Click a row to open a researcher profile.</div>
      <table class="table">
        <thead>
          <tr>
            <th>Enumerator</th>
            <th style="width:120px">Assigned</th>
            <th style="width:120px">Completed</th>
            <th style="width:120px">Drafts</th>
            <th style="width:160px">Avg time</th>
            <th style="width:120px">QA flags</th>
            <th style="width:120px">GPS %</th>
          </tr>
        </thead>
        <tbody>
          {("".join(enum_rows) if enum_rows else "<tr><td colspan='7' class='muted' style='padding:18px'>No enumerator activity yet.</td></tr>")}
        </tbody>
      </table>
    </div>
    ''' if tab == "enumerators" else ""}

    {f'''
    <div class="card" style="margin-top:16px">
      <h3 style="margin-top:0">Data quality & QA alerts</h3>
      <form method="GET" action="{url_for('ui_project_analytics', project_id=project_id)}" style="display:flex; gap:10px; flex-wrap:wrap; align-items:center; margin-bottom:12px;">
        <input type="hidden" name="tab" value="quality" />
        {f"<input type='hidden' name='key' value='{admin_key}' />" if admin_key else ""}
        {f"<input type='hidden' name='template_id' value='{template_id}' />" if template_id else ""}
        {f"<input type='hidden' name='date_from' value='{date_from}' />" if date_from else ""}
        {f"<input type='hidden' name='date_to' value='{date_to}' />" if date_to else ""}
        <label class="muted" style="font-size:12px">Severity</label>
        <select name="severity" style="padding:8px 10px; border-radius:10px; border:1px solid rgba(124,58,237,.25); background:rgba(124,58,237,.08); color:inherit;">
          <option value="">All</option>
          <option value="high" {"selected" if sev_q in ("high","critical") else ""}>High</option>
          <option value="medium" {"selected" if sev_q in ("medium","mid") else ""}>Medium</option>
          <option value="low" {"selected" if sev_q == "low" else ""}>Low</option>
        </select>
        <label class="muted" style="font-size:12px">Flag</label>
        <select name="flag" style="padding:8px 10px; border-radius:10px; border:1px solid rgba(124,58,237,.25); background:rgba(124,58,237,.08); color:inherit;">
          <option value="">All</option>
          {''.join([f"<option value='{f}' {'selected' if flag_q==f else ''}>{f}</option>" for f in flag_options])}
        </select>
        <label class="muted" style="font-size:12px">Enumerator</label>
        <select name="enumerator" style="padding:8px 10px; border-radius:10px; border:1px solid rgba(124,58,237,.25); background:rgba(124,58,237,.06); color:inherit;">
          {enum_options_html}
        </select>
        <button class="btn btn-sm" type="submit">Apply</button>
        <a class="btn btn-sm" href="{qa_clear_href}">Clear</a>
      </form>
      <table class="table">
        <thead>
          <tr>
            <th style="width:90px">Survey</th>
            <th>Facility</th>
            <th style="width:180px">Enumerator</th>
            <th>Flags</th>
            <th style="width:120px">Severity</th>
            <th style="width:140px">Action</th>
          </tr>
        </thead>
        <tbody>
          {("".join(qa_rows) if qa_rows else "<tr><td colspan='6' class='muted' style='padding:18px'>No QA alerts yet.</td></tr>")}
        </tbody>
      </table>
    </div>
    ''' if tab == "quality" else ""}

    {f'''
    <div class="card" style="margin-top:16px">
      <h3 style="margin-top:0">Coverage progress</h3>
      {f"<div class='muted'>Coverage scheme not enabled for this project.</div>" if not scheme_id else ""}
      {f"""
      <div class='ana-grid' style='margin-top:8px'>
        <div class='ana-card'>
          <div class='ana-kpi'><div class='label'>Coverage achieved</div><div class='value'>{coverage_done}/{coverage_target}</div></div>
        </div>
        <div class='ana-card'>
          <div class='ana-kpi'><div class='label'>Coverage %</div><div class='value'>{coverage_pct}%</div></div>
        </div>
      </div>
      <div class='card' style='margin-top:12px'>
        <div class='muted' style='margin-bottom:8px'>Gaps (not yet covered)</div>
        <table class='table'>
          <thead><tr><th>ID</th><th>Location</th><th>Parent</th></tr></thead>
          <tbody>
            {("".join([f"<tr><td><span class='template-id'>#{n.get('id')}</span></td><td>{n.get('name')}</td><td class='muted'>{next((p.get('name') for p in coverage_nodes if p.get('id') == n.get('parent_id')), '—')}</td></tr>" for n in missing_nodes[:50]]) if missing_nodes else "<tr><td colspan='3' class='muted' style='padding:18px'>No coverage gaps detected.</td></tr>")}
          </tbody>
        </table>
      </div>
      """ if scheme_id else ""}
    </div>
    ''' if tab == "coverage" else ""}
    </div>
    </div>
    <script>
      const altToggle=document.getElementById("themeToggleAlt");
      if(altToggle){{
        altToggle.onclick=()=>{{
          const root=document.documentElement;
          const next=root.getAttribute("data-theme")==="dark" ? "light" : "dark";
          root.setAttribute("data-theme", next);
          localStorage.setItem("openfield_theme", next);
        }};
      }}
      const projectSelect=document.getElementById("projectSelect");
      if(projectSelect){{
        projectSelect.addEventListener("change", ()=>{{
          if(!projectSelect.value){{return;}}
          const nextBase=`/ui/projects/${{projectSelect.value}}/analytics`;
          const params=new URLSearchParams(window.location.search);
          const adminKey="{admin_key}";
          if(adminKey){{
            params.set("key", adminKey);
          }}
          const qs=params.toString();
          window.location.href=qs ? `${{nextBase}}?${{qs}}` : nextBase;
        }});
      }}
    </script>
    """
    return html
