# app.py — HurkField Collect (One-file Full MVP, schema-tolerant)
# DELETE your current app.py and paste this entire file.

from __future__ import annotations

import os
import html
import secrets
import json
import io
import base64
import re
import random
import uuid
import hashlib
from datetime import datetime, timedelta
from typing import Optional, List, Any, Dict
import requests

from flask import Flask, request, jsonify, redirect, url_for, render_template_string, render_template, send_file, make_response, g, session
from werkzeug.security import generate_password_hash, check_password_hash
try:
    from authlib.integrations.flask_client import OAuth
    _OAUTH_IMPORT_ERROR = ""
except Exception as _e:
    OAuth = None
    _OAUTH_IMPORT_ERROR = str(_e)
from werkzeug.utils import secure_filename
from werkzeug.datastructures import MultiDict

from db import init_db, get_conn
import config
import templates as tpl
import supervision as sup
import exports as exp
import projects as prj
import coverage as cov
import enumerators as enum
import analytics as ana


APP_NAME = config.APP_NAME
APP_VERSION = config.APP_VERSION
UPLOAD_DIR = config.UPLOAD_DIR
os.makedirs(UPLOAD_DIR, exist_ok=True)
EXPORT_DIR = config.EXPORT_DIR
os.makedirs(EXPORT_DIR, exist_ok=True)
APP_ENV = config.APP_ENV
PLATFORM_MODE = config.PLATFORM_MODE
REQUIRE_SUPERVISOR_KEY = config.REQUIRE_SUPERVISOR_KEY
PROJECT_REQUIRED = config.PROJECT_REQUIRED
SUPERVISOR_KEY_PARAM = config.SUPERVISOR_KEY_PARAM
SUPERVISOR_KEY_COOKIE = config.SUPERVISOR_KEY_COOKIE
SECRET_KEY = config.SECRET_KEY or secrets.token_urlsafe(32)
SMTP_HOST = config.SMTP_HOST
SMTP_PORT = config.SMTP_PORT
SMTP_USER = config.SMTP_USER
SMTP_PASS = config.SMTP_PASS
SMTP_TLS = config.SMTP_TLS
SMTP_FROM = config.SMTP_FROM
TRANSCRIBE_PROVIDER = config.TRANSCRIBE_PROVIDER
TRANSCRIBE_OPENAI_KEY = config.TRANSCRIBE_OPENAI_KEY
TRANSCRIBE_MODEL = config.TRANSCRIBE_MODEL
TRANSCRIBE_DEEPGRAM_KEY = config.TRANSCRIBE_DEEPGRAM_KEY
TRANSCRIBE_DEEPGRAM_MODEL = config.TRANSCRIBE_DEEPGRAM_MODEL
TRANSCRIBE_LANGUAGE = config.TRANSCRIBE_LANGUAGE
TRANSCRIBE_TIMEOUT = config.TRANSCRIBE_TIMEOUT

# Optional lightweight supervisor protection (MVP-only):
# If OPENFIELD_ADMIN_KEY is set, /ui routes require ?key=<that value>
ADMIN_KEY = config.ADMIN_KEY
ENABLE_SERVER_DRAFTS = config.ENABLE_SERVER_DRAFTS
DRAFTS_TABLE = config.DRAFTS_TABLE

app = Flask(__name__)
app.secret_key = SECRET_KEY
app.permanent_session_lifetime = timedelta(days=30)
oauth = OAuth(app) if OAuth else None
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024  # 10MB

UI_BRAND = {
    "name": "HurkField",
    "primary": "#7C3AED",
    "logo": "/static/logos/hurkfield.jpeg",
}

DOCS_ROOT = os.path.join(os.path.dirname(__file__), "docs")

PLAYBOOKS = {
    "health_facility": {
        "label": "Health Facility Assessment",
        "project": {
            "name": "Health Facility Assessment Pilot",
            "description": "Facility-level readiness, staffing, and services.",
        },
        "templates": [
            {
                "name": "Health Facility Assessment Form",
                "description": "Facility profile, staffing, and services snapshot.",
                "enable_consent": 0,
                "enable_attestation": 0,
                "sections": [
                    ("Facility profile", [
                        {"text": "Facility name", "type": "TEXT", "required": 1},
                        {"text": "Facility type", "type": "DROPDOWN", "choices": ["Hospital", "Clinic", "PHC", "Other"]},
                        {"text": "Ownership", "type": "DROPDOWN", "choices": ["Public", "Private", "Faith-based", "NGO"]},
                        {"text": "Catchment population", "type": "NUMBER"},
                    ]),
                    ("Staffing", [
                        {"text": "Total clinical staff", "type": "NUMBER", "required": 1},
                        {"text": "Total non-clinical staff", "type": "NUMBER"},
                        {"text": "Has at least one licensed clinician on-site?", "type": "YESNO", "required": 1},
                    ]),
                    ("Services & supplies", [
                        {"text": "Essential drug stock available today?", "type": "YESNO", "required": 1},
                        {"text": "Basic lab available?", "type": "YESNO"},
                        {"text": "Electricity available now?", "type": "YESNO", "required": 1},
                        {"text": "Water available now?", "type": "YESNO"},
                    ]),
                    ("Notes", [
                        {"text": "Key issues observed", "type": "LONGTEXT"},
                    ]),
                ],
            },
        ],
        "coverage_levels": ["State", "LGA", "Facility"],
        "docs_path": "playbooks/health-facility-assessment.md",
        "export_paths": [
            {"label": "CSV export", "path": "examples/health-facility-export.csv"},
            {"label": "JSON export", "path": "examples/health-facility-export.json"},
        ],
    },
    "ngo_baseline": {
        "label": "NGO Baseline & Endline Survey",
        "project": {
            "name": "NGO Baseline Pilot",
            "description": "Baseline indicators for program planning.",
        },
        "templates": [
            {
                "name": "NGO Baseline Survey Form",
                "description": "Household and program baseline indicators.",
                "enable_consent": 1,
                "enable_attestation": 0,
                "sections": [
                    ("Respondent", [
                        {"text": "Respondent name", "type": "TEXT"},
                        {"text": "Respondent age", "type": "NUMBER", "required": 1},
                        {"text": "Respondent gender", "type": "DROPDOWN", "choices": ["Female", "Male", "Other", "Prefer not to say"]},
                    ]),
                    ("Household", [
                        {"text": "Household size", "type": "NUMBER", "required": 1},
                        {"text": "Primary livelihood", "type": "DROPDOWN", "choices": ["Farming", "Trade", "Services", "Other"]},
                        {"text": "Access to clean water?", "type": "YESNO", "required": 1},
                        {"text": "Main water source", "type": "DROPDOWN", "choices": ["Borehole", "Well", "River", "Piped", "Other"]},
                    ]),
                    ("Program indicators", [
                        {"text": "Children in school?", "type": "YESNO"},
                        {"text": "Health facility within 5km?", "type": "YESNO"},
                        {"text": "Monthly income range", "type": "DROPDOWN", "choices": ["<50", "50-150", "150-300", "300+"]},
                    ]),
                    ("Notes", [
                        {"text": "Enumerator observations", "type": "LONGTEXT"},
                    ]),
                ],
            },
            {
                "name": "NGO Endline Survey Form",
                "description": "Endline outcomes and change tracking.",
                "enable_consent": 1,
                "enable_attestation": 0,
                "sections": [
                    ("Respondent", [
                        {"text": "Respondent name", "type": "TEXT"},
                        {"text": "Respondent age", "type": "NUMBER", "required": 1},
                        {"text": "Respondent gender", "type": "DROPDOWN", "choices": ["Female", "Male", "Other", "Prefer not to say"]},
                    ]),
                    ("Household", [
                        {"text": "Household size", "type": "NUMBER", "required": 1},
                        {"text": "Primary livelihood", "type": "DROPDOWN", "choices": ["Farming", "Trade", "Services", "Other"]},
                        {"text": "Access to clean water?", "type": "YESNO", "required": 1},
                        {"text": "Main water source", "type": "DROPDOWN", "choices": ["Borehole", "Well", "River", "Piped", "Other"]},
                    ]),
                    ("Program outcomes", [
                        {"text": "Children in school?", "type": "YESNO"},
                        {"text": "Health facility within 5km?", "type": "YESNO"},
                        {"text": "Monthly income range", "type": "DROPDOWN", "choices": ["<50", "50-150", "150-300", "300+"]},
                        {"text": "What improved most since baseline?", "type": "TEXT"},
                        {"text": "What remains a challenge?", "type": "LONGTEXT"},
                    ]),
                    ("Notes", [
                        {"text": "Enumerator observations", "type": "LONGTEXT"},
                    ]),
                ],
            },
        ],
        "coverage_levels": ["Country", "State", "LGA", "Community", "Household"],
        "docs_path": "playbooks/ngo-baseline-survey.md",
        "export_paths": [
            {"label": "Baseline CSV", "path": "examples/ngo-baseline-export.csv"},
            {"label": "Baseline JSON", "path": "examples/ngo-baseline-export.json"},
            {"label": "Endline CSV", "path": "examples/ngo-endline-export.csv"},
            {"label": "Endline JSON", "path": "examples/ngo-endline-export.json"},
        ],
    },
    "academic_field": {
        "label": "Academic Field Research",
        "project": {
            "name": "Academic Field Research Pilot",
            "description": "Ethics-first data collection with attestation.",
        },
        "templates": [
            {
                "name": "Academic Field Research Form",
                "description": "Research module with consent and attestation.",
                "enable_consent": 1,
                "enable_attestation": 1,
                "is_sensitive": 1,
                "sections": [
                    ("Respondent profile", [
                        {"text": "Participant code", "type": "TEXT"},
                        {"text": "Respondent age", "type": "NUMBER", "required": 1},
                        {"text": "Education level", "type": "DROPDOWN", "choices": ["None", "Primary", "Secondary", "Tertiary"]},
                        {"text": "Employment status", "type": "DROPDOWN", "choices": ["Employed", "Self-employed", "Unemployed", "Student"]},
                    ]),
                    ("Research module", [
                        {"text": "Q1: Primary outcome", "type": "TEXT", "required": 1},
                        {"text": "Q2: Secondary outcome", "type": "TEXT"},
                        {"text": "Q3: Follow-up needed?", "type": "YESNO"},
                    ]),
                    ("Notes", [
                        {"text": "Researcher notes", "type": "LONGTEXT"},
                    ]),
                ],
            },
        ],
        "coverage_levels": ["Country", "Region", "Site", "Participant"],
        "docs_path": "playbooks/academic-field-research.md",
        "export_paths": [
            {"label": "CSV export", "path": "examples/academic-field-export.csv"},
            {"label": "JSON export", "path": "examples/academic-field-export.json"},
        ],
    },
}

OPERATOR_DOCS = [
    {"title": "How to run your first project", "path": "operator/how-to-run-first-project.md"},
    {"title": "How enumerators collect data", "path": "operator/how-enumerators-collect-data.md"},
    {"title": "How supervisors review & export", "path": "operator/how-supervisors-review-export.md"},
    {"title": "Decision-maker guide", "path": "operator/decision-maker-guide.md"},
]

PILOT_DOCS = [
    {"title": "Deployment checklist", "path": "pilot/deployment-checklist.md"},
    {"title": "Feedback log template", "path": "pilot/feedback-log-template.md"},
]

POSITIONING_DOCS = [
    {"title": "Positioning brief", "path": "positioning/positioning-brief.md"},
    {"title": "Pitch outline", "path": "positioning/pitch-outline.md"},
    {"title": "Case study template", "path": "positioning/case-study-template.md"},
]


def _safe_docs_path(rel_path: str) -> Optional[str]:
    rel = (rel_path or "").strip().lstrip("/\\")
    if not rel:
        return None
    base = os.path.abspath(DOCS_ROOT)
    target = os.path.abspath(os.path.join(base, rel))
    if not (target == base or target.startswith(base + os.sep)):
        return None
    if not os.path.isfile(target):
        return None
    return target


def _markdown_to_html(md_text: str) -> str:
    lines = (md_text or "").splitlines()
    parts = []
    in_list = False
    in_code = False
    code_lines = []

    def close_list():
        nonlocal in_list
        if in_list:
            parts.append("</ul>")
            in_list = False

    for line in lines:
        stripped = line.rstrip()

        if stripped.startswith("```"):
            if in_code:
                parts.append("<pre><code>" + html.escape("\n".join(code_lines)) + "</code></pre>")
                code_lines = []
                in_code = False
            else:
                close_list()
                in_code = True
            continue

        if in_code:
            code_lines.append(line)
            continue

        if not stripped:
            close_list()
            continue

        if stripped.startswith("# "):
            close_list()
            parts.append(f"<h1>{html.escape(stripped[2:])}</h1>")
            continue
        if stripped.startswith("## "):
            close_list()
            parts.append(f"<h2>{html.escape(stripped[3:])}</h2>")
            continue
        if stripped.startswith("### "):
            close_list()
            parts.append(f"<h3>{html.escape(stripped[4:])}</h3>")
            continue

        if stripped.startswith("- "):
            if not in_list:
                parts.append("<ul>")
                in_list = True
            parts.append(f"<li>{html.escape(stripped[2:])}</li>")
            continue

        close_list()
        parts.append(f"<p>{html.escape(stripped)}</p>")

    if in_code:
        parts.append("<pre><code>" + html.escape("\n".join(code_lines)) + "</code></pre>")

    close_list()
    return "\n".join(parts)


def _find_project_by_name(name: str) -> Optional[dict]:
    target = (name or "").strip().lower()
    if not target:
        return None
    for p in prj.list_projects(500):
        if (p.get("name") or "").strip().lower() == target:
            return p
    return None


def _find_template_by_name(name: str, project_id: int) -> Optional[int]:
    target = (name or "").strip().lower()
    if not target:
        return None
    for t in tpl.list_templates(500, project_id=project_id):
        if (t[1] or "").strip().lower() == target:
            return int(t[0])
    return None


def _ensure_coverage_scheme(name: str, levels) -> int:
    target = (name or "").strip().lower()
    scheme = None
    for s in cov.list_schemes(500):
        if (s.get("name") or "").strip().lower() == target:
            scheme = s
            break
    scheme_id = int(scheme.get("id")) if scheme else cov.create_scheme(name)

    nodes = cov.list_nodes(scheme_id, limit=2000)
    if not nodes and levels:
        parent_id = None
        for level in levels:
            node_name = f"{level} (edit me)"
            parent_id = cov.create_node(scheme_id, node_name, parent_id=parent_id)

    return scheme_id


def _create_playbook(playbook_key: str) -> dict:
    if playbook_key not in PLAYBOOKS:
        raise ValueError("Unknown playbook.")
    pb = PLAYBOOKS[playbook_key]

    project_name = pb["project"]["name"]
    project = _find_project_by_name(project_name)
    if project:
        project_id = int(project.get("id"))
    else:
        project_id = prj.create_project(
            name=project_name,
            description=pb["project"]["description"],
            status="ACTIVE",
            source="playbook",
            assignment_mode="OPTIONAL",
            is_test_project=1,
        )

    scheme_name = f"{pb['label']} Coverage"
    scheme_id = _ensure_coverage_scheme(scheme_name, pb.get("coverage_levels", []))

    template_ids = []
    templates_cfg = pb.get("templates") or []

    def seed_questions(target_template_id: int, sections) -> None:
        existing_questions = tpl.get_template_questions(target_template_id)
        if existing_questions:
            return
        for section_title, questions in sections:
            tpl.add_template_question(
                target_template_id,
                f"## {section_title}",
                question_type="TEXT",
                is_required=0,
            )
            for q in questions:
                qid = tpl.add_template_question(
                    target_template_id,
                    q["text"],
                    question_type=q.get("type", "TEXT"),
                    is_required=int(q.get("required", 0)),
                )
                for choice in q.get("choices", []) or []:
                    tpl.add_choice(qid, choice)

    for tmpl in templates_cfg:
        template_name = tmpl["name"]
        template_id = _find_template_by_name(template_name, project_id=project_id)
        if not template_id:
            template_id = tpl.create_template(
                name=template_name,
                description=tmpl["description"],
                is_active=1,
                require_enumerator_code=0,
                enable_gps=0,
                enable_coverage=1,
                coverage_scheme_id=scheme_id,
                project_id=project_id,
                created_by="Playbook",
                source="playbook",
                assignment_mode="INHERIT",
                template_version="v1",
                enable_consent=int(tmpl.get("enable_consent", 0)),
                enable_attestation=int(tmpl.get("enable_attestation", 0)),
                is_sensitive=int(tmpl.get("is_sensitive", 0)),
                restricted_exports=int(tmpl.get("restricted_exports", 0)),
                redacted_fields=(tmpl.get("redacted_fields") or "").strip() or None,
            )
        else:
            tpl.set_template_config(
                template_id,
                enable_coverage=1,
                coverage_scheme_id=scheme_id,
                enable_consent=int(tmpl.get("enable_consent", 0)),
                enable_attestation=int(tmpl.get("enable_attestation", 0)),
                is_sensitive=int(tmpl.get("is_sensitive", 0)),
                restricted_exports=int(tmpl.get("restricted_exports", 0)),
                redacted_fields=(tmpl.get("redacted_fields") or "").strip() or None,
            )
        seed_questions(template_id, tmpl.get("sections", []))
        template_ids.append(template_id)

    return {
        "project_id": project_id,
        "template_ids": template_ids,
        "coverage_scheme_id": scheme_id,
    }


def row_get(row, key, default=None):
    try:
        return row[key]
    except Exception:
        return default


def row_to_dict(row):
    if row is None:
        return None
    if isinstance(row, dict):
        return row
    try:
        return dict(row)
    except Exception:
        return row


def ui_shell(
    title: str,
    inner_html: str,
    show_project_switcher: bool = False,
    show_nav: bool = True,
    nav_variant: str = "full",
):
    """
    Wrap supervisor pages with consistent UI (Poppins, spacing, lilac).
    Uses localStorage for light/dark, similar to landing.html.
    """
    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    current_project_id = None
    try:
        if request.view_args and "project_id" in request.view_args:
            current_project_id = int(request.view_args.get("project_id"))
        elif request.args.get("project_id"):
            current_project_id = int(request.args.get("project_id"))
    except Exception:
        current_project_id = None

    project_options = ""
    is_admin = bool(ADMIN_KEY and request.args.get("key") == ADMIN_KEY)
    org_id = current_org_id() if (REQUIRE_SUPERVISOR_KEY and not is_admin) else None
    user = getattr(g, "user", None)
    show_team_link = True if user else False
    show_logout_link = True if user else False
    user_name = (user.get("full_name") or "").strip() if user else ""
    if not user_name:
        user_name = (session.get("user_name") or "").strip()
    user_email = (user.get("email") or "").strip().lower() if user else ""
    if not user_email:
        user_email = (session.get("user_email") or "").strip().lower()
    user_image = (user.get("profile_image_path") or "").strip() if user else ""
    if not user_image:
        user_image = (session.get("user_image") or "").strip()
    initials_src = user_name or user_email or "OF"
    initials = "".join([p[0] for p in initials_src.split()[:2]]).upper() or "OF"
    avatar_small = (
        f"<img src='/uploads/{html.escape(user_image)}' alt='Profile photo' />"
        if user_image
        else html.escape(initials[:2])
    )
    avatar_large = (
        f"<img src='/uploads/{html.escape(user_image)}' alt='Profile photo' />"
        if user_image
        else html.escape(initials[:2])
    )
    profile_html = ""
    if show_logout_link:
        profile_html = f"""
        <div class="profile-menu" id="profileMenuRoot">
          <button class="profile-trigger" id="profileMenuToggle" type="button" aria-haspopup="menu" aria-expanded="false">
            <span class="profile-avatar">{avatar_small}</span>
          </button>
          <div class="profile-panel" id="profileMenuPanel" role="menu" aria-label="Profile menu">
            <div class="profile-head">
              <div class="profile-avatar lg">{avatar_large}</div>
              <div>
                <div class="profile-name">{html.escape(user_name or "Workspace user")}</div>
                <div class="profile-email">{html.escape(user_email or "")}</div>
              </div>
            </div>
            <a class="profile-item" href="/ui/profile{key_q}" role="menuitem">View profile</a>
            <a class="profile-item" href="/ui/settings/security{key_q}" role="menuitem">Security settings</a>
            <a class="profile-item" href="/ui/settings/sessions{key_q}" role="menuitem">Sessions & devices</a>
            <a class="profile-item danger" href="/logout" role="menuitem">Log out</a>
          </div>
        </div>
        """
    home_href = f"/ui{key_q}" if show_logout_link else "/"
    try:
        projects = prj.list_projects(200, organization_id=org_id)
        for p in projects:
            pid = int(p.get("id"))
            status = (p.get("status") or "ACTIVE").upper()
            flags = []
            if int(p.get("is_test_project") or 0) == 1:
                flags.append("Test")
            if int(p.get("is_live_project") or 0) == 1:
                flags.append("Live")
            flag_text = f" ({', '.join(flags)})" if flags else ""
            status_text = " [Archived]" if status == "ARCHIVED" else (" [Draft]" if status == "DRAFT" else "")
            selected = "selected" if current_project_id == pid else ""
            project_options += f"<option value='{pid}' {selected}>{p.get('name')}{status_text}{flag_text}</option>"
    except Exception:
        project_options = ""

    nav_html = ""
    if show_nav:
        if nav_variant == "profile_only":
            minimal_content = profile_html or "<a class='btn btn-sm' href='/login'>Sign in</a>"
            nav_html = f"""
          <div class="nav">
            <div class="container">
              <div class="nav-inner nav-minimal">
                {minimal_content}
              </div>
            </div>
          </div>
        """
        else:
            nav_html = f"""
          <div class="nav">
            <div class="container">
              <div class="nav-inner">
                <a href="{home_href}" class="brand" aria-label="{UI_BRAND['name']} home">
                  <img src="{UI_BRAND.get('logo','/static/logos/hurkfield.jpeg')}" alt="{UI_BRAND['name']} logo" style="height:56px; width:auto; max-width:210px; object-fit:contain; border-radius:14px; display:block;" />
                </a>
                <button class="mobile-nav-toggle" id="mobileNavToggle" type="button" aria-expanded="false" aria-controls="mainNavActions">Menu</button>
                <div class="nav-actions" id="mainNavActions">
                  <a class="btn" href="{home_href}">{'Home'}</a>
                  <a class="btn" href="/ui{key_q}">{'Dashboard'}</a>
                  <a class="btn" href="/ui/projects{key_q}">{'Projects'}</a>
                  <a class="btn" href="/ui/templates{key_q}">{'Templates'}</a>
                  <div class="nav-dropdown" data-navdrop>
                    <button class="btn nav-dropbtn" type="button">Operations ▾</button>
                    <div class="nav-panel">
                      <a href="/ui/surveys{key_q}">Submissions</a>
                      <a href="/ui/qa{key_q}">QA Alerts</a>
                      <a href="/ui/exports{key_q}">Exports</a>
                      <a href="/ui/errors{key_q}">Errors</a>
                    </div>
                  </div>
                  <div class="nav-dropdown" data-navdrop>
                    <button class="btn nav-dropbtn" type="button">Insights ▾</button>
                    <div class="nav-panel">
                      <a href="/ui/analytics{key_q}">Analytics</a>
                      {f"<a href='/ui/audit{key_q}'>Audit log</a>" if show_team_link else ""}
                    </div>
                  </div>
                  <div class="nav-dropdown" data-navdrop>
                    <button class="btn nav-dropbtn" type="button">Admin ▾</button>
                    <div class="nav-panel">
                      {f"<a href='/ui/org/users{key_q}'>Team</a>" if show_team_link else ""}
                      <a href="/ui/admin{key_q}">Admin</a>
                      <a href="/ui/adoption{key_q}">Adoption</a>
                    </div>
                  </div>
                  {profile_html}
                  {f'''
                  <div class="proj-switcher">
                    <label>Project</label>
                    <select id="projectSwitcher">
                      <option value="">All</option>
                      {project_options}
                    </select>
                  </div>
                  ''' if show_project_switcher else ""}
                  <span class="env-badge {'env-live' if APP_ENV in ('production','live') else ('env-pilot' if APP_ENV == 'pilot' else 'env-dev')}">
                    {APP_ENV.upper()}
                  </span>
                  <button class="toggle" id="themeToggle" title="Toggle dark mode">
                    <svg viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                      <circle cx="12" cy="12" r="5"></circle>
                      <line x1="12" y1="1" x2="12" y2="3"></line>
                      <line x1="12" y1="21" x2="12" y2="23"></line>
                      <line x1="4.22" y1="4.22" x2="5.64" y2="5.64"></line>
                      <line x1="18.36" y1="18.36" x2="19.78" y2="19.78"></line>
                      <line x1="1" y1="12" x2="3" y2="12"></line>
                      <line x1="21" y1="12" x2="23" y2="12"></line>
                      <line x1="4.22" y1="19.78" x2="5.64" y2="18.36"></line>
                      <line x1="18.36" y1="5.64" x2="19.78" y2="4.22"></line>
                    </svg>
                  </button>
                </div>
              </div>
            </div>
          </div>
        """

    return render_template_string(
        f"""
        <!doctype html>
        <html lang="en" data-theme="light">
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width,initial-scale=1" />
          <title>{title} — {UI_BRAND["name"]}</title>
          <link rel="icon" href="/static/favicon-32.png" type="image/png" />
          <link rel="apple-touch-icon" href="/static/apple-touch-icon.png" />

          <link rel="preconnect" href="https://fonts.googleapis.com">
          <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
          <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Poppins:wght@400;500;600;700;800&display=swap" rel="stylesheet">
          <script>
            tailwind.config = {{
              corePlugins: {{ preflight: false }},
              darkMode: "class",
              theme: {{
                extend: {{
                  colors: {{
                    brand: "{UI_BRAND["primary"]}",
                    primary: "{UI_BRAND["primary"]}",
                    primarySoft: "#8A84FF",
                    primaryLight: "#EDEBFF",
                    ink: "#0F172A"
                  }},
                  fontFamily: {{
                    heading: ["Poppins", "sans-serif"],
                    body: ["Inter", "sans-serif"]
                  }},
                  borderRadius: {{
                    xl: "1rem",
                    "2xl": "1.125rem"
                  }}
                }}
              }}
            }}
          </script>
          <script src="https://cdn.tailwindcss.com"></script>

          <style>
            :root{{
              --font-heading:"Poppins", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
              --font-body:"Inter", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
              --text-xs:0.75rem;
              --text-sm:0.875rem;
              --text-md:1rem;
              --text-lg:1.125rem;
              --text-xl:1.25rem;
              --h4:1.5rem;
              --h3:1.875rem;
              --h2:2.25rem;
              --h1:3rem;
              --lh-tight:1.1;
              --lh-snug:1.25;
              --lh-normal:1.5;
              --lh-relaxed:1.6;
              --w-regular:400;
              --w-medium:500;
              --w-semibold:600;
              --w-bold:700;
              --ls-tight:-0.01em;
              --ls-wide:0.01em;
              --primary-600:{UI_BRAND["primary"]};
              --primary-500:#8B5CF6;
              --primary-100:#EDE9FE;
              --neutral-950:#0B1220;
              --neutral-500:#6B7280;
              --neutral-200:#E5E7EB;
              --neutral-50:#F9FAFB;
              --success:#16A34A;
              --warning:#F59E0B;
              --danger:#DC2626;
              --info:#2563EB;
              --primary:var(--primary-600);
              --primary-soft:rgba(124,58,237,.12);
              --bg:var(--neutral-50);
              --surface:#ffffff;
              --surface-2:#F3F4F6;
              --text:var(--neutral-950);
              --muted:var(--neutral-500);
              --border:var(--neutral-200);
              --shadow:0 16px 40px rgba(15,18,34,.08);
              --radius:18px;
              --max:1120px;
            }}
            html[data-theme="dark"]{{
              --bg:#070A12;
              --surface:#0B1220;
              --surface-2:#111827;
              --text:#E5E7EB;
              --muted:#94A3B8;
              --border:#1F2937;
              --primary:var(--primary-500);
              --primary-soft:rgba(139,92,246,.18);
              --shadow:0 18px 48px rgba(0,0,0,.45);
            }}
            *{{box-sizing:border-box}}
            body{{
              margin:0;
              font-family:var(--font-body);
              font-size:var(--text-md);
              background:radial-gradient(900px 380px at 20% -10%, var(--primary-soft), transparent 60%), var(--bg);
              color:var(--text);
              line-height:var(--lh-relaxed);
            }}
            a{{text-decoration:none;color:inherit}}
            .container{{max-width:var(--max); margin:0 auto; padding:0 20px}}
            h1,h2,h3,h4,h5,h6{{font-family:var(--font-heading); letter-spacing:var(--ls-tight);}}
            table th{{font-family:var(--font-heading); letter-spacing:.2px;}}
            .card h2,.card h3,.card h4{{font-family:var(--font-heading);}}
            .section-title,.panel-title{{font-family:var(--font-heading);}}
            .nav{{
              position:sticky; top:0; z-index:50;
              background:rgba(221,212,248,.92);
              backdrop-filter:blur(12px);
              border-bottom:1px solid var(--border);
            }}
            html[data-theme="dark"] .nav{{background:rgba(221,212,248,.9)}}
            .nav-inner{{display:grid; grid-template-columns:auto 1fr auto; align-items:center; gap:16px; padding:18px 0}}
            .nav-inner.nav-minimal{{display:flex; justify-content:flex-end}}
            .brand{{display:flex; align-items:center; cursor:pointer; transition:all 0.3s ease; text-decoration:none; padding:8px 0; margin-right:16px; position:relative}}
            .brand::after{{content:""; position:absolute; right:-8px; top:50%; transform:translateY(-50%); width:1px; height:24px; background:var(--border); opacity:.7}}
            .brand:hover{{opacity:0.8; transform:scale(1.05)}}
            .brand img{{height:36px; width:auto; border-radius:10px; display:block}}
            .nav-actions{{display:flex; gap:12px; align-items:center; flex-wrap:wrap; justify-content:center; width:100%}}
            .nav-actions .btn{{font-family:var(--font-heading); padding:8px 12px; border-radius:10px; border:1px solid #D1D5DB; background:#FFFFFF; color:#475569; font-weight:600; cursor:pointer; display:inline-flex; align-items:center; font-size:12px; transition:all 0.3s ease}}
            .nav-actions .btn:hover{{color:var(--primary); border-color:var(--primary); box-shadow:0 4px 12px rgba(124,58,237,.15); background:#FFFFFF}}
            html[data-theme="dark"] .nav-actions .btn{{background:#FFFFFF; color:#475569; border-color:#D1D5DB}}
            .proj-switcher{{display:flex; align-items:center; gap:8px; padding:6px 10px; border-radius:12px; border:1px solid var(--border); background:var(--surface)}}
            .proj-switcher label{{font-size:11px; color:var(--muted); font-weight:700}}
            .proj-switcher select{{border:none; padding:6px 8px; font-size:12px; background:transparent; color:var(--text)}}
            .env-badge{{display:inline-flex; align-items:center; gap:6px; padding:6px 10px; border-radius:999px; font-size:11px; font-weight:800; border:1px solid var(--border); background:var(--surface-2)}}
            .env-dev{{color:#b45309; border-color:rgba(245,158,11,.35); background:rgba(245,158,11,.12)}}
            .env-pilot{{color:#1d4ed8; border-color:rgba(59,130,246,.35); background:rgba(59,130,246,.12)}}
            .env-live{{color:#0f766e; border-color:rgba(13,148,136,.35); background:rgba(13,148,136,.12)}}
            .toggle{{font-family:var(--font-heading); padding:8px 14px; border-radius:999px; border:1px solid var(--border); background:var(--surface); font-weight:600; cursor:pointer; display:flex; align-items:center; gap:8px; min-width:44px; justify-content:center; transition:all 0.3s ease}}
            .toggle:hover{{border-color:var(--primary); box-shadow:0 4px 12px rgba(124,58,237,.2)}}
            .toggle svg{{width:18px; height:18px; stroke:var(--text)}}
            .nav-dropdown{{position:relative}}
            .nav-dropbtn{{display:inline-flex; align-items:center; gap:6px}}
            .nav-panel{{position:absolute; top:42px; left:0; min-width:190px; padding:6px; border:1px solid var(--border); background:var(--surface); border-radius:14px; box-shadow:var(--shadow); display:none; z-index:75}}
            .nav-dropdown.open .nav-panel{{display:block}}
            .mobile-nav-toggle{{display:none; align-items:center; justify-content:center; gap:6px; min-width:42px; height:40px; border-radius:12px; border:1px solid var(--border); background:var(--surface); color:var(--text); font-weight:700; padding:0 12px; cursor:pointer}}
            .mobile-nav-toggle:hover{{border-color:var(--primary); color:var(--primary)}}
            .nav-panel a{{display:block; padding:9px 10px; border-radius:10px; font-weight:600; font-size:12px; color:var(--text)}}
            .nav-panel a:hover{{background:var(--surface-2); color:var(--primary)}}
            .profile-menu{{position:relative; margin-left:0}}
            .profile-trigger{{display:flex; align-items:center; gap:8px; border:1px solid var(--border); background:var(--surface); padding:6px; border-radius:999px; cursor:pointer; min-width:auto}}
            .profile-trigger:hover{{border-color:var(--primary); box-shadow:0 6px 16px rgba(124,58,237,.15)}}
            .profile-avatar{{width:28px; height:28px; border-radius:999px; display:grid; place-items:center; font-weight:800; font-size:12px; color:#fff; background:linear-gradient(135deg, var(--primary), var(--primary-500)); box-shadow:0 6px 14px rgba(124,58,237,.35)}}
            .profile-avatar.lg{{width:42px; height:42px; font-size:14px}}
            .profile-avatar img{{width:100%; height:100%; object-fit:cover; border-radius:inherit; display:block}}
            .profile-name{{font-weight:700; font-size:12px}}
            .profile-email{{font-size:11px; color:var(--muted)}}
            .profile-panel{{position:absolute; right:0; top:46px; width:240px; border:1px solid var(--border); background:var(--surface); border-radius:14px; box-shadow:var(--shadow); padding:10px; display:none; z-index:80}}
            .profile-menu.open .profile-panel{{display:block}}
            .profile-head{{display:flex; gap:10px; align-items:center; padding:6px 6px 10px; border-bottom:1px solid var(--border); margin-bottom:6px}}
            .profile-item{{display:block; padding:9px 10px; border-radius:10px; font-weight:600; font-size:13px; color:var(--text)}}
            .profile-item:hover{{background:var(--surface-2); color:var(--primary)}}
            .profile-item.danger{{color:#b91c1c}}
            .profile-item.danger:hover{{background:rgba(239,68,68,.12); color:#991b1b}}
            html[data-theme="dark"] .profile-trigger{{background:var(--surface);}}
            html[data-theme="dark"] .profile-panel{{box-shadow:0 18px 48px rgba(0,0,0,.55)}}
            .btn{{font-family:var(--font-heading); padding:12px 18px; border-radius:14px; border:1px solid var(--border); background:var(--surface); font-weight:var(--w-medium); letter-spacing:var(--ls-wide); cursor:pointer; display:inline-block; transition:all 0.3s ease}}
            .btn:hover{{color:var(--primary); border-color:var(--primary); box-shadow:0 4px 12px rgba(124,58,237,.15)}}
            .btn-sm{{padding:8px 12px; font-size:12px; border-radius:10px;}}
            .btn-primary{{background:linear-gradient(135deg, var(--primary), var(--primary-500)); color:#fff; border:none; box-shadow:0 12px 30px rgba(124,58,237,.35)}}
            .btn-primary:hover{{box-shadow:0 16px 40px rgba(124,58,237,.45); transform:translateY(-2px)}}
            .card{{border:1px solid var(--border); background:var(--surface); box-shadow:var(--shadow); border-radius:var(--radius); padding:20px}}
            .stack{{display:grid; gap:16px}}
            .row{{display:flex; gap:12px; flex-wrap:wrap; align-items:center}}
            html[data-theme="dark"] .card{{
              position:relative;
              overflow:hidden;
            }}
            html[data-theme="dark"] .card::before{{
              content:"";
              position:absolute;
              inset:-2px;
              border-radius:inherit;
              background:conic-gradient(from 0deg, rgba(124,58,237,.0), rgba(124,58,237,.35), rgba(16,185,129,.25), rgba(124,58,237,.35), rgba(124,58,237,.0));
              filter:blur(8px);
              opacity:.45;
              animation:glow-spin 10s linear infinite;
              z-index:0;
              pointer-events:none;
            }}
            html[data-theme="dark"] .card > *{{position:relative; z-index:1}}
            @keyframes glow-spin{{from{{transform:rotate(0deg)}} to{{transform:rotate(360deg)}}}}
            .muted{{color:var(--muted)}}
            .h1{{font-family:var(--font-heading); font-size:var(--h2); line-height:var(--lh-snug); margin:0; letter-spacing:var(--ls-tight)}}
            .h2{{font-family:var(--font-heading); font-size:var(--h4); line-height:1.35; margin:0; letter-spacing:var(--ls-tight)}}
            .table{{width:100%; border-collapse:collapse}}
            .table th,.table td{{padding:12px; border-bottom:1px solid var(--border); vertical-align:top; text-align:left}}
            input[type="text"],
            input[type="email"],
            input[type="password"],
            input[type="number"],
            input[type="tel"],
            input[type="url"],
            input[type="search"],
            input[type="date"],
            input[type="time"],
            input[type="datetime-local"],
            input[type="month"],
            input[type="week"],
            textarea,
            select{{
              width:100%;
              padding:12px 14px;
              border-radius:14px;
              border:1px solid rgba(124,58,237,.22);
              background:linear-gradient(180deg, #ffffff 0%, #f8f8fc 100%);
              color:var(--text);
              box-shadow: inset 0 1px 0 rgba(255,255,255,.85);
              transition:border-color .18s ease, box-shadow .18s ease, background .18s ease;
            }}
            html[data-theme="dark"] input[type="text"],
            html[data-theme="dark"] input[type="email"],
            html[data-theme="dark"] input[type="password"],
            html[data-theme="dark"] input[type="number"],
            html[data-theme="dark"] input[type="tel"],
            html[data-theme="dark"] input[type="url"],
            html[data-theme="dark"] input[type="search"],
            html[data-theme="dark"] input[type="date"],
            html[data-theme="dark"] input[type="time"],
            html[data-theme="dark"] input[type="datetime-local"],
            html[data-theme="dark"] input[type="month"],
            html[data-theme="dark"] input[type="week"],
            html[data-theme="dark"] textarea,
            html[data-theme="dark"] select{{
              background:linear-gradient(180deg, rgba(30,41,59,.9) 0%, rgba(17,24,39,.92) 100%);
              border-color:rgba(167,139,250,.28);
              color:#e5e7eb;
              box-shadow: inset 0 1px 0 rgba(255,255,255,.02);
            }}
            input[type="text"]::placeholder,
            input[type="email"]::placeholder,
            input[type="password"]::placeholder,
            input[type="number"]::placeholder,
            input[type="tel"]::placeholder,
            input[type="url"]::placeholder,
            input[type="search"]::placeholder,
            textarea::placeholder{{
              color:#97a3b6;
            }}
            input[type="text"]:focus,
            input[type="email"]:focus,
            input[type="password"]:focus,
            input[type="number"]:focus,
            input[type="tel"]:focus,
            input[type="url"]:focus,
            input[type="search"]:focus,
            input[type="date"]:focus,
            input[type="time"]:focus,
            input[type="datetime-local"]:focus,
            input[type="month"]:focus,
            input[type="week"]:focus,
            textarea:focus,
            select:focus{{
              outline:none;
              border-color:rgba(124,58,237,.72);
              box-shadow:0 0 0 4px rgba(124,58,237,.14);
            }}
            input[type="file"]{{
              width:100%;
              border:1px solid rgba(124,58,237,.2);
              border-radius:12px;
              background:var(--surface-2);
              color:var(--text);
              padding:10px 12px;
            }}
            textarea{{min-height:90px}}
            @media (max-width: 1100px){{
              .nav-inner{{display:grid; grid-template-columns:1fr auto; gap:12px}}
              .brand::after{{display:none}}
              .mobile-nav-toggle{{display:inline-flex}}
              .nav-actions{{
                grid-column:1/-1;
                display:none;
                flex-direction:column;
                align-items:stretch;
                width:100%;
                gap:10px;
                padding-top:8px;
              }}
              .nav-actions.open{{display:flex}}
              .nav-actions > .btn,
              .nav-actions > .nav-dropdown,
              .nav-actions > .proj-switcher,
              .nav-actions > .env-badge,
              .nav-actions > .toggle,
              .nav-actions > .profile-menu{{width:100%}}
              .nav-actions .btn{{width:100%; justify-content:center; padding:10px 12px; font-size:13px}}
              .nav-dropdown .nav-dropbtn{{width:100%; justify-content:center}}
              .nav-dropdown.open .nav-panel{{position:static; margin-top:6px; width:100%}}
              .nav-panel a{{padding:10px}}
              .proj-switcher{{justify-content:space-between}}
              .profile-menu{{display:block}}
              .profile-trigger{{width:100%; justify-content:center; border-radius:12px; padding:8px 10px}}
              .profile-panel{{position:static; width:100%; margin-top:8px}}
              .h1{{font-size:28px}}
            }}
            @media (max-width: 700px){{
              .container{{padding:0 16px}}
              .nav{{position:sticky}}
              .nav-inner{{padding:14px 0}}
              .brand img{{height:46px; max-width:180px}}
              .mobile-nav-toggle{{height:38px; padding:0 10px; font-size:12px}}
              .table thead{{display:none}}
              .table,
              .table tbody,
              .table tr,
              .table td{{display:block; width:100%}}
              .table tr{{
                border:1px solid var(--border);
                border-radius:12px;
                padding:8px 10px;
                margin-bottom:10px;
                background:var(--surface);
              }}
              .table td{{
                display:flex;
                align-items:flex-start;
                justify-content:space-between;
                gap:10px;
                padding:8px 0;
                border-bottom:1px dashed var(--border);
                white-space:normal;
                word-break:break-word;
              }}
              .table td:last-child{{border-bottom:none}}
              .table td::before{{
                content:attr(data-label);
                font-weight:700;
                color:var(--muted);
                min-width:108px;
                max-width:45%;
                font-size:12px;
                line-height:1.35;
              }}
              .table td .row,
              .table td .action-buttons,
              .table td .assign-actions{{justify-content:flex-start; width:100%}}
              .table td .btn{{font-size:12px}}
              .row{{gap:10px}}
              .card{{padding:16px}}
              input[type="text"],
              input[type="email"],
              input[type="password"],
              input[type="number"],
              input[type="tel"],
              input[type="url"],
              input[type="search"],
              input[type="date"],
              input[type="time"],
              input[type="datetime-local"],
              input[type="month"],
              input[type="week"],
              textarea,
              select{{font-size:16px}}
            }}
            .scroll-fab{{
              position:fixed;
              right:18px;
              bottom:18px;
              width:44px;
              height:44px;
              border-radius:999px;
              border:1px solid var(--border);
              background:var(--surface);
              box-shadow:0 12px 28px rgba(15,18,34,.15);
              display:flex;
              align-items:center;
              justify-content:center;
              cursor:pointer;
              z-index:400;
              transition:transform .15s ease, opacity .15s ease;
              opacity:.85;
            }}
            .scroll-fab:hover{{transform:translateY(-2px); opacity:1;}}
            .scroll-fab span{{font-size:18px; font-weight:800; color:var(--text);}}
            @media (max-width: 700px){{
              .scroll-fab{{right:12px; bottom:12px;}}
            }}
          </style>
        </head>
        <body>
          {nav_html}

          <div class="container" style="padding:28px 20px 72px;">
            {inner_html}
            <div class="muted" style="margin-top:26px; font-size:12px; text-align:center">{APP_VERSION}</div>
          </div>
          <button class="scroll-fab" id="scrollFab" title="Scroll">
            <span id="scrollFabIcon">↓</span>
          </button>

          <script>
            const root = document.documentElement;
            const toggle = document.getElementById("themeToggle");
            const mobileNavToggle = document.getElementById("mobileNavToggle");
            const mainNavActions = document.getElementById("mainNavActions");
            const switcher = document.getElementById("projectSwitcher");
            const profileRoot = document.getElementById("profileMenuRoot");
            const profileToggle = document.getElementById("profileMenuToggle");
            const navDrops = Array.from(document.querySelectorAll("[data-navdrop]"));

            function setTheme(t){{
              root.setAttribute("data-theme", t);
              localStorage.setItem("openfield_theme", t);
            }}
            const saved = localStorage.getItem("openfield_theme");
            if(saved) setTheme(saved);

            if(toggle){{
              toggle.onclick = () => {{
                setTheme(root.getAttribute("data-theme")==="dark" ? "light" : "dark");
              }};
            }}

            function setMobileNav(open){{
              if(!mainNavActions || !mobileNavToggle) return;
              if(open){{
                mainNavActions.classList.add("open");
                mobileNavToggle.setAttribute("aria-expanded", "true");
                mobileNavToggle.textContent = "Close";
              }} else {{
                mainNavActions.classList.remove("open");
                mobileNavToggle.setAttribute("aria-expanded", "false");
                mobileNavToggle.textContent = "Menu";
              }}
            }}
            if(mobileNavToggle && mainNavActions){{
              mobileNavToggle.addEventListener("click", (e)=>{{
                e.stopPropagation();
                const willOpen = !mainNavActions.classList.contains("open");
                setMobileNav(willOpen);
              }});
              mainNavActions.querySelectorAll("a").forEach((el)=>{{
                el.addEventListener("click", ()=>{{
                  if(window.innerWidth <= 1100) setMobileNav(false);
                }});
              }});
              document.addEventListener("click", (e)=>{{
                if(window.innerWidth > 1100) return;
                if(!mainNavActions.contains(e.target) && !mobileNavToggle.contains(e.target)){{
                  setMobileNav(false);
                }}
              }});
              window.addEventListener("resize", ()=>{{
                if(window.innerWidth > 1100) setMobileNav(false);
              }});
            }}
            if(profileRoot && profileToggle){{
              profileToggle.addEventListener("click", (e)=>{{
                e.stopPropagation();
                navDrops.forEach((d)=>d.classList.remove("open"));
                const open = profileRoot.classList.toggle("open");
                profileToggle.setAttribute("aria-expanded", open ? "true" : "false");
              }});
              document.addEventListener("click", (e)=>{{
                if(!profileRoot.contains(e.target)) {{
                  profileRoot.classList.remove("open");
                  profileToggle.setAttribute("aria-expanded", "false");
                }}
              }});
              document.addEventListener("keydown", (e)=>{{
                if(e.key === "Escape") {{
                  profileRoot.classList.remove("open");
                  profileToggle.setAttribute("aria-expanded", "false");
                }}
              }});
            }}
            if(navDrops.length){{
              navDrops.forEach((drop)=>{{
                const btn = drop.querySelector("button");
                if(!btn) return;
                btn.addEventListener("click", (e)=>{{
                  e.stopPropagation();
                  const willOpen = !drop.classList.contains("open");
                  navDrops.forEach((d)=>d.classList.remove("open"));
                  if(profileRoot) {{
                    profileRoot.classList.remove("open");
                    if(profileToggle) profileToggle.setAttribute("aria-expanded", "false");
                  }}
                  if(willOpen) drop.classList.add("open");
                }});
              }});
              document.addEventListener("click", ()=>navDrops.forEach((d)=>d.classList.remove("open")));
              document.addEventListener("keydown", (e)=>{{
                if(e.key === "Escape") navDrops.forEach((d)=>d.classList.remove("open"));
              }});
            }}
            if(switcher){{
              switcher.addEventListener("change", (e)=>{{
                const val = e.target.value;
                if(!val) {{
                  window.location.href = "/ui{key_q}";
                }} else {{
                  window.location.href = "/ui/projects/" + val + "{key_q}";
                }}
              }});
            }}

            async function copyText(text){{
              try{{
                if(navigator.clipboard && window.isSecureContext){{
                  await navigator.clipboard.writeText(text);
                  return true;
                }}
              }}catch(e){{}}

              try{{
                const ta = document.createElement("textarea");
                ta.value = text;
                ta.style.position = "fixed";
                ta.style.left = "-9999px";
                ta.style.top = "-9999px";
                document.body.appendChild(ta);
                ta.focus();
                ta.select();
                const ok = document.execCommand("copy");
                document.body.removeChild(ta);
                return ok;
              }}catch(e){{
                return false;
              }}
            }}

            document.addEventListener("click", async (e)=>{{
              const btn = e.target.closest("[data-copy]");
              if(!btn) return;

              const text = btn.getAttribute("data-copy") || "";
              const ok = await copyText(text);

              const original = btn.innerText;
              btn.innerText = ok ? "Copied" : "Copy failed";
              setTimeout(()=>{{ btn.innerText = original; }}, 1200);
            }});

            (function(){{
              const tables = Array.from(document.querySelectorAll(".table"));
              tables.forEach((table)=>{{
                const headers = Array.from(table.querySelectorAll("thead th")).map((th)=>((th.innerText || "").trim()));
                const bodyRows = Array.from(table.querySelectorAll("tbody tr"));
                bodyRows.forEach((row)=>{{
                  const cells = Array.from(row.children).filter((el)=>el.tagName === "TD");
                  cells.forEach((cell, idx)=>{{
                    if(cell.hasAttribute("data-label")) return;
                    const label = headers[idx] || `Field ${{idx + 1}}`;
                    cell.setAttribute("data-label", label);
                  }});
                }});
              }});
            }})();

            (function(){{
              const fab = document.getElementById("scrollFab");
              const icon = document.getElementById("scrollFabIcon");
              if(!fab || !icon) return;
              function atBottom(){{
                return (window.innerHeight + window.scrollY) >= (document.body.scrollHeight - 40);
              }}
              function update(){{
                if(window.scrollY < 40){{
                  fab.dataset.dir = "down";
                  icon.textContent = "↓";
                }} else if(atBottom()) {{
                  fab.dataset.dir = "up";
                  icon.textContent = "↑";
                }} else {{
                  fab.dataset.dir = "up";
                  icon.textContent = "↑";
                }}
              }}
              update();
              window.addEventListener("scroll", update, {{passive:true}});
              fab.addEventListener("click", ()=>{{
                const dir = fab.dataset.dir || "up";
                if(dir === "down"){{
                  window.scrollTo({{ top: document.body.scrollHeight, behavior: "smooth" }});
                }} else {{
                  window.scrollTo({{ top: 0, behavior: "smooth" }});
                }}
              }});
            }})();
          </script>
        </body>
        </html>
        """
    )

# ---------------------------
# Schema helpers (auto-detect)
# ---------------------------


def _table_cols(table: str) -> set:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        cols = []
        for r in cur.fetchall():
            try:
                cols.append(r["name"])
            except Exception:
                cols.append(r[1])
        return set(cols)


_SURVEYS_COLS = None
_ANSWERS_COLS = None
_TEMPLATES_COLS = None
_FACILITIES_COLS = None
_TQ_COLS = None
_TQC_COLS = None


def surveys_cols():
    global _SURVEYS_COLS
    if _SURVEYS_COLS is None:
        _SURVEYS_COLS = _table_cols("surveys")
    return _SURVEYS_COLS


def answers_cols():
    global _ANSWERS_COLS
    if _ANSWERS_COLS is None:
        _ANSWERS_COLS = _table_cols("survey_answers")
    return _ANSWERS_COLS


def templates_cols():
    global _TEMPLATES_COLS
    if _TEMPLATES_COLS is None:
        _TEMPLATES_COLS = _table_cols("survey_templates")
    return _TEMPLATES_COLS


def _enumerator_is_active(enumerator: dict | None) -> bool:
    if not enumerator:
        return True
    status = (enumerator.get("status") or "").strip().upper()
    if status and status != "ACTIVE":
        return False
    if "is_active" in enumerator and enumerator.get("is_active") is not None:
        try:
            return int(enumerator.get("is_active") or 0) == 1
        except Exception:
            return False
    return True


def _assignment_is_active(assignment: dict | None) -> bool:
    if not assignment:
        return True
    if "is_active" in assignment and assignment.get("is_active") is not None:
        try:
            return int(assignment.get("is_active") or 0) == 1
        except Exception:
            return False
    return True


def facilities_cols():
    global _FACILITIES_COLS
    if _FACILITIES_COLS is None:
        _FACILITIES_COLS = _table_cols("facilities")
    return _FACILITIES_COLS


def template_questions_cols():
    global _TQ_COLS
    if _TQ_COLS is None:
        _TQ_COLS = _table_cols("template_questions")
    return _TQ_COLS


def template_choices_cols():
    global _TQC_COLS
    if _TQC_COLS is None:
        _TQC_COLS = _table_cols("template_question_choices")
    return _TQC_COLS


# ---------------------------
# Helpers
# ---------------------------
def ensure_drafts_table():
    if not ENABLE_SERVER_DRAFTS:
        return
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {DRAFTS_TABLE} (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              template_id INTEGER,
              token TEXT NOT NULL,
              draft_key TEXT NOT NULL,
              data_json TEXT NOT NULL,
              filled_count INTEGER DEFAULT 0,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{DRAFTS_TABLE}_token ON {DRAFTS_TABLE}(token)"
        )
        cur.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{DRAFTS_TABLE}_key ON {DRAFTS_TABLE}(draft_key)"
        )
        conn.commit()


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def _resolve_uploaded_audio_path(audio_file_url: str) -> str:
    raw = (audio_file_url or "").strip()
    if not raw:
        raise ValueError("No audio file attached.")
    if raw.startswith("/uploads/"):
        name = secure_filename(os.path.basename(raw))
        path = os.path.join(UPLOAD_DIR, name)
    else:
        name = secure_filename(os.path.basename(raw))
        path = os.path.join(UPLOAD_DIR, name)
    if not os.path.isfile(path):
        raise ValueError("Audio file not found on server.")
    return path


def _guess_audio_mime(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    return {
        ".webm": "audio/webm",
        ".wav": "audio/wav",
        ".flac": "audio/flac",
        ".aac": "audio/aac",
        ".aiff": "audio/aiff",
        ".aif": "audio/aiff",
        ".mp3": "audio/mpeg",
        ".m4a": "audio/mp4",
        ".mp4": "audio/mp4",
        ".ogg": "audio/ogg",
        ".oga": "audio/ogg",
        ".opus": "audio/ogg",
        ".amr": "audio/amr",
        ".3gp": "audio/3gpp",
        ".wma": "audio/x-ms-wma",
        ".mov": "audio/quicktime",
    }.get(ext, "application/octet-stream")


def _guess_audio_ext_from_mime(mime: str) -> str:
    m = (mime or "").strip().lower()
    return {
        "audio/webm": ".webm",
        "audio/wav": ".wav",
        "audio/x-wav": ".wav",
        "audio/mpeg": ".mp3",
        "audio/mp3": ".mp3",
        "audio/mp4": ".m4a",
        "audio/aac": ".aac",
        "audio/flac": ".flac",
        "audio/ogg": ".ogg",
        "audio/opus": ".opus",
        "audio/amr": ".amr",
        "audio/3gpp": ".3gp",
        "audio/quicktime": ".mov",
    }.get(m, "")


def _transcribe_audio_openai(local_path: str) -> str:
    if not TRANSCRIBE_OPENAI_KEY:
        raise ValueError("OpenAI transcription key is not configured.")
    api_url = "https://api.openai.com/v1/audio/transcriptions"
    data = {"model": TRANSCRIBE_MODEL or "gpt-4o-mini-transcribe"}
    if TRANSCRIBE_LANGUAGE:
        data["language"] = TRANSCRIBE_LANGUAGE
    with open(local_path, "rb") as audio_file:
        files = {
            "file": (os.path.basename(local_path), audio_file, _guess_audio_mime(local_path)),
        }
        resp = requests.post(
            api_url,
            headers={"Authorization": f"Bearer {TRANSCRIBE_OPENAI_KEY}"},
            data=data,
            files=files,
            timeout=max(10, int(TRANSCRIBE_TIMEOUT or 120)),
        )
    if resp.status_code >= 400:
        detail = ""
        try:
            payload = resp.json()
            detail = payload.get("error", {}).get("message") or ""
        except Exception:
            detail = (resp.text or "").strip()
        raise ValueError(f"Transcription failed ({resp.status_code}). {detail}".strip())
    try:
        payload = resp.json()
    except Exception:
        payload = {}
    text = (payload.get("text") or "").strip() if isinstance(payload, dict) else ""
    if not text:
        raise ValueError("Transcription returned no text.")
    return text


def _transcribe_audio_deepgram(local_path: str) -> str:
    if not TRANSCRIBE_DEEPGRAM_KEY:
        raise ValueError("Deepgram transcription key is not configured.")
    if TRANSCRIBE_DEEPGRAM_KEY.startswith("sk-"):
        raise ValueError("Deepgram key looks invalid for this provider. Use a Deepgram API key, not an OpenAI key.")
    if re.fullmatch(r"[a-fA-F0-9]{32}", TRANSCRIBE_DEEPGRAM_KEY or ""):
        raise ValueError("Deepgram key looks like a Project ID, not an API key. Create an API key in Deepgram Console (API Keys) and use that value.")
    api_url = "https://api.deepgram.com/v1/listen"
    params = {
        "model": TRANSCRIBE_DEEPGRAM_MODEL or "nova-2",
        "smart_format": "true",
        "punctuate": "true",
    }
    if TRANSCRIBE_LANGUAGE:
        params["language"] = TRANSCRIBE_LANGUAGE
    timeout_secs = max(10, int(TRANSCRIBE_TIMEOUT or 120))
    with open(local_path, "rb") as audio_file:
        payload = audio_file.read()
    base_headers = {
        "Content-Type": _guess_audio_mime(local_path),
    }

    # Primary auth scheme for Deepgram is Token; retry with Bearer for compatibility.
    resp = requests.post(
        api_url,
        params=params,
        headers={**base_headers, "Authorization": f"Token {TRANSCRIBE_DEEPGRAM_KEY}"},
        data=payload,
        timeout=timeout_secs,
    )
    if resp.status_code == 401:
        resp = requests.post(
            api_url,
            params=params,
            headers={**base_headers, "Authorization": f"Bearer {TRANSCRIBE_DEEPGRAM_KEY}"},
            data=payload,
            timeout=timeout_secs,
        )
    if resp.status_code >= 400:
        detail = ""
        try:
            payload = resp.json()
            detail = payload.get("err_msg") or payload.get("message") or ""
        except Exception:
            detail = (resp.text or "").strip()
        if resp.status_code == 401:
            raise ValueError("Deepgram returned 401 Invalid credentials. Check HURKFIELD_TRANSCRIBE_DEEPGRAM_KEY / OPENFIELD_TRANSCRIBE_DEEPGRAM_KEY, create a fresh key in Deepgram Console, then restart the server.")
        raise ValueError(f"Transcription failed ({resp.status_code}). {detail}".strip())
    try:
        payload = resp.json()
    except Exception:
        payload = {}
    text = ""
    try:
        text = (
            (((payload.get("results") or {}).get("channels") or [{}])[0].get("alternatives") or [{}])[0].get("transcript")
            or ""
        ).strip()
    except Exception:
        text = ""
    if not text:
        raise ValueError("Transcription returned no text.")
    return text


def run_audio_transcription(audio_file_url: str) -> str:
    local_path = _resolve_uploaded_audio_path(audio_file_url)
    provider = (TRANSCRIBE_PROVIDER or "openai").strip().lower()
    if provider == "openai":
        return _transcribe_audio_openai(local_path)
    if provider == "deepgram":
        return _transcribe_audio_deepgram(local_path)
    raise ValueError(f"Unsupported transcription provider: {provider}")


def transcription_config_status() -> tuple[bool, str]:
    provider = (TRANSCRIBE_PROVIDER or "openai").strip().lower()
    if provider == "openai":
        if TRANSCRIBE_OPENAI_KEY:
            return True, ""
        return False, "Transcription not configured. Set OPENFIELD_TRANSCRIBE_OPENAI_KEY (or HURKFIELD_TRANSCRIBE_OPENAI_KEY) and restart."
    if provider == "deepgram":
        if TRANSCRIBE_DEEPGRAM_KEY:
            return True, ""
        return False, "Transcription not configured. Set OPENFIELD_TRANSCRIBE_DEEPGRAM_KEY (or HURKFIELD_TRANSCRIBE_DEEPGRAM_KEY) and restart."
    return False, f"Unsupported transcription provider: {provider}"


def _parse_validation_json(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _validate_answer(qtype: str, answer: str, validation: dict, choices: list | None = None) -> str | None:
    if answer is None:
        return None
    qtype = (qtype or "TEXT").upper()
    text = str(answer).strip()
    if not text:
        return None

    if qtype in ("TEXT", "LONGTEXT", "EMAIL", "PHONE"):
        min_len = validation.get("min_length")
        max_len = validation.get("max_length")
        pattern = validation.get("pattern")
        if isinstance(min_len, int) and len(text) < min_len:
            return f"Minimum length is {min_len}."
        if isinstance(max_len, int) and len(text) > max_len:
            return f"Maximum length is {max_len}."
        if pattern:
            try:
                if not re.fullmatch(pattern, text):
                    return "Value does not match required format."
            except Exception:
                return "Invalid validation pattern."
        if qtype == "EMAIL":
            if "@" not in text or "." not in text:
                return "Enter a valid email."
    elif qtype == "NUMBER":
        try:
            val = float(text)
        except Exception:
            return "Enter a valid number."
        min_val = validation.get("min_value")
        max_val = validation.get("max_value")
        if isinstance(min_val, (int, float)) and val < float(min_val):
            return f"Minimum value is {min_val}."
        if isinstance(max_val, (int, float)) and val > float(max_val):
            return f"Maximum value is {max_val}."
    elif qtype in ("SINGLE_CHOICE", "DROPDOWN", "MULTI_CHOICE"):
        if choices:
            allowed = {str(c).strip() for c in choices if str(c).strip()}
            if qtype == "MULTI_CHOICE":
                parts = [p.strip() for p in text.split(",") if p.strip()]
                invalid = [p for p in parts if p not in allowed]
                if invalid:
                    return "Selected option is not valid."
            else:
                if text not in allowed:
                    return "Selected option is not valid."

    return None


def _is_marker_question(text: str) -> bool:
    t = (text or "").strip()
    upper = t.upper()
    return (
        t.startswith("## ")
        or upper.startswith("[SECTION]")
        or upper.startswith("[IMAGE] ")
        or upper.startswith("[VIDEO] ")
    )


def _build_response_summary(template_id: int) -> List[dict]:
    questions = tpl.get_template_questions(int(template_id))
    summaries = []
    ans_has_qid = "template_question_id" in answers_cols()
    surveys_has_deleted = "deleted_at" in surveys_cols()

    for row in questions:
        qid = row[0]
        qtext = row[1]
        qtype = (row[2] or "TEXT").upper()
        if _is_marker_question(qtext):
            continue

        with get_conn() as conn:
            cur = conn.cursor()
            if ans_has_qid:
                sql = """
                    SELECT a.answer
                    FROM survey_answers a
                    JOIN surveys s ON s.id = a.survey_id
                    WHERE s.template_id=?
                      AND a.template_question_id=?
                """
                params = [int(template_id), int(qid)]
            else:
                sql = """
                    SELECT a.answer
                    FROM survey_answers a
                    JOIN surveys s ON s.id = a.survey_id
                    WHERE s.template_id=?
                      AND COALESCE(a.question,'')=?
                """
                params = [int(template_id), str(qtext)]
            if surveys_has_deleted:
                sql += " AND s.deleted_at IS NULL"
            cur.execute(sql, tuple(params))
            answers = [str(r["answer"] or "").strip() for r in cur.fetchall()]

        answers = [a for a in answers if a]
        total = len(answers)

        if qtype in ("SINGLE_CHOICE", "DROPDOWN", "YESNO", "MULTI_CHOICE"):
            if qtype == "YESNO":
                choices = ["YES", "NO"]
            else:
                choices = [c[2] for c in q_choices(qid)] if qtype != "MULTI_CHOICE" else [c[2] for c in q_choices(qid)]
            counts = {str(c): 0 for c in choices if str(c).strip()}
            if qtype == "MULTI_CHOICE":
                for ans in answers:
                    parts = [p.strip() for p in ans.split(",") if p.strip()]
                    seen = set()
                    for p in parts:
                        if p in seen:
                            continue
                        seen.add(p)
                        counts[p] = counts.get(p, 0) + 1
            else:
                for ans in answers:
                    key = ans.strip()
                    if qtype == "YESNO":
                        key = key.upper()
                    counts[key] = counts.get(key, 0) + 1

            items = []
            for label, count in counts.items():
                pct = round((count / total) * 100, 1) if total else 0
                items.append({"label": label, "count": count, "pct": pct})
            summaries.append(
                {
                    "id": qid,
                    "text": qtext,
                    "type": qtype,
                    "total": total,
                    "kind": "choice",
                    "items": items,
                }
            )
        elif qtype == "NUMBER":
            vals = []
            for ans in answers:
                try:
                    vals.append(float(ans))
                except Exception:
                    continue
            stats = None
            if vals:
                stats = {
                    "min": min(vals),
                    "max": max(vals),
                    "avg": round(sum(vals) / len(vals), 2),
                }
            summaries.append(
                {
                    "id": qid,
                    "text": qtext,
                    "type": qtype,
                    "total": total,
                    "kind": "number",
                    "stats": stats,
                }
            )
        else:
            samples = answers[:5]
            summaries.append(
                {
                    "id": qid,
                    "text": qtext,
                    "type": qtype,
                    "total": total,
                    "kind": "text",
                    "samples": samples,
                }
            )

    return summaries


def log_submission_error(
    template_id: Optional[int],
    project_id: Optional[int],
    survey_id: Optional[int],
    error_type: str,
    error_message: str,
    context: Optional[dict] = None,
) -> None:
    payload = json.dumps(context or {}, ensure_ascii=True)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO submission_errors
              (project_id, template_id, survey_id, error_type, error_message, context_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (int(project_id) if project_id is not None else None),
                (int(template_id) if template_id is not None else None),
                (int(survey_id) if survey_id is not None else None),
                (error_type or "system").strip().lower(),
                (error_message or "Unknown error").strip(),
                payload,
                now_iso(),
            ),
        )
        conn.commit()


def save_server_draft(token: str, template_id: Optional[int], draft_key: str, data: dict, filled_count: int) -> None:
    ensure_drafts_table()
    payload = json.dumps(data, ensure_ascii=True)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            INSERT INTO {DRAFTS_TABLE} (template_id, token, draft_key, data_json, filled_count, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(draft_key) DO UPDATE SET
              data_json=excluded.data_json,
              filled_count=excluded.filled_count,
              updated_at=excluded.updated_at
            """,
            (template_id, token, draft_key, payload, int(
                filled_count or 0), now_iso(), now_iso()),
        )
        conn.commit()


def fetch_server_draft(token: str, draft_key: str):
    ensure_drafts_table()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT * FROM {DRAFTS_TABLE} WHERE token=? AND draft_key=? LIMIT 1",
            (token, draft_key),
        )
        row = cur.fetchone()
    return row_to_dict(row)


def delete_server_draft(token: str, draft_key: str) -> None:
    ensure_drafts_table()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"DELETE FROM {DRAFTS_TABLE} WHERE token=? AND draft_key=?",
            (token, draft_key),
        )
        conn.commit()


def _supervisor_key_from_request() -> str:
    key = (request.args.get(SUPERVISOR_KEY_PARAM) or "").strip()
    if key:
        return key
    return (request.cookies.get(SUPERVISOR_KEY_COOKIE) or "").strip()


def _load_supervisor_context():
    if not REQUIRE_SUPERVISOR_KEY:
        return None
    key = _supervisor_key_from_request()
    if not key:
        return None
    sup = prj.get_supervisor_by_key(key)
    if not sup:
        return None
    status = (sup.get("status") or "ACTIVE").strip().upper()
    if status != "ACTIVE":
        return None
    return sup


@app.before_request
def _before_request_load_supervisor():
    g.supervisor = _load_supervisor_context()


def _load_user_context():
    uid = session.get("user_id")
    if not uid:
        return None
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM users WHERE id=? LIMIT 1", (int(uid),))
            row = cur.fetchone()
            if not row:
                return None
            user = dict(row)
            status = (user.get("status") or "ACTIVE").strip().upper()
            if status != "ACTIVE":
                return None
            try:
                session["user_name"] = (user.get("full_name") or "").strip()
                session["user_email"] = (user.get("email") or "").strip().lower()
                if user.get("profile_image_path"):
                    session["user_image"] = user.get("profile_image_path")
            except Exception:
                pass
            return user
    except Exception:
        return None


@app.before_request
def _before_request_load_user():
    g.user = _load_user_context()
    if getattr(g, "user", None):
        _touch_session_record()


@app.after_request
def _after_request_set_supervisor_cookie(response):
    if REQUIRE_SUPERVISOR_KEY:
        key = (request.args.get(SUPERVISOR_KEY_PARAM) or "").strip()
        if key and getattr(g, "supervisor", None):
            response.set_cookie(
                SUPERVISOR_KEY_COOKIE,
                key,
                httponly=True,
                samesite="Lax",
            )
    return response


def _password_is_valid(pw: str) -> bool:
    if not pw or len(pw) < 10:
        return False
    has_letter = any(c.isalpha() for c in pw)
    has_number = any(c.isdigit() for c in pw)
    return has_letter and has_number


def _oauth_env(key: str) -> str:
    """
    Read OAuth env values with backward-compatible key aliases.
    Supports OPENFIELD_* and HURKFIELD_* namespaces, plus common generic keys.
    """
    candidates = [key]
    if key.startswith("OPENFIELD_"):
        suffix = key[len("OPENFIELD_"):]
        candidates.append(f"HURKFIELD_{suffix}")
        # Generic fallback (e.g. GOOGLE_OAUTH_CLIENT_ID)
        candidates.append(suffix)
    for cand in candidates:
        val = (os.environ.get(cand) or "").strip()
        if val:
            return val
    return ""


def _token_hash(raw: str) -> str:
    return hashlib.sha256((raw or "").encode("utf-8")).hexdigest()


def _client_ip() -> str:
    xfwd = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
    return xfwd or (request.remote_addr or "")


def _device_label() -> str:
    ua = (request.user_agent.string or "").strip()
    if not ua:
        return "Unknown device"
    # Very lightweight device hint for the sessions page.
    return ua.split("(", 1)[0].strip()[:80] or "Browser session"


def _log_security_event(user_id: int | None, event_type: str, meta: dict | None = None) -> None:
    try:
        with get_conn() as conn:
            conn.execute(
                """
                INSERT INTO security_events (user_id, event_type, ip_address, user_agent, created_at, meta_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    int(user_id) if user_id is not None else None,
                    (event_type or "").strip().upper(),
                    _client_ip(),
                    (request.user_agent.string or "")[:200],
                    now_iso(),
                    json.dumps(meta or {}),
                ),
            )
            conn.commit()
    except Exception:
        return


def _ensure_user_security_settings(user_id: int) -> dict:
    defaults = {
        "notify_new_login": 1,
        "notify_password_change": 1,
        "notify_oauth_changes": 1,
    }
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM user_security_settings WHERE user_id=? LIMIT 1", (int(user_id),))
            row = cur.fetchone()
            if row:
                data = dict(row)
                for k, v in defaults.items():
                    data.setdefault(k, v)
                return data
            conn.execute(
                """
                INSERT INTO user_security_settings (user_id, notify_new_login, notify_password_change, notify_oauth_changes, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (int(user_id), 1, 1, 1, now_iso()),
            )
            conn.commit()
            return {"user_id": int(user_id), **defaults, "updated_at": now_iso()}
    except Exception:
        return {"user_id": int(user_id), **defaults}


def _create_session_record(user_id: int) -> None:
    try:
        raw = secrets.token_urlsafe(32)
        h = _token_hash(raw)
        with get_conn() as conn:
            conn.execute(
                """
                INSERT INTO sessions (user_id, session_token_hash, device_label, ip_address, user_agent, created_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(user_id),
                    h,
                    _device_label(),
                    _client_ip(),
                    (request.user_agent.string or "")[:200],
                    now_iso(),
                    now_iso(),
                ),
            )
            conn.commit()
        session["session_token"] = raw
    except Exception:
        return


def _touch_session_record() -> None:
    raw = (session.get("session_token") or "").strip()
    uid = session.get("user_id")
    if not raw or not uid:
        return
    try:
        h = _token_hash(raw)
        with get_conn() as conn:
            conn.execute(
                """
                UPDATE sessions
                SET last_seen_at=?
                WHERE user_id=? AND session_token_hash=? AND revoked_at IS NULL
                """,
                (now_iso(), int(uid), h),
            )
            conn.commit()
    except Exception:
        return


def _revoke_session_token(raw_token: str | None) -> None:
    token = (raw_token or "").strip()
    uid = session.get("user_id")
    if not token or not uid:
        return
    try:
        with get_conn() as conn:
            conn.execute(
                "UPDATE sessions SET revoked_at=? WHERE user_id=? AND session_token_hash=? AND revoked_at IS NULL",
                (now_iso(), int(uid), _token_hash(token)),
            )
            conn.commit()
    except Exception:
        return


def _revoke_all_sessions(user_id: int, exclude_raw_token: str | None = None) -> None:
    try:
        exclude_hash = _token_hash(exclude_raw_token or "") if exclude_raw_token else ""
        with get_conn() as conn:
            if exclude_hash:
                conn.execute(
                    """
                    UPDATE sessions
                    SET revoked_at=?
                    WHERE user_id=? AND revoked_at IS NULL AND session_token_hash<>?
                    """,
                    (now_iso(), int(user_id), exclude_hash),
                )
            else:
                conn.execute(
                    "UPDATE sessions SET revoked_at=? WHERE user_id=? AND revoked_at IS NULL",
                    (now_iso(), int(user_id)),
                )
            conn.commit()
    except Exception:
        return


def _register_oauth_providers():
    if not oauth:
        return
    # Google (OIDC)
    g_client_id = _oauth_env("OPENFIELD_GOOGLE_OAUTH_CLIENT_ID")
    g_client_secret = _oauth_env("OPENFIELD_GOOGLE_OAUTH_CLIENT_SECRET")
    if g_client_id and g_client_secret:
        oauth.register(
            name="google",
            client_id=g_client_id,
            client_secret=g_client_secret,
            server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
            client_kwargs={"scope": "openid email profile"},
        )

    # Microsoft (OIDC)
    m_client_id = _oauth_env("OPENFIELD_MICROSOFT_OAUTH_CLIENT_ID")
    m_client_secret = _oauth_env("OPENFIELD_MICROSOFT_OAUTH_CLIENT_SECRET")
    if m_client_id and m_client_secret:
        oauth.register(
            name="microsoft",
            client_id=m_client_id,
            client_secret=m_client_secret,
            server_metadata_url="https://login.microsoftonline.com/common/v2.0/.well-known/openid-configuration",
            client_kwargs={"scope": "openid email profile"},
        )

    # LinkedIn (OIDC)
    l_client_id = _oauth_env("OPENFIELD_LINKEDIN_OAUTH_CLIENT_ID")
    l_client_secret = _oauth_env("OPENFIELD_LINKEDIN_OAUTH_CLIENT_SECRET")
    if l_client_id and l_client_secret:
        oauth.register(
            name="linkedin",
            client_id=l_client_id,
            client_secret=l_client_secret,
            server_metadata_url="https://www.linkedin.com/oauth/.well-known/openid-configuration",
            client_kwargs={"scope": "openid email profile"},
        )

    # Facebook (OAuth2)
    f_client_id = _oauth_env("OPENFIELD_FACEBOOK_OAUTH_CLIENT_ID")
    f_client_secret = _oauth_env("OPENFIELD_FACEBOOK_OAUTH_CLIENT_SECRET")
    if f_client_id and f_client_secret:
        oauth.register(
            name="facebook",
            client_id=f_client_id,
            client_secret=f_client_secret,
            access_token_url="https://graph.facebook.com/v18.0/oauth/access_token",
            authorize_url="https://www.facebook.com/v18.0/dialog/oauth",
            api_base_url="https://graph.facebook.com/",
            client_kwargs={"scope": "email public_profile"},
        )


_register_oauth_providers()


def _normalize_domain_value(value: str) -> str:
    v = (value or "").strip().lower()
    if not v:
        return ""
    if "://" in v:
        v = v.split("://", 1)[1]
    if "/" in v:
        v = v.split("/", 1)[0]
    if "@" in v:
        v = v.split("@", 1)[1]
    return v.strip()


def _get_org_by_domain(domain: str):
    domain = _normalize_domain_value(domain)
    if not domain:
        return None
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM organizations WHERE LOWER(domain)=LOWER(?) LIMIT 1", (domain,))
            row = cur.fetchone()
            if row:
                return dict(row)
            cur.execute("SELECT * FROM organizations WHERE domain IS NOT NULL")
            rows = cur.fetchall()
        for r in rows:
            if _normalize_domain_value(r["domain"]) == domain:
                return dict(r)
    except Exception:
        return None


def _create_user(
    organization_id: int,
    full_name: str,
    email: str,
    password: str | None,
    role: str = "OWNER",
    title: str = "",
    phone: str = "",
    email_verified: int = 0,
    status: str = "ACTIVE",
):
    with get_conn() as conn:
        cur = conn.cursor()
        pw_hash = generate_password_hash(password) if password else None
        cur.execute(
            """
            INSERT INTO users (organization_id, full_name, email, password_hash, role, title, phone, status, email_verified, verified_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(organization_id),
                full_name.strip(),
                email.strip().lower(),
                pw_hash,
                (role or "OWNER").upper(),
                (title or "").strip(),
                (phone or "").strip() or None,
                (status or "ACTIVE").strip().upper(),
                int(email_verified),
                now_iso() if email_verified else None,
                now_iso(),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def _create_user_token(user_id: int, token_type: str, hours_valid: int = 48) -> str:
    token = secrets.token_urlsafe(32)
    expires_at = (datetime.now() + timedelta(hours=hours_valid)).isoformat(timespec="seconds")
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO user_tokens (user_id, token, token_type, expires_at, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (int(user_id), token, token_type, expires_at, now_iso()),
        )
        conn.commit()
    return token


def _get_valid_token(token: str, token_type: str):
    if not token:
        return None
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT * FROM user_tokens
                WHERE token=? AND token_type=? AND used_at IS NULL
                LIMIT 1
                """,
                (token, token_type),
            )
            row = cur.fetchone()
            if not row:
                return None
            data = dict(row)
    except Exception:
        return None

    exp = data.get("expires_at")
    try:
        if exp and datetime.fromisoformat(exp) < datetime.now():
            return None
    except Exception:
        pass
    return data


def _mark_token_used(token_id: int):
    try:
        with get_conn() as conn:
            conn.execute("UPDATE user_tokens SET used_at=? WHERE id=?", (now_iso(), int(token_id)))
            conn.commit()
    except Exception:
        pass


def _send_email(to_email: str, subject: str, body: str, html_body: str | None = None) -> bool:
    if not SMTP_HOST or not SMTP_FROM:
        return False
    try:
        import smtplib
        from email.message import EmailMessage

        msg = EmailMessage()
        msg["From"] = SMTP_FROM
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.set_content(body)
        if html_body:
            msg.add_alternative(html_body, subtype="html")

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
            if SMTP_TLS:
                server.starttls()
            if SMTP_USER:
                server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        return True
    except Exception:
        return False


def _email_template(kind: str, **ctx):
    app_name = UI_BRAND.get("name", "HurkField")
    if kind == "verify":
        link = ctx.get("link", "")
        subject = f"Verify your {app_name} email"
        text = f"Verify your email using this link:\n{link}\n\nIf you did not create this account, ignore this email."
        html_body = f"""
        <div style="font-family:Arial,sans-serif; line-height:1.5; color:#111;">
          <h2 style="margin:0 0 8px 0">Verify your email</h2>
          <p>Welcome to {html.escape(app_name)}. Please verify your email to activate your workspace.</p>
          <p><a href="{html.escape(link)}" style="display:inline-block; padding:10px 14px; background:#111827; color:#fff; text-decoration:none; border-radius:8px">Verify email</a></p>
          <p style="color:#6b7280; font-size:12px">If you did not create this account, ignore this email.</p>
          <p style="color:#6b7280; font-size:12px">Link: {html.escape(link)}</p>
        </div>
        """
        return subject, text, html_body
    if kind == "reset":
        link = ctx.get("link", "")
        subject = f"Reset your {app_name} password"
        text = f"Reset your password using this link:\n{link}\n\nIf you did not request this, ignore this email."
        html_body = f"""
        <div style="font-family:Arial,sans-serif; line-height:1.5; color:#111;">
          <h2 style="margin:0 0 8px 0">Reset your password</h2>
          <p>Use the button below to reset your password.</p>
          <p><a href="{html.escape(link)}" style="display:inline-block; padding:10px 14px; background:#111827; color:#fff; text-decoration:none; border-radius:8px">Reset password</a></p>
          <p style="color:#6b7280; font-size:12px">If you did not request this, ignore this email.</p>
          <p style="color:#6b7280; font-size:12px">Link: {html.escape(link)}</p>
        </div>
        """
        return subject, text, html_body
    if kind == "invite":
        link = ctx.get("link", "")
        org_name = (ctx.get("org_name") or "").strip()
        inviter = (ctx.get("inviter") or "").strip()
        note = (ctx.get("note") or "").strip()
        org_label = org_name if org_name else app_name
        subject = f"{org_label} invited you to {app_name}"
        intro = f"{org_label} invited you to join their workspace on {app_name}."
        if inviter:
            intro = f"{org_label} invited you to join their workspace on {app_name}. Invited by {inviter}."
        if note:
            text = f"{intro}\n\nMessage from {inviter or org_label}:\n{note}\n\nAccept invite:\n{link}\n\nIf you did not expect this, ignore this email."
        else:
            text = f"{intro}\n\nAccept invite:\n{link}\n\nIf you did not expect this, ignore this email."
        html_note = f'''
          <div style="margin:12px 0; padding:10px 12px; background:#f3f4f6; border-radius:10px;">
            <div style="font-size:12px; color:#6b7280; margin-bottom:6px;">Message from {html.escape(inviter or org_label)}:</div>
            <div style="white-space:pre-line;">{html.escape(note)}</div>
          </div>
        ''' if note else ""
        html_body = f"""
        <div style="font-family:Arial,sans-serif; line-height:1.5; color:#111;">
          <h2 style="margin:0 0 8px 0">You're invited</h2>
          <p>{html.escape(intro)}</p>
          {html_note}
          <p><a href="{html.escape(link)}" style="display:inline-block; padding:10px 14px; background:#111827; color:#fff; text-decoration:none; border-radius:8px">Accept invite</a></p>
          <p style="color:#6b7280; font-size:12px">If you did not expect this, ignore this email.</p>
          <p style="color:#6b7280; font-size:12px">Link: {html.escape(link)}</p>
        </div>
        """
        return subject, text, html_body
    return "HurkField", "", None


def _log_audit(org_id: int | None, actor_user_id: int | None, action: str, target_type: str = "", target_id: int | None = None, meta: dict | None = None):
    try:
        with get_conn() as conn:
            conn.execute(
                """
                INSERT INTO audit_logs (organization_id, actor_user_id, action, target_type, target_id, meta_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(org_id) if org_id is not None else None,
                    int(actor_user_id) if actor_user_id is not None else None,
                    action,
                    target_type or None,
                    int(target_id) if target_id is not None else None,
                    json.dumps(meta or {}),
                    now_iso(),
                ),
            )
            conn.commit()
    except Exception:
        pass


def _create_invite(org_id: int, email: str, role: str, created_by: int | None = None, hours_valid: int = 72) -> str:
    token = secrets.token_urlsafe(24)
    expires_at = (datetime.now() + timedelta(hours=hours_valid)).isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO user_invites (organization_id, email, role, token, status, expires_at, created_by, created_at)
            VALUES (?, ?, ?, ?, 'PENDING', ?, ?, ?)
            """,
            (int(org_id), email.lower(), role.upper(), token, expires_at, created_by, now_iso()),
        )
        conn.commit()
    return token


def _get_invite(token: str):
    if not token:
        return None
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM user_invites WHERE token=? LIMIT 1", (token,))
            row = cur.fetchone()
            if not row:
                return None
            data = dict(row)
    except Exception:
        return None
    if (data.get("status") or "").upper() != "PENDING":
        return None
    exp = data.get("expires_at")
    try:
        if exp and datetime.fromisoformat(exp) < datetime.now():
            return None
    except Exception:
        pass
    return data


@app.route("/signup", methods=["GET", "POST"])
def ui_signup():
    if getattr(g, "user", None):
        return redirect(url_for("ui_dashboard"))
    err = ""
    msg = ""
    oauth_pending = session.get("oauth_pending") or {}
    oauth_flow = request.values.get("oauth") == "1" or request.form.get("oauth_flow") == "1" or bool(oauth_pending)
    from_login_hint = request.args.get("from") == "login"
    if request.method == "POST":
        try:
            org_type = (request.form.get("org_type") or "").strip()
            org_name = (request.form.get("org_name") or "").strip()
            country = (request.form.get("country") or "").strip()
            region = (request.form.get("region") or "").strip()
            sector = (request.form.get("sector") or "").strip()
            size = (request.form.get("size") or "").strip()
            website = (request.form.get("website") or "").strip()
            domain = _normalize_domain_value(request.form.get("domain") or "")
            address = (request.form.get("address") or "").strip()

            full_name = (request.form.get("full_name") or "").strip() or (oauth_pending.get("name") or "").strip()
            email = (request.form.get("email") or "").strip().lower() or (oauth_pending.get("email") or "").strip().lower()
            oauth_provider = (oauth_pending.get("provider") or "").strip().lower()
            oauth_sub = (oauth_pending.get("sub") or "").strip()
            title = (request.form.get("title") or "").strip()
            phone = (request.form.get("phone") or "").strip()
            password = (request.form.get("password") or "")
            confirm = (request.form.get("confirm") or "")
            agree_terms = request.form.get("agree_terms") == "on"
            agree_privacy = request.form.get("agree_privacy") == "on"

            if not org_name or not full_name or not email:
                raise ValueError("Organization name, full name, and work email are required.")
            if not agree_terms or not agree_privacy:
                raise ValueError("You must agree to the Terms and Privacy Policy.")
            if not oauth_flow:
                if not password or not confirm:
                    raise ValueError("Password and confirmation are required.")
                if password != confirm:
                    raise ValueError("Passwords do not match.")
                if not _password_is_valid(password):
                    raise ValueError("Password must be at least 10 characters and include letters and numbers.")

            # Prevent duplicate user
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id FROM users WHERE LOWER(email)=LOWER(?) LIMIT 1", (email,))
                if cur.fetchone():
                    raise ValueError("An account with this email already exists. Please sign in.")

            # Infer domain from email if not provided
            if not domain and "@" in email:
                domain = _normalize_domain_value(email)

            # Join existing org if domain matches
            matched_org = _get_org_by_domain(domain)
            organization_id = None
            if matched_org:
                organization_id = int(matched_org.get("id"))
            else:
                organization_id = prj.create_organization(
                    name=org_name,
                    sector=sector,
                    org_type=org_type,
                    country=country,
                    region=region,
                    size=size,
                    website=website,
                    domain=domain,
                    address=address,
                )

            # Create user
            user_status = "PENDING" if matched_org else "ACTIVE"
            user_role = "OWNER" if not matched_org else "SUPERVISOR"
            verified_flag = 1 if oauth_flow else 0
            user_id = _create_user(
                organization_id=organization_id,
                full_name=full_name,
                email=email,
                password=None if oauth_flow else password,
                role=user_role,
                title=title,
                phone=phone,
                email_verified=verified_flag,
                status=user_status,
            )

            session.pop("oauth_pending", None)

            if not oauth_flow:
                verify_token = _create_user_token(user_id, "VERIFY_EMAIL", hours_valid=48)
                verify_url = url_for("verify_email", token=verify_token, _external=True)
                subject, text_body, html_body = _email_template("verify", link=verify_url)
                sent = _send_email(email, subject, text_body, html_body)
                extra = "<div class='muted' style='margin-top:10px'>We sent a verification link to your email.</div>" if sent else f"<div class='muted' style='margin-top:10px'>Email sending is not configured. Use this link: <code>{html.escape(verify_url)}</code></div>"
                return ui_shell(
                    "Verify Email",
                    f"<div class='card'><h2>Verify your email</h2><div class='muted'>Use the link below to verify your email address.</div><div style='margin-top:12px'><a class='btn btn-primary' href='{verify_url}'>Verify email</a></div>{extra}</div>",
                    show_project_switcher=False,
                )

            if matched_org:
                return redirect(url_for("ui_login") + "?pending=1")

            # OAuth flow: auto-login and send to quicklinks
            session["user_id"] = int(user_id)
            session["org_id"] = int(organization_id) if organization_id is not None else None
            session["role"] = user_role
            session["user_name"] = (full_name or "").strip()
            session["user_email"] = email
            session.permanent = True
            _create_session_record(int(user_id))
            _log_security_event(int(user_id), "LOGIN_SUCCESS", {"provider": oauth_provider or "oauth"})
            try:
                with get_conn() as conn:
                    if oauth_provider == "google" and oauth_sub:
                        conn.execute(
                            "UPDATE users SET google_sub=?, auth_provider=?, updated_at=?, last_login_at=? WHERE id=?",
                            (oauth_sub, "google", now_iso(), now_iso(), int(user_id)),
                        )
                    else:
                        conn.execute("UPDATE users SET last_login_at=? WHERE id=?", (now_iso(), int(user_id)))
                    conn.commit()
            except Exception:
                pass
            return redirect(url_for("ui_dashboard"))
        except Exception as e:
            err = str(e)

    err_html = err if "<a" in err else html.escape(err)
    social_html = """
      <div class="mt-4 grid grid-cols-2 gap-2">
        <a class="flex items-center justify-center gap-2 rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs font-semibold text-slate-700 hover:border-slate-300" href="/auth/google?intent=signup">Continue with Google</a>
        <a class="flex items-center justify-center gap-2 rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs font-semibold text-slate-700 hover:border-slate-300" href="/auth/microsoft?intent=signup">Continue with Microsoft</a>
        <a class="flex items-center justify-center gap-2 rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs font-semibold text-slate-700 hover:border-slate-300" href="/auth/linkedin?intent=signup">Continue with LinkedIn</a>
        <a class="flex items-center justify-center gap-2 rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs font-semibold text-slate-700 hover:border-slate-300" href="/auth/facebook?intent=signup">Continue with Facebook</a>
      </div>
      <div class="mt-3 text-xs text-slate-500">or create account with email</div>
    """ if not oauth_flow else ""
    control_cls = "mt-1 w-full rounded-2xl border border-slate-200 bg-white px-3 py-2.5 text-sm text-slate-900 placeholder:text-slate-400 shadow-sm transition focus:border-violet-500 focus:outline-none focus:ring-4 focus:ring-violet-100"

    html_page = f"""
<div class="min-h-screen bg-gradient-to-b from-violet-50 via-white to-slate-50 py-8 sm:py-10">
  <div class="max-w-7xl mx-auto px-4 sm:px-6">
    <div class="bg-white border border-violet-100 rounded-[28px] shadow-[0_26px_90px_-36px_rgba(124,58,237,0.48)] overflow-hidden grid lg:grid-cols-2">
      <div class="p-8 sm:p-10 text-white bg-gradient-to-br from-violet-700 via-fuchsia-600 to-indigo-600">
        <a href="/" class="inline-flex items-center">
          <img src="/static/logos/hurkfield.jpeg" alt="HurkField logo" class="h-16 w-auto rounded-2xl shadow-2xl ring-1 ring-white/30" />
        </a>
        <h2 class="text-3xl sm:text-4xl font-extrabold mt-5 tracking-tight">Create your HurkField workspace</h2>
        <p class="text-white/90 mt-3 text-sm sm:text-base leading-relaxed">Set up a secure workspace to build forms, assign enumerators, and monitor submissions.</p>
        <div class="mt-7 space-y-2.5 text-sm text-white/95">
          <div class="flex items-center gap-2"><span class="inline-flex h-5 w-5 items-center justify-center rounded-full bg-white/20 text-xs">✓</span> Project-based data collection</div>
          <div class="flex items-center gap-2"><span class="inline-flex h-5 w-5 items-center justify-center rounded-full bg-white/20 text-xs">✓</span> Enumerator assignments + tracking</div>
          <div class="flex items-center gap-2"><span class="inline-flex h-5 w-5 items-center justify-center rounded-full bg-white/20 text-xs">✓</span> QA, exports, and audit trails</div>
        </div>
        <div class="mt-7 rounded-3xl bg-white/10 border border-white/20 p-4 shadow-inner shadow-white/10">
          <svg viewBox="0 0 420 240" width="100%" height="200" aria-hidden="true">
            <path d="M36 118c-8-52 42-92 94-88 34 3 54-4 78-22 34-25 90-5 110 30 22 38 56 34 70 68 18 43-10 88-54 96-38 7-58 34-102 36-48 2-62-22-100-24-48-2-82-24-96-96Z" fill="rgba(255,255,255,0.18)"/>
            <rect x="46" y="184" width="328" height="12" rx="6" fill="rgba(255,255,255,0.45)"/>
            <rect x="48" y="70" width="170" height="104" rx="14" fill="rgba(255,255,255,0.25)"/>
            <rect x="60" y="82" width="146" height="80" rx="10" fill="rgba(255,255,255,0.35)"/>
            <rect x="70" y="92" width="110" height="8" rx="4" fill="rgba(255,255,255,0.85)"/>
            <rect x="70" y="108" width="86" height="7" rx="3.5" fill="rgba(255,255,255,0.65)"/>
            <rect x="70" y="124" width="100" height="7" rx="3.5" fill="rgba(255,255,255,0.65)"/>
            <rect x="166" y="92" width="6" height="8" rx="3" fill="#5b2ccf"/>
            <rect x="106" y="174" width="56" height="6" rx="3" fill="rgba(255,255,255,0.55)"/>
            <rect x="208" y="150" width="96" height="46" rx="10" fill="rgba(255,255,255,0.35)"/>
            <rect x="218" y="158" width="76" height="26" rx="6" fill="rgba(255,255,255,0.55)"/>
            <circle cx="286" cy="88" r="18" fill="rgba(255,255,255,0.9)"/>
            <path d="M268 88c6-16 28-18 36 0" fill="rgba(91,44,207,0.75)"/>
            <circle cx="280" cy="88" r="3" fill="#5b2ccf"/>
            <circle cx="292" cy="88" r="3" fill="#5b2ccf"/>
            <path d="M278 98c4 4 12 4 16 0" stroke="#5b2ccf" stroke-width="3" stroke-linecap="round"/>
            <path d="M254 120c12-14 44-16 60 0v36h-60v-36Z" fill="rgba(255,255,255,0.4)"/>
            <path d="M252 134c10 4 18 8 24 16" stroke="rgba(255,255,255,0.85)" stroke-width="6" stroke-linecap="round"/>
            <path d="M318 134c-10 4-18 8-24 16" stroke="rgba(255,255,255,0.85)" stroke-width="6" stroke-linecap="round"/>
          </svg>
        </div>
      </div>
      <div class="p-6 sm:p-8 lg:p-10 bg-slate-50/50">
        <div class="text-2xl sm:text-3xl font-extrabold text-slate-900">Workspace details</div>
        <div class="text-sm text-slate-600 mt-1">Complete your organization profile to continue.</div>
        {"<div class='mt-4 rounded-xl border border-indigo-200 bg-indigo-50 px-4 py-3 text-sm text-indigo-800'><b>No account found:</b> complete signup to create your workspace.</div>" if from_login_hint else ""}
        {f"<div class='mt-4 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700'><b>Error:</b> {err_html}</div>" if err else ""}
        {social_html}
        <form method="POST" class="mt-6 space-y-4 rounded-2xl border border-slate-200 bg-white p-4 sm:p-5 shadow-sm">
          <input type="hidden" name="oauth_flow" value="{1 if oauth_flow else 0}" />
          <div class="grid md:grid-cols-2 gap-4">
            <div>
              <label class="text-xs font-semibold text-slate-600">Account type</label>
              <select name="org_type" class="{control_cls}">
                <option value="Organization">Organization / NGO / Agency</option>
                <option value="Individual">Individual Researcher</option>
                <option value="Firm">Research Firm / Consultancy</option>
                <option value="Government">Government / Public Sector</option>
              </select>
            </div>
            <div>
              <label class="text-xs font-semibold text-slate-600">Organization name</label>
              <input name="org_name" placeholder="e.g., Global Health Initiative" required class="{control_cls}" />
            </div>
          </div>
          <div class="grid md:grid-cols-3 gap-4">
            <div>
              <label class="text-xs font-semibold text-slate-600">Country</label>
              <input name="country" placeholder="e.g., Nigeria" class="{control_cls}" />
            </div>
            <div>
              <label class="text-xs font-semibold text-slate-600">Region / State</label>
              <input name="region" placeholder="e.g., Lagos" class="{control_cls}" />
            </div>
            <div>
              <label class="text-xs font-semibold text-slate-600">Sector</label>
              <select name="sector" class="{control_cls}">
                <option value="">Select sector</option>
                <option>Health</option>
                <option>Education</option>
                <option>WASH</option>
                <option>Infrastructure</option>
                <option>Agriculture</option>
                <option>Gender</option>
                <option>Humanitarian</option>
                <option>Research</option>
                <option>Other</option>
              </select>
            </div>
          </div>
          <div class="grid md:grid-cols-3 gap-4">
            <div>
              <label class="text-xs font-semibold text-slate-600">Organization size</label>
              <select name="size" class="{control_cls}">
                <option value="">Select size</option>
                <option>1-10</option>
                <option>11-50</option>
                <option>51-200</option>
                <option>200+</option>
              </select>
            </div>
            <div>
              <label class="text-xs font-semibold text-slate-600">Website (optional)</label>
              <input name="website" placeholder="https://example.org" class="{control_cls}" />
            </div>
            <div>
              <label class="text-xs font-semibold text-slate-600">Org email domain (optional)</label>
              <input name="domain" placeholder="example.org" class="{control_cls}" />
            </div>
          </div>
          <div>
            <label class="text-xs font-semibold text-slate-600">Address (optional)</label>
            <input name="address" placeholder="Office address" class="{control_cls}" />
          </div>
          <div class="pt-4 border-t border-slate-200">
            <div class="text-lg font-bold">Your details</div>
            <div class="grid md:grid-cols-2 gap-4 mt-3">
              <div>
                <label class="text-xs font-semibold text-slate-600">Full name</label>
                <input name="full_name" placeholder="Your name" value="{html.escape(oauth_pending.get('name',''))}" required class="{control_cls}" />
              </div>
              <div>
                <label class="text-xs font-semibold text-slate-600">Work email</label>
                <input name="email" type="email" placeholder="you@org.org" value="{html.escape(oauth_pending.get('email',''))}" required class="{control_cls}" />
              </div>
            </div>
            <div class="grid md:grid-cols-2 gap-4 mt-3">
              <div>
                <label class="text-xs font-semibold text-slate-600">Role / Title</label>
                <input name="title" placeholder="e.g., M&E Officer" class="{control_cls}" />
              </div>
              <div>
                <label class="text-xs font-semibold text-slate-600">Phone (optional)</label>
                <input name="phone" placeholder="+234..." class="{control_cls}" />
              </div>
            </div>
            {"" if oauth_flow else """
            <div class=\"grid md:grid-cols-2 gap-4 mt-3\">
              <div>
                <label class=\"text-xs font-semibold text-slate-600\">Password</label>
                <input name=\"password\" type=\"password\" required class=\"""" + control_cls + """\" />
              </div>
              <div>
                <label class=\"text-xs font-semibold text-slate-600\">Confirm password</label>
                <input name=\"confirm\" type=\"password\" required class=\"""" + control_cls + """\" />
              </div>
            </div>
            <div class=\"mt-2 text-xs text-slate-500\">Password: 10+ characters, letters + numbers.</div>
            """}
          </div>
          <div class="flex flex-wrap gap-4 text-xs text-slate-600 pt-2">
            <label class="flex items-center gap-2"><input type="checkbox" name="agree_terms" /> I agree to the Terms of Service</label>
            <label class="flex items-center gap-2"><input type="checkbox" name="agree_privacy" /> I agree to the Privacy Policy</label>
          </div>
          <button class="mt-2 inline-flex w-full items-center justify-center rounded-2xl bg-violet-600 py-3 text-sm font-semibold tracking-wide text-white shadow-lg shadow-violet-300/60 transition hover:bg-violet-700 focus:outline-none focus:ring-4 focus:ring-violet-200" style="background:linear-gradient(135deg,#7C3AED,#8B5CF6);color:#fff;" type="submit">Create workspace</button>
          <div class="text-xs text-slate-500 mt-2">Already have an account? <a class="font-semibold text-violet-700 hover:text-violet-800" href="/login">Sign in</a></div>
        </form>
      </div>
    </div>
  </div>
</div>
"""
    return ui_shell("Sign up", html_page, show_project_switcher=False, show_nav=False)


@app.route("/login", methods=["GET", "POST"])
def ui_login():
    if getattr(g, "user", None):
        return redirect(url_for("ui_dashboard"))
    err = ""
    next_url = request.values.get("next") or url_for("ui_dashboard")
    pending_flag = request.args.get("pending") == "1"
    if request.method == "POST":
        try:
            email = (request.form.get("email") or "").strip().lower()
            password = (request.form.get("password") or "")
            if not email or not password:
                raise ValueError("Email and password are required.")
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT * FROM users WHERE LOWER(email)=LOWER(?) LIMIT 1", (email,))
                row = cur.fetchone()
                if not row:
                    raise ValueError("Invalid email or password.")
                user = dict(row)
            if not check_password_hash(user.get("password_hash") or "", password):
                raise ValueError("Invalid email or password.")
            status = (user.get("status") or "ACTIVE").upper()
            if status == "PENDING":
                raise ValueError("Account pending approval. Please wait for an owner to approve access.")
            if status != "ACTIVE":
                raise ValueError("Account inactive.")
            if int(user.get("email_verified") or 0) != 1:
                raise ValueError(f"Email not verified. <a href='/resend-verification?email={html.escape(email)}'>Resend verification</a>.")
            session["user_id"] = int(user.get("id"))
            session["org_id"] = user.get("organization_id")
            session["role"] = user.get("role")
            session["user_name"] = (user.get("full_name") or "").strip()
            session["user_email"] = (user.get("email") or "").strip().lower()
            session.permanent = True
            _create_session_record(int(user.get("id")))
            _log_security_event(int(user.get("id")), "LOGIN_SUCCESS", {"provider": "local"})
            try:
                with get_conn() as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT google_sub FROM users WHERE id=? LIMIT 1", (int(user.get("id")),))
                    r = cur.fetchone()
                    has_google = bool(r and (r["google_sub"] or "").strip())
                    conn.execute(
                        "UPDATE users SET auth_provider=?, updated_at=?, last_login_at=? WHERE id=?",
                        ("both" if has_google else "local", now_iso(), now_iso(), int(user.get("id"))),
                    )
                    conn.commit()
            except Exception:
                pass
            return redirect(next_url)
        except Exception as e:
            err = str(e)

    pending_html = (
        "<div class='mt-4 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800'><b>Pending:</b> Your account is awaiting approval.</div>"
        if pending_flag
        else ""
    )
    err_html = (
        f"<div class='mt-4 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700'><b>Error:</b> {html.escape(err)}</div>"
        if err
        else ""
    )

    html_page = f"""
    <div class="min-h-screen bg-gradient-to-br from-primaryLight via-white to-slate-100 py-10">
      <div class="max-w-6xl mx-auto px-5 lg:px-8">
        <div class="overflow-hidden rounded-3xl border border-slate-200 bg-white shadow-2xl">
          <div class="grid lg:grid-cols-2">
            <div class="relative p-8 md:p-12 text-white bg-gradient-to-br from-[#6D28D9] via-[#7C3AED] to-[#8B5CF6]">
              <div class="inline-flex items-center rounded-full border border-white/35 bg-white/15 px-3 py-1 text-xs font-semibold">
                Welcome back
              </div>
              <h1 class="mt-4 text-3xl md:text-4xl font-extrabold">Sign in to HurkField</h1>
              <p class="mt-3 text-sm md:text-base text-white/85">
                Access your organization workspace and continue field operations with clarity.
              </p>
              <div class="mt-8 rounded-2xl border border-white/25 bg-white/10 p-4">
                <div class="grid grid-cols-3 gap-3">
                  <div class="rounded-xl bg-white/20 p-3">
                    <div class="text-xl font-extrabold">32K+</div>
                    <div class="text-[11px] text-white/75">Organizations</div>
                  </div>
                  <div class="rounded-xl bg-white/20 p-3">
                    <div class="text-xl font-extrabold">220+</div>
                    <div class="text-[11px] text-white/75">Countries</div>
                  </div>
                  <div class="rounded-xl bg-white/20 p-3">
                    <div class="text-xl font-extrabold">20M+</div>
                    <div class="text-[11px] text-white/75">Monthly surveys</div>
                  </div>
                </div>
              </div>
              <div class="mt-6 text-xs text-white/75">Secure access for owners, supervisors, and analysts.</div>
            </div>

            <div class="p-8 md:p-10">
              <h2 class="text-2xl font-extrabold text-slate-900">Sign in</h2>
              <p class="mt-1 text-sm text-slate-500">Access your organization workspace.</p>

              {pending_html}
              {err_html}

              <form method="POST" class="mt-6 space-y-4">
                <input type="hidden" name="next" value="{html.escape(next_url)}" />
                <div>
                  <label class="mb-1 block text-xs font-semibold uppercase tracking-wide text-slate-600" for="login-email">Email</label>
                  <input id="login-email" name="email" type="email" required autocomplete="email" class="w-full rounded-xl border border-slate-300 px-3 py-2.5 text-sm outline-none transition focus:border-primary focus:ring-2 focus:ring-primary/25" />
                </div>
                <div>
                  <label class="mb-1 block text-xs font-semibold uppercase tracking-wide text-slate-600" for="login-password">Password</label>
                  <input id="login-password" name="password" type="password" required autocomplete="current-password" class="w-full rounded-xl border border-slate-300 px-3 py-2.5 text-sm outline-none transition focus:border-primary focus:ring-2 focus:ring-primary/25" />
                </div>
                <button class="w-full rounded-xl border border-[#7C3AED] bg-[#7C3AED] py-2.5 font-semibold text-white shadow-lg shadow-[rgba(124,58,237,0.35)] transition hover:bg-[#6D28D9]" type="submit">Sign in</button>
              </form>

              <div class="mt-4 text-center text-xs text-slate-500">
                No account? <a class="font-semibold text-brand" href="/signup">Create workspace</a>
              </div>
              <div class="mt-2 text-center text-xs text-slate-500">
                <a class="font-semibold text-brand" href="/forgot-password">Forgot password?</a>
                <span class="px-1">·</span>
                <a class="font-semibold text-brand" href="/resend-verification">Resend verification</a>
              </div>
              <div class="mt-2 text-center text-xs text-slate-500">
                Have an access key? <a class="font-semibold text-brand" href="/ui/access">Use supervisor key</a>
              </div>

              <div class="mt-6 flex items-center gap-3 text-xs uppercase tracking-[0.14em] text-slate-400">
                <span class="h-px flex-1 bg-slate-200"></span>
                continue with
                <span class="h-px flex-1 bg-slate-200"></span>
              </div>

              <div class="mt-4 grid grid-cols-1 sm:grid-cols-2 gap-3">
                <a href="/auth/google?intent=login" class="group inline-flex items-center gap-3 rounded-xl border border-slate-300 bg-white px-3 py-2.5 text-sm font-semibold text-slate-700 transition hover:border-primary/40 hover:bg-primaryLight/40">
                  <svg viewBox="0 0 24 24" class="h-5 w-5" aria-hidden="true">
                    <path fill="#EA4335" d="M12 5c1.6 0 3 .6 4.1 1.6l3-3C17.2 1.8 14.8 1 12 1 7.7 1 3.9 3.5 2.1 7.1l3.5 2.7C6.5 7 9 5 12 5z"></path>
                    <path fill="#34A853" d="M12 23c2.8 0 5.2-.9 7-2.5l-3.2-2.5c-1 .7-2.3 1.2-3.8 1.2-3 0-5.5-2-6.4-4.8l-3.6 2.8C3.8 20.5 7.6 23 12 23z"></path>
                    <path fill="#FBBC05" d="M5.6 14.4c-.2-.7-.4-1.5-.4-2.4s.1-1.7.4-2.4L2 6.8C1.3 8.3 1 10.1 1 12s.3 3.7 1 5.2l3.6-2.8z"></path>
                    <path fill="#4285F4" d="M23 12c0-.8-.1-1.5-.2-2.2H12v4.2h6.2c-.3 1.4-1.1 2.7-2.4 3.5l3.2 2.5c1.9-1.8 3-4.4 3-8z"></path>
                  </svg>
                  Continue with Google
                </a>

                <a href="/auth/microsoft?intent=login" class="group inline-flex items-center gap-3 rounded-xl border border-slate-300 bg-white px-3 py-2.5 text-sm font-semibold text-slate-700 transition hover:border-primary/40 hover:bg-primaryLight/40">
                  <svg viewBox="0 0 24 24" class="h-5 w-5" aria-hidden="true">
                    <rect x="2" y="2" width="9" height="9" fill="#F25022"></rect>
                    <rect x="13" y="2" width="9" height="9" fill="#7FBA00"></rect>
                    <rect x="2" y="13" width="9" height="9" fill="#00A4EF"></rect>
                    <rect x="13" y="13" width="9" height="9" fill="#FFB900"></rect>
                  </svg>
                  Continue with Microsoft
                </a>

                <a href="/auth/linkedin?intent=login" class="group inline-flex items-center gap-3 rounded-xl border border-slate-300 bg-white px-3 py-2.5 text-sm font-semibold text-slate-700 transition hover:border-primary/40 hover:bg-primaryLight/40">
                  <svg viewBox="0 0 24 24" class="h-5 w-5" aria-hidden="true">
                    <rect x="1" y="1" width="22" height="22" rx="4" fill="#0A66C2"></rect>
                    <path fill="#fff" d="M7.1 9h2.4v7.6H7.1V9zm1.2-3.8c.8 0 1.4.6 1.4 1.4S9.1 8 8.3 8 6.9 7.4 6.9 6.6s.6-1.4 1.4-1.4zm2.7 3.8h2.3v1c.3-.6 1.1-1.2 2.3-1.2 2.4 0 2.9 1.6 2.9 3.7v4.1h-2.4v-3.6c0-.9 0-2-1.2-2s-1.4.9-1.4 2v3.6H11V9z"></path>
                  </svg>
                  Continue with LinkedIn
                </a>

                <a href="/auth/facebook?intent=login" class="group inline-flex items-center gap-3 rounded-xl border border-slate-300 bg-white px-3 py-2.5 text-sm font-semibold text-slate-700 transition hover:border-primary/40 hover:bg-primaryLight/40">
                  <svg viewBox="0 0 24 24" class="h-5 w-5" aria-hidden="true">
                    <circle cx="12" cy="12" r="11" fill="#1877F2"></circle>
                    <path fill="#fff" d="M13.6 8.2h1.8V5.4c-.3 0-1.2-.1-2.3-.1-2.3 0-3.9 1.4-3.9 4v2.2H6.6v3.1h2.6v4.8h3.2v-4.8h2.5l.4-3.1h-2.9V9.7c0-.9.2-1.5 1.2-1.5z"></path>
                  </svg>
                  Continue with Facebook
                </a>
              </div>

              <div class="mt-4 text-center text-xs text-slate-400">
                By signing in, you agree to the Terms and Privacy Policy.
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    """
    return ui_shell("Login", html_page, show_project_switcher=False, show_nav=False)


@app.route("/signin")
def ui_signin_alias():
    return redirect(url_for("ui_login"))


@app.route("/logout")
def ui_logout():
    uid = session.get("user_id")
    raw = session.get("session_token")
    if uid:
        _revoke_session_token(raw)
        _log_security_event(int(uid), "LOGOUT")
    session.clear()
    return redirect(url_for("ui_login"))


@app.route("/verify-email")
def verify_email():
    token = (request.args.get("token") or "").strip()
    row = _get_valid_token(token, "VERIFY_EMAIL")
    if not row:
        return ui_shell("Verify Email", "<div class='card'><h2>Invalid or expired link</h2></div>", show_project_switcher=False), 400
    user_id = int(row.get("user_id"))
    try:
        with get_conn() as conn:
            conn.execute(
                "UPDATE users SET email_verified=1, verified_at=? WHERE id=?",
                (now_iso(), user_id),
            )
            conn.commit()
    except Exception:
        pass
    _mark_token_used(int(row.get("id")))
    return ui_shell(
        "Verify Email",
        "<div class='card'><h2>Email verified</h2><div class='muted'>You can now sign in.</div><a class='btn btn-primary' href='/login'>Sign in</a></div>",
        show_project_switcher=False,
    )


@app.route("/resend-verification", methods=["GET", "POST"])
def resend_verification():
    err = ""
    msg = ""
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id, email_verified FROM users WHERE LOWER(email)=LOWER(?) LIMIT 1", (email,))
                row = cur.fetchone()
                if not row:
                    msg = "If the email exists, a verification link will be sent."
                else:
                    if int(row["email_verified"] or 0) == 1:
                        msg = "Email already verified."
                    else:
                        token = _create_user_token(int(row["id"]), "VERIFY_EMAIL", hours_valid=48)
                        verify_url = url_for("verify_email", token=token, _external=True)
                        subject, text_body, html_body = _email_template("verify", link=verify_url)
                        sent = _send_email(email, subject, text_body, html_body)
                        msg = "Verification email sent." if sent else f"Verification link generated: <a href='{verify_url}'>Verify email</a>"
        except Exception as e:
            err = str(e)

    msg_html = msg if "<a" in msg else html.escape(msg)
    html_page = f"""
    <div class="card" style="max-width:520px;margin:40px auto;">
      <h2 class="h2" style="margin-top:0">Resend verification</h2>
      <div class="muted">Enter your email to get a new verification link.</div>
      {f"<div class='card' style='border-color: rgba(231, 76, 60, .35);margin-top:12px'><b>Error:</b> {html.escape(err)}</div>" if err else ""}
      {f"<div class='card' style='border-color: rgba(46, 204, 113, .35);margin-top:12px'>{msg_html}</div>" if msg else ""}
      <form method="POST" class="stack" style="margin-top:16px">
        <label style="font-weight:800">Email</label>
        <input name="email" type="email" required />
        <button class="btn btn-primary" type="submit">Send link</button>
        <div class="muted" style="margin-top:8px"><a href="/login">Back to login</a></div>
      </form>
    </div>
    """
    return ui_shell("Resend Verification", html_page, show_project_switcher=False)


@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    err = ""
    msg = ""
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id FROM users WHERE LOWER(email)=LOWER(?) LIMIT 1", (email,))
                row = cur.fetchone()
                if row:
                    token = _create_user_token(int(row["id"]), "RESET_PASSWORD", hours_valid=2)
                    reset_url = url_for("reset_password", token=token, _external=True)
                    subject, text_body, html_body = _email_template("reset", link=reset_url)
                    sent = _send_email(email, subject, text_body, html_body)
                    msg = "Password reset email sent." if sent else f"Reset link generated: <a href='{reset_url}'>Reset password</a>"
                else:
                    msg = "If the email exists, a reset link will be sent."
        except Exception as e:
            err = str(e)

    msg_html = msg if "<a" in msg else html.escape(msg)
    html_page = f"""
    <div class="card" style="max-width:520px;margin:40px auto;">
      <h2 class="h2" style="margin-top:0">Reset password</h2>
      <div class="muted">Enter your email to receive a reset link.</div>
      {f"<div class='card' style='border-color: rgba(231, 76, 60, .35);margin-top:12px'><b>Error:</b> {html.escape(err)}</div>" if err else ""}
      {f"<div class='card' style='border-color: rgba(46, 204, 113, .35);margin-top:12px'>{msg_html}</div>" if msg else ""}
      <form method="POST" class="stack" style="margin-top:16px">
        <label style="font-weight:800">Email</label>
        <input name="email" type="email" required />
        <button class="btn btn-primary" type="submit">Send reset link</button>
        <div class="muted" style="margin-top:8px"><a href="/login">Back to login</a></div>
      </form>
    </div>
    """
    return ui_shell("Forgot Password", html_page, show_project_switcher=False)


@app.route("/reset-password", methods=["GET", "POST"])
def reset_password():
    token = (request.args.get("token") or request.form.get("token") or "").strip()
    row = _get_valid_token(token, "RESET_PASSWORD")
    if not row:
        return ui_shell("Reset Password", "<div class='card'><h2>Invalid or expired reset link</h2></div>", show_project_switcher=False), 400
    err = ""
    if request.method == "POST":
        try:
            password = (request.form.get("password") or "")
            confirm = (request.form.get("confirm") or "")
            if not password or not confirm:
                raise ValueError("Password and confirmation are required.")
            if password != confirm:
                raise ValueError("Passwords do not match.")
            if not _password_is_valid(password):
                raise ValueError("Password must be at least 10 characters and include letters and numbers.")
            with get_conn() as conn:
                conn.execute(
                    "UPDATE users SET password_hash=? WHERE id=?",
                    (generate_password_hash(password), int(row.get("user_id"))),
                )
                conn.commit()
            _mark_token_used(int(row.get("id")))
            return ui_shell(
                "Reset Password",
                "<div class='card'><h2>Password updated</h2><div class='muted'>You can now sign in.</div><a class='btn btn-primary' href='/login'>Sign in</a></div>",
                show_project_switcher=False,
            )
        except Exception as e:
            err = str(e)

    html_page = f"""
    <div class="card" style="max-width:520px;margin:40px auto;">
      <h2 class="h2" style="margin-top:0">Choose a new password</h2>
      {f"<div class='card' style='border-color: rgba(231, 76, 60, .35);margin-top:12px'><b>Error:</b> {html.escape(err)}</div>" if err else ""}
      <form method="POST" class="stack" style="margin-top:16px">
        <input type="hidden" name="token" value="{html.escape(token)}" />
        <label style="font-weight:800">New password</label>
        <input name="password" type="password" required />
        <label style="font-weight:800">Confirm password</label>
        <input name="confirm" type="password" required />
        <button class="btn btn-primary" type="submit">Update password</button>
      </form>
    </div>
    """
    return ui_shell("Reset Password", html_page, show_project_switcher=False)


@app.route("/invite/accept", methods=["GET", "POST"])
def accept_invite():
    token = (request.args.get("token") or request.form.get("token") or "").strip()
    invite = _get_invite(token)
    if not invite:
        return ui_shell("Invite", "<div class='card'><h2>Invalid or expired invite</h2></div>", show_project_switcher=False), 400

    err = ""
    if request.method == "POST":
        try:
            full_name = (request.form.get("full_name") or "").strip()
            password = (request.form.get("password") or "")
            confirm = (request.form.get("confirm") or "")
            if not full_name:
                raise ValueError("Full name is required.")
            if not password or not confirm:
                raise ValueError("Password and confirmation are required.")
            if password != confirm:
                raise ValueError("Passwords do not match.")
            if not _password_is_valid(password):
                raise ValueError("Password must be at least 10 characters and include letters and numbers.")

            email = (invite.get("email") or "").strip().lower()
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id FROM users WHERE LOWER(email)=LOWER(?) LIMIT 1", (email,))
                if cur.fetchone():
                    raise ValueError("An account with this email already exists.")

            user_id = _create_user(
                organization_id=int(invite.get("organization_id")),
                full_name=full_name,
                email=email,
                password=password,
                role=invite.get("role") or "SUPERVISOR",
                title="",
                phone="",
                email_verified=1,
                status="ACTIVE",
            )
            with get_conn() as conn:
                conn.execute("UPDATE user_invites SET status='USED', used_at=? WHERE id=?", (now_iso(), int(invite.get("id"))))
                conn.commit()
            _log_audit(int(invite.get("organization_id")), None, "user.invite.accepted", "user", user_id, {"email": email})
            return ui_shell(
                "Invite accepted",
                "<div class='card'><h2>Welcome!</h2><div class='muted'>Your account is ready. You can now sign in.</div><a class='btn btn-primary' href='/login'>Sign in</a></div>",
                show_project_switcher=False,
            )
        except Exception as e:
            err = str(e)

    html_page = f"""
    <div class="card" style="max-width:520px;margin:40px auto;">
      <h2 class="h2" style="margin-top:0">Accept invite</h2>
      <div class="muted">Set your password to join the workspace.</div>
      {f"<div class='card' style='border-color: rgba(231, 76, 60, .35);margin-top:12px'><b>Error:</b> {html.escape(err)}</div>" if err else ""}
      <form method="POST" class="stack" style="margin-top:16px">
        <input type="hidden" name="token" value="{html.escape(token)}" />
        <label style="font-weight:800">Full name</label>
        <input name="full_name" required />
        <label style="font-weight:800">Password</label>
        <input name="password" type="password" required />
        <label style="font-weight:800">Confirm password</label>
        <input name="confirm" type="password" required />
        <button class="btn btn-primary" type="submit">Create account</button>
      </form>
    </div>
    """
    return ui_shell("Accept Invite", html_page, show_project_switcher=False)


@app.route("/ui/audit")
def ui_audit_logs():
    gate = admin_gate()
    if gate:
        return gate

    org_id = current_org_id()
    if not org_id:
        return ui_shell("Audit Log", "<div class='card'><h2>No organization context</h2></div>", show_project_switcher=False), 400

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT al.*, u.full_name AS actor_name
            FROM audit_logs al
            LEFT JOIN users u ON u.id = al.actor_user_id
            WHERE al.organization_id=?
            ORDER BY al.id DESC
            LIMIT 300
            """,
            (int(org_id),),
        )
        logs = [dict(r) for r in cur.fetchall()]

    rows = []
    for a in logs:
        rows.append(
            f"""
            <tr>
              <td><span class="template-id">#{a.get('id')}</span></td>
              <td>{html.escape(a.get('actor_name') or 'System')}</td>
              <td class="muted">{html.escape(a.get('action') or '')}</td>
              <td class="muted">{html.escape(a.get('target_type') or '')}</td>
              <td class="muted">{a.get('target_id') or ''}</td>
              <td class="muted">{html.escape(a.get('meta_json') or '')}</td>
              <td class="muted">{a.get('created_at') or ''}</td>
            </tr>
            """
        )

    html_page = f"""
    <style>
      .audit-hero{{background:linear-gradient(135deg, rgba(124,58,237,.12), rgba(124,58,237,.04)); border-radius:18px; padding:18px; border:1px solid rgba(124,58,237,.12);}}
      .audit-stats{{display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:12px; margin-top:16px;}}
      .audit-stat{{border-radius:16px; padding:14px; background:var(--surface); border:1px solid rgba(124,58,237,.12); box-shadow:0 12px 24px rgba(15,18,34,.06);}}
      .audit-stat .label{{font-size:11px; text-transform:uppercase; letter-spacing:.08em; color:var(--muted);}}
      .audit-stat .value{{font-size:20px; font-weight:800; margin-top:6px; color:var(--primary);}}
      .audit-tools{{display:flex; gap:12px; flex-wrap:wrap; align-items:center; margin-top:12px;}}
      .audit-search{{min-width:240px; flex:1;}}
      .audit-badge{{display:inline-flex; align-items:center; gap:6px; padding:4px 8px; border-radius:999px; font-size:11px; font-weight:700; background:var(--primary-soft); color:var(--primary); border:1px solid rgba(124,58,237,.2);}}
      .audit-meta{{font-size:12px; color:var(--muted); word-break:break-word;}}
      .audit-table td{{vertical-align:top;}}
    </style>

    <div class="card audit-hero">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div>
          <h1 class="h1" style="margin:0">Audit log</h1>
          <div class="muted">Approvals, role changes, invites, and security actions.</div>
        </div>
        <a class="btn" href="/ui">Back to dashboard</a>
      </div>
      <div class="audit-stats">
        <div class="audit-stat">
          <div class="label">Total events</div>
          <div class="value">{len(logs)}</div>
        </div>
        <div class="audit-stat">
          <div class="label">Unique actors</div>
          <div class="value">{len(set([a.get('actor_name') or 'System' for a in logs]))}</div>
        </div>
        <div class="audit-stat">
          <div class="label">Invites</div>
          <div class="value">{len([a for a in logs if "invite" in str(a.get("action") or "").lower()])}</div>
        </div>
        <div class="audit-stat">
          <div class="label">Role changes</div>
          <div class="value">{len([a for a in logs if "role" in str(a.get("action") or "").lower()])}</div>
        </div>
      </div>
      <div class="audit-tools">
        <input id="auditSearch" class="audit-search" placeholder="Search actor, action, target, meta..." />
        <span class="audit-badge">Live audit trail</span>
      </div>
    </div>

    <div class="card" style="margin-top:16px">
      <table class="table audit-table" id="auditTable">
        <thead>
          <tr>
            <th style="width:90px">ID</th>
            <th style="width:160px">Actor</th>
            <th style="width:220px">Action</th>
            <th style="width:140px">Target</th>
            <th style="width:100px">Target ID</th>
            <th>Meta</th>
            <th style="width:160px">When</th>
          </tr>
        </thead>
        <tbody>
          {("".join(rows) if rows else "<tr><td colspan='7' class='muted' style='padding:18px'>No audit events yet.</td></tr>")}
        </tbody>
      </table>
    </div>
    <script>
      (function(){{
        const input = document.getElementById("auditSearch");
        const table = document.getElementById("auditTable");
        if(!input || !table) return;
        const rows = Array.from(table.querySelectorAll("tbody tr"));
        input.addEventListener("input", () => {{
          const q = input.value.toLowerCase().trim();
          rows.forEach(r => {{
            if(!q) {{ r.style.display = ""; return; }}
            const text = r.innerText.toLowerCase();
            r.style.display = text.includes(q) ? "" : "none";
          }});
        }});
      }})();
    </script>
    """
    return ui_shell("Audit Log", html_page, show_project_switcher=False)


@app.route("/auth/<provider>")
def auth_provider(provider):
    provider = (provider or "").strip().lower()
    if not oauth:
        return (
            f"<h2>OAuth not available</h2><p>OAuth dependencies are missing. Install the <code>requests</code> package to enable OAuth.</p><div class='muted'>{html.escape(_OAUTH_IMPORT_ERROR)}</div>",
            501,
        )
    oauth_intent = (request.args.get("intent") or "login").strip().lower()
    if oauth_intent not in ("login", "signup"):
        oauth_intent = "login"
    link_intent = request.args.get("link") == "1"
    user = getattr(g, "user", None)
    if link_intent and user and provider == "google":
        session["oauth_link_intent"] = "google"
        session["oauth_link_user_id"] = int(user.get("id"))
    elif not link_intent:
        session["oauth_intent"] = oauth_intent
    client = oauth.create_client(provider)
    if not client:
        env_hint = {
            "google": "OPENFIELD_GOOGLE_OAUTH_CLIENT_ID / OPENFIELD_GOOGLE_OAUTH_CLIENT_SECRET (or HURKFIELD_* equivalents)",
            "microsoft": "OPENFIELD_MICROSOFT_OAUTH_CLIENT_ID / OPENFIELD_MICROSOFT_OAUTH_CLIENT_SECRET (or HURKFIELD_* equivalents)",
            "linkedin": "OPENFIELD_LINKEDIN_OAUTH_CLIENT_ID / OPENFIELD_LINKEDIN_OAUTH_CLIENT_SECRET (or HURKFIELD_* equivalents)",
            "facebook": "OPENFIELD_FACEBOOK_OAUTH_CLIENT_ID / OPENFIELD_FACEBOOK_OAUTH_CLIENT_SECRET (or HURKFIELD_* equivalents)",
        }
        return (
            f"<h2>OAuth not configured</h2><p>{html.escape(provider.title())} sign-in is not configured yet. Set: <code>{html.escape(env_hint.get(provider, 'provider OAuth client ID/secret env vars'))}</code>.</p>",
            501,
        )
    redirect_uri = url_for("auth_callback", provider=provider, _external=True)
    try:
        return client.authorize_redirect(redirect_uri)
    except Exception as e:
        err_text = html.escape(str(e))
        hint = "Secure connection to the OAuth provider failed on this machine. Try again, switch networks, or update certificates."
        return (
            ui_shell(
                "OAuth Error",
                f"<div class='card' style='max-width:720px;margin:40px auto;'><h2 style='margin-top:0'>OAuth temporarily unavailable</h2><div class='muted'>{hint}</div><div class='muted' style='margin-top:10px'><code>{err_text}</code></div><div style='margin-top:14px'><a class='btn btn-primary' href='/login'>Back to sign in</a></div></div>",
                show_project_switcher=False,
                show_nav=False,
            ),
            502,
        )


@app.route("/auth/<provider>/callback")
def auth_callback(provider):
    provider = (provider or "").strip().lower()
    oauth_intent = (session.pop("oauth_intent", "") or "login").strip().lower()
    if oauth_intent not in ("login", "signup"):
        oauth_intent = "login"
    oauth_error = (request.args.get("error") or "").strip()
    if oauth_error:
        desc = html.escape(request.args.get("error_description") or oauth_error)
        back_url = "/signup" if oauth_intent == "signup" else "/login"
        back_text = "Back to sign up" if oauth_intent == "signup" else "Back to sign in"
        return (
            ui_shell(
                "OAuth Error",
                f"<div class='card' style='max-width:720px;margin:40px auto;'><h2 style='margin-top:0'>OAuth was cancelled or failed</h2><div class='muted'>{desc}</div><div style='margin-top:14px'><a class='btn btn-primary' href='{back_url}'>{back_text}</a></div></div>",
                show_project_switcher=False,
                show_nav=False,
            ),
            400,
        )
    if not oauth:
        return (
            f"<h2>OAuth not available</h2><p>OAuth dependencies are missing. Install the <code>requests</code> package to enable OAuth.</p><div class='muted'>{html.escape(_OAUTH_IMPORT_ERROR)}</div>",
            501,
        )
    client = oauth.create_client(provider)
    if not client:
        env_hint = {
            "google": "OPENFIELD_GOOGLE_OAUTH_CLIENT_ID / OPENFIELD_GOOGLE_OAUTH_CLIENT_SECRET (or HURKFIELD_* equivalents)",
            "microsoft": "OPENFIELD_MICROSOFT_OAUTH_CLIENT_ID / OPENFIELD_MICROSOFT_OAUTH_CLIENT_SECRET (or HURKFIELD_* equivalents)",
            "linkedin": "OPENFIELD_LINKEDIN_OAUTH_CLIENT_ID / OPENFIELD_LINKEDIN_OAUTH_CLIENT_SECRET (or HURKFIELD_* equivalents)",
            "facebook": "OPENFIELD_FACEBOOK_OAUTH_CLIENT_ID / OPENFIELD_FACEBOOK_OAUTH_CLIENT_SECRET (or HURKFIELD_* equivalents)",
        }
        return (
            f"<h2>OAuth not configured</h2><p>{html.escape(provider.title())} sign-in is not configured yet. Set: <code>{html.escape(env_hint.get(provider, 'provider OAuth client ID/secret env vars'))}</code>.</p>",
            501,
        )
    try:
        token = client.authorize_access_token()
    except Exception as e:
        err_text = html.escape(str(e))
        hint = (
            "OAuth callback failed. This is usually caused by a changing SECRET_KEY, "
            "a redirect URI mismatch, or stale cookies. Set OPENFIELD_SECRET_KEY in .env, "
            "make sure the Google redirect URI is exactly http://127.0.0.1:5000/auth/google/callback, "
            "then clear cookies and try again."
        )
        return (
            ui_shell(
                "OAuth Error",
                f"<div class='card' style='max-width:760px;margin:40px auto;'><h2 style='margin-top:0'>OAuth callback failed</h2><div class='muted'>{hint}</div><div class='muted' style='margin-top:10px'><code>{err_text}</code></div><div style='margin-top:14px'><a class='btn btn-primary' href='/login'>Back to sign in</a></div></div>",
                show_project_switcher=False,
                show_nav=False,
            ),
            400,
        )

    userinfo = {}
    try:
        if provider in ("google", "microsoft", "linkedin"):
            try:
                userinfo = client.parse_id_token(token)
            except Exception:
                userinfo = client.userinfo()
        elif provider == "facebook":
            resp = client.get("me?fields=id,name,email")
            userinfo = resp.json()
    except Exception:
        userinfo = {}

    email = (userinfo.get("email") or "").strip().lower()
    name = (userinfo.get("name") or userinfo.get("given_name") or "").strip()
    google_sub = (userinfo.get("sub") or "").strip() if provider == "google" else ""
    if not email:
        return (
            "<h2>OAuth failed</h2><p>Email permission not granted. Please allow email access or use email signup.</p>",
            400,
        )

    # Link Google to the currently signed-in user
    link_intent = session.get("oauth_link_intent") == "google" and provider == "google"
    link_uid = session.get("oauth_link_user_id")
    if link_intent and link_uid and getattr(g, "user", None):
        try:
            uid = int(link_uid)
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT * FROM users WHERE id=? LIMIT 1", (uid,))
                row = cur.fetchone()
                if row and google_sub:
                    u = dict(row)
                    if (u.get("email") or "").lower() == email:
                        cur.execute("SELECT password_hash, auth_provider FROM users WHERE id=? LIMIT 1", (uid,))
                        r2 = cur.fetchone()
                        has_pw = bool(r2 and r2["password_hash"])
                        current_provider = (r2["auth_provider"] if r2 else "local") or "local"
                        next_provider = "both" if has_pw else "google"
                        conn.execute(
                            "UPDATE users SET google_sub=?, auth_provider=?, updated_at=? WHERE id=?",
                            (google_sub, next_provider, now_iso(), uid),
                        )
                        conn.commit()
                        session["user_email"] = email
                        _log_security_event(uid, "OAUTH_LINKED", {"provider": "google"})
        except Exception:
            pass
        session.pop("oauth_link_intent", None)
        session.pop("oauth_link_user_id", None)
        return redirect(url_for("ui_settings_security"))

    # If user exists, sign in
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM users WHERE LOWER(email)=LOWER(?) LIMIT 1", (email,))
            row = cur.fetchone()
            if row:
                user = dict(row)
                status = (user.get("status") or "ACTIVE").upper()
                if status == "PENDING":
                    return redirect(url_for("ui_login") + "?pending=1")
                if status != "ACTIVE":
                    return redirect(url_for("ui_login"))
                if int(user.get("email_verified") or 0) != 1:
                    try:
                        with get_conn() as conn:
                            conn.execute("UPDATE users SET email_verified=1, verified_at=? WHERE id=?", (now_iso(), int(user.get("id"))))
                            conn.commit()
                    except Exception:
                        pass
                session["user_id"] = int(user.get("id"))
                session["org_id"] = user.get("organization_id")
                session["role"] = user.get("role")
                session["user_name"] = (user.get("full_name") or "").strip()
                session["user_email"] = (user.get("email") or "").strip().lower()
                session.permanent = True
                _create_session_record(int(user.get("id")))
                _log_security_event(int(user.get("id")), "LOGIN_SUCCESS", {"provider": provider})
                try:
                    with get_conn() as conn:
                        if provider == "google" and google_sub:
                            cur2 = conn.cursor()
                            cur2.execute("SELECT google_sub, password_hash FROM users WHERE id=? LIMIT 1", (int(user.get("id")),))
                            r2 = cur2.fetchone()
                            existing_sub = (r2["google_sub"] or "") if r2 else ""
                            has_pw = bool(r2 and r2["password_hash"])
                            if not existing_sub:
                                next_provider = "both" if has_pw else "google"
                                conn.execute(
                                    "UPDATE users SET google_sub=?, auth_provider=?, updated_at=? WHERE id=?",
                                    (google_sub, next_provider, now_iso(), int(user.get("id"))),
                                )
                        conn.execute("UPDATE users SET last_login_at=? WHERE id=?", (now_iso(), int(user.get("id"))))
                        conn.commit()
                except Exception:
                    pass
                return redirect(url_for("ui_dashboard"))
    except Exception:
        pass

    session["oauth_pending"] = {"provider": provider, "email": email, "name": name, "sub": google_sub}
    if oauth_intent == "login":
        return redirect(url_for("ui_signup") + "?oauth=1&from=login")
    return redirect(url_for("ui_signup") + "?oauth=1")


def require_admin():
    if PLATFORM_MODE:
        return bool(getattr(g, "user", None))
    if not ADMIN_KEY:
        return True
    key = request.args.get("key", "")
    return key == ADMIN_KEY


def supervisor_required() -> bool:
    return bool(REQUIRE_SUPERVISOR_KEY)


def supervisor_ok() -> bool:
    if not REQUIRE_SUPERVISOR_KEY:
        return True
    return bool(getattr(g, "supervisor", None) or getattr(g, "user", None))


def current_supervisor():
    return getattr(g, "supervisor", None)


def current_supervisor_id() -> Optional[int]:
    sup = current_supervisor()
    if not sup:
        return None
    try:
        return int(sup.get("id")) if sup.get("id") is not None else None
    except Exception:
        return None


def current_org_id():
    user = getattr(g, "user", None)
    if user:
        org_id = user.get("organization_id")
        try:
            return int(org_id) if org_id is not None else None
        except Exception:
            return None
    sup = current_supervisor()
    if not sup:
        return None
    org_id = sup.get("organization_id")
    try:
        return int(org_id) if org_id is not None else None
    except Exception:
        return None


def _scope_allows_project(project_id: int) -> bool:
    sup = current_supervisor()
    if not sup:
        return True
    try:
        sup_id = int(sup.get("id")) if sup.get("id") is not None else None
    except Exception:
        sup_id = None
    if sup_id is not None:
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    "PRAGMA table_info(enumerator_assignments)"
                )
                cols = [r["name"] for r in cur.fetchall()]
                if "supervisor_id" in cols:
                    cur.execute(
                        """
                        SELECT 1 FROM enumerator_assignments
                        WHERE supervisor_id=? AND project_id=?
                        LIMIT 1
                        """,
                        (int(sup_id), int(project_id)),
                    )
                    if not cur.fetchone():
                        return False
        except Exception:
            pass
    org_id = current_org_id()
    if not org_id:
        return True
    project = prj.get_project(int(project_id))
    if not project:
        return False
    proj_org = project.get("organization_id")
    if proj_org is None:
        return True
    try:
        return int(proj_org) == int(org_id)
    except Exception:
        return False


def _scope_allows_template(template_id: int) -> bool:
    cfg = tpl.get_template_config(int(template_id))
    if not cfg:
        return False
    pid = cfg.get("project_id")
    if not pid:
        # If template isn't linked, allow only in non-strict mode
        return not PROJECT_REQUIRED
    return _scope_allows_project(int(pid))


def _scope_allows_survey(survey_id: int) -> bool:
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT project_id, supervisor_id FROM surveys WHERE id=? LIMIT 1", (int(survey_id),))
            row = cur.fetchone()
            if not row:
                return False
            pid = row["project_id"]
            sup = current_supervisor()
            if sup and row.get("supervisor_id") is not None:
                try:
                    sup_id = int(sup.get("id")) if sup.get("id") is not None else None
                except Exception:
                    sup_id = None
                if sup_id is not None and int(row.get("supervisor_id") or 0) != int(sup_id):
                    return False
    except Exception:
        return True
    if not pid:
        return not PROJECT_REQUIRED
    return _scope_allows_project(int(pid))


def _scope_allows_assignment(assignment_id: int) -> bool:
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT project_id, supervisor_id FROM enumerator_assignments WHERE id=? LIMIT 1",
                (int(assignment_id),),
            )
            row = cur.fetchone()
            if not row:
                return False
            pid = row["project_id"]
            sup = current_supervisor()
            if sup and row.get("supervisor_id") is not None:
                try:
                    sup_id = int(sup.get("id")) if sup.get("id") is not None else None
                except Exception:
                    sup_id = None
                if sup_id is not None and int(row.get("supervisor_id") or 0) != int(sup_id):
                    return False
    except Exception:
        return True
    if not pid:
        return not PROJECT_REQUIRED
    return _scope_allows_project(int(pid))


def admin_gate(allow_supervisor: bool = True, enforce_project_scope: bool = True):
    if require_admin():
        return None

    if allow_supervisor and supervisor_ok():
        user = getattr(g, "user", None)
        if user and (user.get("role") or "").upper() == "ANALYST" and request.method != "GET":
            return ("Forbidden: read-only analyst role.", 403)
        if REQUIRE_SUPERVISOR_KEY and enforce_project_scope:
            try:
                view_args = request.view_args or {}
            except Exception:
                view_args = {}
            if "project_id" in view_args and not _scope_allows_project(int(view_args["project_id"])):
                return ("Forbidden: project not in supervisor scope.", 403)
            if "template_id" in view_args and not _scope_allows_template(int(view_args["template_id"])):
                return ("Forbidden: template not in supervisor scope.", 403)
            if "survey_id" in view_args and not _scope_allows_survey(int(view_args["survey_id"])):
                return ("Forbidden: survey not in supervisor scope.", 403)
            if "assignment_id" in view_args and not _scope_allows_assignment(int(view_args["assignment_id"])):
                return ("Forbidden: assignment not in supervisor scope.", 403)
            # project_id provided in query params
            qp = request.args.get("project_id")
            if qp and str(qp).isdigit() and not _scope_allows_project(int(qp)):
                return ("Forbidden: project not in supervisor scope.", 403)
        return None

    if REQUIRE_SUPERVISOR_KEY and allow_supervisor:
        next_url = request.full_path or "/ui"
        if PLATFORM_MODE:
            return redirect(url_for("ui_login") + f"?next={next_url}")
        return redirect(url_for("ui_access") + f"?next={next_url}")

    return (
        render_template_string(
            """
            <h2>Supervisor access protected</h2>
            <p>This instance requires an admin key to access supervisor pages.</p>
            <p>Append <code>?key=YOUR_KEY</code> to the URL.</p>
            """
        ),
        403,
    )


@app.route("/ui/access", methods=["GET", "POST"])
def ui_access():
    if not REQUIRE_SUPERVISOR_KEY:
        return redirect(url_for("ui_dashboard"))

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    next_url = request.values.get("next") or "/ui"
    err = ""
    if request.method == "POST":
        skey = (request.form.get("access_key") or "").strip()
        sup = prj.get_supervisor_by_key(skey)
        if not sup or (sup.get("status") or "ACTIVE").strip().upper() != "ACTIVE":
            err = "Invalid or inactive supervisor access key."
        else:
            resp = make_response(redirect(next_url))
            resp.set_cookie(
                SUPERVISOR_KEY_COOKIE,
                skey,
                httponly=True,
                samesite="Lax",
            )
            return resp

    html_page = f"""
    <div class="card" style="max-width:520px;margin:40px auto;">
      <h2 class="h2" style="margin-top:0">Supervisor access</h2>
      <div class="muted">Enter your supervisor access key to continue.</div>
      {f"<div class='card' style='border-color:rgba(231,76,60,.3);margin-top:12px'><b>Error:</b> {html.escape(err)}</div>" if err else ""}
      <form method="POST" style="margin-top:16px" class="stack">
        <input type="hidden" name="next" value="{html.escape(next_url)}" />
        <label style="font-weight:800">Access key</label>
        <input name="access_key" placeholder="e.g., sup_12345" required />
        <button class="btn btn-primary" type="submit">Continue</button>
        <a class="btn" href="/ui{key_q}">Back</a>
      </form>
    </div>
    """
    return ui_shell("Supervisor Access", html_page, show_project_switcher=False)


@app.route("/uploads/<path:filename>", methods=["GET"])
def serve_upload(filename):
    safe_name = secure_filename(filename)
    if not safe_name:
        return jsonify({"error": "File not found"}), 404
    path = os.path.abspath(os.path.join(UPLOAD_DIR, safe_name))
    base = os.path.abspath(UPLOAD_DIR)
    if not path.startswith(base + os.sep) or not os.path.isfile(path):
        return jsonify({"error": "File not found"}), 404
    return send_file(path)


def project_is_locked(project: dict) -> bool:
    return (project.get("status") or "").strip().upper() == "ARCHIVED"


def template_project_locked(template_id: int) -> bool:
    cfg = tpl.get_template_config(int(template_id))
    if not cfg:
        return False
    pid = cfg.get("project_id")
    if not pid:
        return False
    project = prj.get_project(int(pid))
    return project_is_locked(project) if project else False


def key_qs() -> str:
    return f"?key={ADMIN_KEY}" if ADMIN_KEY else ""


def resolve_project_context(project_id: int | None) -> int | None:
    if not PROJECT_REQUIRED:
        return project_id
    if project_id is not None:
        return project_id
    org_id = current_org_id()
    projects = prj.list_projects(200, organization_id=org_id)
    if len(projects) == 1:
        try:
            return int(projects[0].get("id"))
        except Exception:
            return None
    return None


def ensure_share_token(template_id: int) -> str:
    cfg = tpl.get_template_config(template_id)
    if not cfg:
        return ""
    token = cfg.get("share_token")
    if token:
        return token

    if "share_token" not in templates_cols():
        # Share link requires share_token column
        return ""

    token = secrets.token_urlsafe(16)
    tpl.set_template_config(template_id, share_token=token)
    return token


def share_path_for_template_row(template_row, token: str) -> str:
    project_id = row_get(template_row, "project_id")
    if project_id:
        return url_for("fill_form_project", project_id=int(project_id), token=token)
    return url_for("fill_form", token=token)


def get_template_by_token(token: str):
    if "share_token" not in templates_cols():
        return None
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM survey_templates WHERE share_token=? AND deleted_at IS NULL LIMIT 1",
            (token,),
        )
        return cur.fetchone()


def template_submissions_count(template_id: int) -> int:
    if "template_id" not in surveys_cols():
        return 0
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) AS c FROM surveys WHERE template_id=? AND deleted_at IS NULL",
            (int(template_id),),
        )
        r = cur.fetchone()
        return int(r["c"] or 0)


def get_or_create_facility_by_name(name: str) -> int:
    n = (name or "").strip()
    if not n:
        raise ValueError("Facility name is required.")

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM facilities WHERE LOWER(name)=LOWER(?) LIMIT 1", (n,))
        r = cur.fetchone()
        if r:
            return int(r["id"])

        if "created_at" in facilities_cols():
            cur.execute(
                "INSERT INTO facilities (name, created_at) VALUES (?, ?)", (n, now_iso()))
        else:
            cur.execute("INSERT INTO facilities (name) VALUES (?)", (n,))
        conn.commit()
        return int(cur.lastrowid)


def get_facility_name_by_id(facility_id: int) -> Optional[str]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM facilities WHERE id=? LIMIT 1", (int(facility_id),))
        row = cur.fetchone()
    return row["name"] if row else None


def q_choices(question_id: int):
    try:
        return tpl.list_choices(question_id)
    except Exception:
        return []


def _insert_survey_dynamic(
    facility_id: int,
    template_id: Optional[int],
    project_id: Optional[int],
    survey_type: str,
    enumerator_name: str,
    enumerator_code: Optional[str],
    respondent_email: Optional[str] = None,
    enumerator_id: Optional[int] = None,
    assignment_id: Optional[int] = None,
    supervisor_id: Optional[int] = None,
    gps: Optional[dict] = None,
    coverage_node_id: Optional[int] = None,
    coverage_node_name: Optional[str] = None,
    qa_flags: Optional[str] = None,
    gps_missing_flag: int = 0,
    duplicate_flag: int = 0,
    created_by: Optional[str] = None,
    source: str = "manual",
    consent_obtained: Optional[int] = None,
    consent_timestamp: Optional[str] = None,
    consent_signature: Optional[str] = None,
    consent_signature_ts: Optional[str] = None,
    attestation_text: Optional[str] = None,
    attestation_timestamp: Optional[str] = None,
    client_uuid: Optional[str] = None,
    client_created_at: Optional[str] = None,
    sync_source: Optional[str] = None,
    synced_at: Optional[str] = None,
) -> int:
    cols = surveys_cols()
    gps = gps or {}

    fields = []
    values = []

    if "facility_id" in cols:
        fields.append("facility_id")
        values.append(int(facility_id))

    if "template_id" in cols and template_id is not None:
        fields.append("template_id")
        values.append(int(template_id))

    if "project_id" in cols and project_id is not None:
        fields.append("project_id")
        values.append(int(project_id))

    if "survey_type" in cols:
        fields.append("survey_type")
        values.append(survey_type)

    if "enumerator_name" in cols:
        fields.append("enumerator_name")
        values.append(enumerator_name)

    if "enumerator_code" in cols:
        fields.append("enumerator_code")
        values.append(enumerator_code)

    if "respondent_email" in cols and respondent_email:
        fields.append("respondent_email")
        values.append(respondent_email)

    if "client_uuid" in cols and client_uuid:
        fields.append("client_uuid")
        values.append(client_uuid)
    if "client_created_at" in cols and client_created_at:
        fields.append("client_created_at")
        values.append(client_created_at)
    if "sync_source" in cols and sync_source:
        fields.append("sync_source")
        values.append(sync_source)
    if "synced_at" in cols and synced_at:
        fields.append("synced_at")
        values.append(synced_at)

    if "enumerator_id" in cols and enumerator_id is not None:
        fields.append("enumerator_id")
        values.append(int(enumerator_id))

    if "assignment_id" in cols and assignment_id is not None:
        fields.append("assignment_id")
        values.append(int(assignment_id))
    if "supervisor_id" in cols and supervisor_id is not None:
        fields.append("supervisor_id")
        values.append(int(supervisor_id))

    if "status" in cols:
        fields.append("status")
        values.append("DRAFT")

    if "created_at" in cols:
        fields.append("created_at")
        values.append(now_iso())
    if "created_by" in cols:
        fields.append("created_by")
        values.append((created_by or enumerator_name or "System").strip())
    if "updated_at" in cols:
        fields.append("updated_at")
        values.append(now_iso())
    if "source" in cols:
        fields.append("source")
        values.append((source or "manual").strip().lower())
    if "consent_obtained" in cols and consent_obtained is not None:
        fields.append("consent_obtained")
        values.append(int(consent_obtained))
    if "consent_timestamp" in cols and consent_timestamp is not None:
        fields.append("consent_timestamp")
        values.append(consent_timestamp)
    if "consent_signature" in cols and consent_signature is not None:
        fields.append("consent_signature")
        values.append(consent_signature)
    if "consent_signature_ts" in cols and consent_signature_ts is not None:
        fields.append("consent_signature_ts")
        values.append(consent_signature_ts)
    if "attestation_text" in cols and attestation_text is not None:
        fields.append("attestation_text")
        values.append(attestation_text)
    if "attestation_timestamp" in cols and attestation_timestamp is not None:
        fields.append("attestation_timestamp")
        values.append(attestation_timestamp)

    # GPS (only if columns exist)
    if "gps_lat" in cols:
        fields.append("gps_lat")
        values.append(gps.get("gps_lat"))
    if "gps_lng" in cols:
        fields.append("gps_lng")
        values.append(gps.get("gps_lng"))
    if "gps_accuracy" in cols:
        fields.append("gps_accuracy")
        values.append(gps.get("gps_accuracy"))
    if "gps_timestamp" in cols:
        fields.append("gps_timestamp")
        values.append(gps.get("gps_timestamp"))

    if "coverage_node_id" in cols and coverage_node_id is not None:
        fields.append("coverage_node_id")
        values.append(int(coverage_node_id))
    if "coverage_node_name" in cols and coverage_node_name is not None:
        fields.append("coverage_node_name")
        values.append(coverage_node_name)

    if "qa_flags" in cols and qa_flags is not None:
        fields.append("qa_flags")
        values.append(qa_flags)
    if "gps_missing_flag" in cols:
        fields.append("gps_missing_flag")
        values.append(int(gps_missing_flag or 0))
    if "duplicate_flag" in cols:
        fields.append("duplicate_flag")
        values.append(int(duplicate_flag or 0))

    if not fields:
        # should never happen in our schema, but safe
        raise RuntimeError("Surveys table does not have expected columns.")

    placeholders = ",".join(["?"] * len(fields))
    sql = f"INSERT INTO surveys ({', '.join(fields)}) VALUES ({placeholders})"

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, tuple(values))
        conn.commit()
        return int(cur.lastrowid)


def _update_survey_dynamic(
    survey_id: int,
    facility_id: int,
    template_id: Optional[int],
    project_id: Optional[int],
    survey_type: str,
    enumerator_name: str,
    enumerator_code: Optional[str],
    respondent_email: Optional[str] = None,
    enumerator_id: Optional[int] = None,
    assignment_id: Optional[int] = None,
    supervisor_id: Optional[int] = None,
    gps: Optional[dict] = None,
    coverage_node_id: Optional[int] = None,
    coverage_node_name: Optional[str] = None,
    qa_flags: Optional[str] = None,
    gps_missing_flag: int = 0,
    duplicate_flag: int = 0,
    created_by: Optional[str] = None,
    source: str = "edit",
    consent_obtained: Optional[int] = None,
    consent_timestamp: Optional[str] = None,
    consent_signature: Optional[str] = None,
    consent_signature_ts: Optional[str] = None,
    attestation_text: Optional[str] = None,
    attestation_timestamp: Optional[str] = None,
    client_uuid: Optional[str] = None,
    client_created_at: Optional[str] = None,
    sync_source: Optional[str] = None,
    synced_at: Optional[str] = None,
) -> None:
    cols = surveys_cols()
    gps = gps or {}

    fields = []
    values = []

    if "facility_id" in cols:
        fields.append("facility_id=?")
        values.append(int(facility_id))
    if "template_id" in cols and template_id is not None:
        fields.append("template_id=?")
        values.append(int(template_id))
    if "project_id" in cols and project_id is not None:
        fields.append("project_id=?")
        values.append(int(project_id))
    if "survey_type" in cols:
        fields.append("survey_type=?")
        values.append(survey_type)
    if "enumerator_name" in cols:
        fields.append("enumerator_name=?")
        values.append(enumerator_name)
    if "enumerator_code" in cols:
        fields.append("enumerator_code=?")
        values.append(enumerator_code)
    if "respondent_email" in cols:
        fields.append("respondent_email=?")
        values.append(respondent_email)
    if "client_uuid" in cols and client_uuid:
        fields.append("client_uuid=?")
        values.append(client_uuid)
    if "client_created_at" in cols and client_created_at:
        fields.append("client_created_at=?")
        values.append(client_created_at)
    if "sync_source" in cols and sync_source:
        fields.append("sync_source=?")
        values.append(sync_source)
    if "synced_at" in cols and synced_at:
        fields.append("synced_at=?")
        values.append(synced_at)
    if "enumerator_id" in cols and enumerator_id is not None:
        fields.append("enumerator_id=?")
        values.append(int(enumerator_id))
    if "assignment_id" in cols and assignment_id is not None:
        fields.append("assignment_id=?")
        values.append(int(assignment_id))
    if "supervisor_id" in cols and supervisor_id is not None:
        fields.append("supervisor_id=?")
        values.append(int(supervisor_id))

    if "updated_at" in cols:
        fields.append("updated_at=?")
        values.append(now_iso())
    if "source" in cols:
        fields.append("source=?")
        values.append((source or "edit").strip().lower())
    if "consent_obtained" in cols:
        fields.append("consent_obtained=?")
        values.append(int(consent_obtained) if consent_obtained is not None else None)
    if "consent_timestamp" in cols:
        fields.append("consent_timestamp=?")
        values.append(consent_timestamp)
    if "consent_signature" in cols:
        fields.append("consent_signature=?")
        values.append(consent_signature)
    if "consent_signature_ts" in cols:
        fields.append("consent_signature_ts=?")
        values.append(consent_signature_ts)
    if "attestation_text" in cols:
        fields.append("attestation_text=?")
        values.append(attestation_text)
    if "attestation_timestamp" in cols:
        fields.append("attestation_timestamp=?")
        values.append(attestation_timestamp)

    if "gps_lat" in cols:
        fields.append("gps_lat=?")
        values.append(gps.get("gps_lat"))
    if "gps_lng" in cols:
        fields.append("gps_lng=?")
        values.append(gps.get("gps_lng"))
    if "gps_accuracy" in cols:
        fields.append("gps_accuracy=?")
        values.append(gps.get("gps_accuracy"))
    if "gps_timestamp" in cols:
        fields.append("gps_timestamp=?")
        values.append(gps.get("gps_timestamp"))
    if "coverage_node_id" in cols:
        fields.append("coverage_node_id=?")
        values.append(int(coverage_node_id) if coverage_node_id is not None else None)
    if "coverage_node_name" in cols:
        fields.append("coverage_node_name=?")
        values.append(coverage_node_name)
    if "qa_flags" in cols:
        fields.append("qa_flags=?")
        values.append(qa_flags)
    if "gps_missing_flag" in cols:
        fields.append("gps_missing_flag=?")
        values.append(int(gps_missing_flag or 0))
    if "duplicate_flag" in cols:
        fields.append("duplicate_flag=?")
        values.append(int(duplicate_flag or 0))

    if not fields:
        return

    with get_conn() as conn:
        conn.execute(
            f"UPDATE surveys SET {', '.join(fields)} WHERE id=?",
            (*values, int(survey_id)),
        )
        conn.commit()

def _insert_answer_dynamic(
    survey_id: int,
    template_question_id: Optional[int],
    question_text: str,
    answer_value: str,
    answer_source: Optional[str] = None,
) -> None:
    cols = answers_cols()

    fields = []
    values = []

    if "survey_id" in cols:
        fields.append("survey_id")
        values.append(int(survey_id))

    if "template_question_id" in cols and template_question_id is not None:
        fields.append("template_question_id")
        values.append(int(template_question_id))

    if "question" in cols:
        fields.append("question")
        values.append(question_text)

    if "answer" in cols:
        fields.append("answer")
        values.append(answer_value)

    if "answer_source" in cols and answer_source:
        fields.append("answer_source")
        values.append(answer_source)

    if "created_at" in cols:
        fields.append("created_at")
        values.append(now_iso())

    placeholders = ",".join(["?"] * len(fields))
    sql = f"INSERT INTO survey_answers ({', '.join(fields)}) VALUES ({placeholders})"

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, tuple(values))
        conn.commit()


def _complete_survey(survey_id: int):
    cols = surveys_cols()
    with get_conn() as conn:
        cur = conn.cursor()
        if "completed_at" in cols:
            if "updated_at" in cols:
                cur.execute(
                    "UPDATE surveys SET status='COMPLETED', completed_at=?, updated_at=? WHERE id=?",
                    (now_iso(), now_iso(), int(survey_id)),
                )
            else:
                cur.execute(
                    "UPDATE surveys SET status='COMPLETED', completed_at=? WHERE id=?",
                    (now_iso(), int(survey_id)),
                )
        else:
            if "updated_at" in cols:
                cur.execute(
                    "UPDATE surveys SET status='COMPLETED', updated_at=? WHERE id=?",
                    (now_iso(), int(survey_id)),
                )
            else:
                cur.execute(
                    "UPDATE surveys SET status='COMPLETED' WHERE id=?", (int(survey_id),))
        if "review_status" in cols:
            cur.execute(
                "UPDATE surveys SET review_status=COALESCE(review_status,'PENDING') WHERE id=?",
                (int(survey_id),),
            )
        conn.commit()


def save_survey_from_share_link(template_row, form, existing_survey_id: Optional[int] = None) -> int:
    template_id = int(template_row["id"]
                      ) if "id" in template_row.keys() else None
    template_name = row_get(template_row, "name", "Survey") or "Survey"
    project_id = row_get(template_row, "project_id")

    require_enum_code = int(
        row_get(template_row, "require_enumerator_code", 0) or 0)
    enable_gps = int(row_get(template_row, "enable_gps", 0) or 0)
    enable_coverage = int(row_get(template_row, "enable_coverage", 0) or 0)
    coverage_scheme_id = row_get(template_row, "coverage_scheme_id")
    enable_consent = int(row_get(template_row, "enable_consent", 0) or 0)
    enable_attestation = int(row_get(template_row, "enable_attestation", 0) or 0)
    collect_email = int(row_get(template_row, "collect_email", 0) or 0)
    limit_one_response = int(row_get(template_row, "limit_one_response", 0) or 0)

    facility_name = (form.get("facility_name") or "").strip()
    facility_id_raw = (form.get("facility_id") or "").strip()
    facility_id_selected = int(facility_id_raw) if str(facility_id_raw).isdigit() else None
    enumerator_name = (form.get("enumerator_name") or "").strip()
    enumerator_code = (form.get("enumerator_code") or "").strip()
    respondent_email = (form.get("respondent_email") or "").strip().lower()
    client_uuid = (form.get("client_uuid") or "").strip()
    client_created_at = (form.get("client_created_at") or "").strip()
    sync_source = (form.get("sync_source") or "").strip().upper()
    assignment_id = (form.get("assign_id") or "").strip()
    assignment_id = int(assignment_id) if str(assignment_id).isdigit() else None
    coverage_node_id = (form.get("coverage_node_id") or "").strip()
    coverage_node_id = int(coverage_node_id) if str(coverage_node_id).isdigit() else None
    consent_value = (form.get("consent_obtained") or "").strip().upper()
    consent_signature = (form.get("consent_signature") or "").strip()
    attestation_confirm = (form.get("attestation_confirm") or "").strip().lower()

    is_offline_sync = sync_source == "OFFLINE_SYNC"

    # Idempotent: if client_uuid already exists, return existing survey id
    if client_uuid and not existing_survey_id and "client_uuid" in surveys_cols():
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id FROM surveys WHERE client_uuid=? LIMIT 1", (client_uuid,))
                r = cur.fetchone()
                if r:
                    return int(r["id"])
        except Exception:
            pass

    assignment = enum.get_assignment(assignment_id) if assignment_id else None
    enumerator_id = None
    assigned_enumerator = None
    prior_facility_id = None
    prior_assignment_id = None
    if existing_survey_id:
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT facility_id, assignment_id FROM surveys WHERE id=? LIMIT 1",
                    (int(existing_survey_id),),
                )
                r = cur.fetchone()
                if r:
                    prior_facility_id = r["facility_id"]
                    prior_assignment_id = r["assignment_id"]
        except Exception:
            prior_facility_id = None
            prior_assignment_id = None

    if assignment:
        assigned_enumerator = enum.get_enumerator(int(assignment.get("enumerator_id")))
        if assigned_enumerator:
            enumerator_id = int(assigned_enumerator.get("id"))
            enumerator_name = assigned_enumerator.get("name") or enumerator_name
            enumerator_code = assigned_enumerator.get("code") or enumerator_code
        if assigned_enumerator and not _enumerator_is_active(assigned_enumerator):
            raise ValueError("Enumerator is inactive. Contact your supervisor.")
        if assignment and not _assignment_is_active(assignment):
            raise ValueError("Assignment is inactive. Contact your supervisor.")

    if not assignment and enumerator_code and project_id:
        candidate = enum.get_enumerator_by_code(int(project_id), enumerator_code)
        if candidate and _enumerator_is_active(candidate):
            enumerator_id = int(candidate.get("id"))
            assigned_enumerator = candidate
            enumerator_name = candidate.get("name") or enumerator_name
            enumerator_code = candidate.get("code") or enumerator_code
            assignment = enum.get_assignment_for_enumerator(
                int(project_id), enumerator_id, template_id=int(template_id) if template_id else None
            )
            assignment_id = int(assignment.get("id")) if assignment else assignment_id
            if assignment and not _assignment_is_active(assignment):
                raise ValueError("Assignment is inactive. Contact your supervisor.")

    assignment_mode = "OPTIONAL"
    template_mode = (row_get(template_row, "assignment_mode") or "INHERIT").strip().upper()
    if template_mode and template_mode != "INHERIT":
        assignment_mode = template_mode
    elif project_id:
        project = prj.get_project(int(project_id))
        assignment_mode = (project.get("assignment_mode") or "OPTIONAL").strip().upper() if project else "OPTIONAL"

    if assignment_mode in ("REQUIRED_PROJECT", "REQUIRED_TEMPLATE") and not assignment:
        raise ValueError("Assignment required. Please use your supervisor's link.")
    if assignment_mode == "REQUIRED_TEMPLATE" and assignment:
        if assignment.get("template_id") and int(assignment.get("template_id")) != int(template_id):
            raise ValueError("Assignment is not valid for this form.")

    if require_enum_code and not enumerator_code:
        raise ValueError("Enumerator code is required for this template.")
    if not enumerator_name:
        raise ValueError("Enumerator name is required.")
    if collect_email:
        if not respondent_email or "@" not in respondent_email or "." not in respondent_email:
            raise ValueError("A valid email is required for this form.")
        if limit_one_response and "respondent_email" in surveys_cols():
            try:
                with get_conn() as conn:
                    cur = conn.cursor()
                    if existing_survey_id:
                        cur.execute(
                            """
                            SELECT COUNT(*) AS c
                            FROM surveys
                            WHERE template_id=?
                              AND LOWER(COALESCE(respondent_email,''))=LOWER(?)
                              AND id<>?
                            """,
                            (int(template_id), respondent_email, int(existing_survey_id)),
                        )
                    else:
                        cur.execute(
                            """
                            SELECT COUNT(*) AS c
                            FROM surveys
                            WHERE template_id=? AND LOWER(COALESCE(respondent_email,''))=LOWER(?)
                            """,
                            (int(template_id), respondent_email),
                        )
                    if int(cur.fetchone()["c"] or 0) > 0:
                        raise ValueError("This email has already submitted a response.")
            except ValueError:
                raise
            except Exception:
                pass
    if enable_coverage and coverage_scheme_id and not coverage_node_id:
        raise ValueError("Coverage selection is required for this template.")
    if enable_consent and consent_value not in ("YES", "NO"):
        raise ValueError("Consent selection is required for this template.")
    if enable_consent and consent_value == "YES" and not consent_signature:
        raise ValueError("Signature is required to confirm consent.")
    if enable_attestation and attestation_confirm != "on":
        raise ValueError("Enumerator attestation is required for this template.")

    consent_obtained = None
    consent_timestamp = None
    consent_signature_ts = None
    if enable_consent and consent_value in ("YES", "NO"):
        consent_obtained = 1 if consent_value == "YES" else 0
        consent_timestamp = now_iso()
        if consent_signature:
            consent_signature_ts = now_iso()

    attestation_text = None
    attestation_timestamp = None
    if enable_attestation and attestation_confirm == "on":
        attestation_text = (form.get("attestation_text") or "").strip() or "I confirm this submission is accurate and collected ethically."
        attestation_timestamp = now_iso()

    assigned_facilities = []
    if assignment:
        assigned_facilities = enum.list_assignment_facilities(int(assignment.get("id")))

    if assigned_facilities:
        if not facility_id_selected:
            raise ValueError("Facility selection is required for this assignment.")
        allowed_ids = {int(f.get("facility_id")) for f in assigned_facilities if f.get("facility_id")}
        if int(facility_id_selected) not in allowed_ids:
            raise ValueError("Selected facility is not part of your assigned list.")
        facility_id = int(facility_id_selected)
        facility_name = get_facility_name_by_id(facility_id) or facility_name
    else:
        allow_unlisted = 0
        if project_id:
            project = prj.get_project(int(project_id))
            allow_unlisted = int(project.get("allow_unlisted_facilities") or 0) if project else 0
        if assignment and not allow_unlisted:
            raise ValueError("Facility list not configured for this assignment. Contact your supervisor.")
        if not facility_name:
            raise ValueError("Facility name is required.")
        facility_id = get_or_create_facility_by_name(facility_name)

    gps = {"gps_lat": None, "gps_lng": None,
           "gps_accuracy": None, "gps_timestamp": None}
    if enable_gps:
        try:
            gps["gps_lat"] = float(form.get("gps_lat")) if form.get(
                "gps_lat") else None
            gps["gps_lng"] = float(form.get("gps_lng")) if form.get(
                "gps_lng") else None
            gps["gps_accuracy"] = float(form.get("gps_accuracy")) if form.get(
                "gps_accuracy") else None
            gps["gps_timestamp"] = form.get(
                "gps_timestamp") if form.get("gps_timestamp") else None
        except Exception:
            pass

    gps_missing_flag = 1 if enable_gps and (gps.get("gps_lat") is None or gps.get("gps_lng") is None) else 0
    duplicate_flag = 0
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT COUNT(*) AS c
                FROM surveys
                WHERE facility_id=? AND COALESCE(enumerator_name,'')=?
                  AND date(created_at)=date(?)
                """,
                (int(facility_id), enumerator_name, now_iso()),
            )
            duplicate_flag = 1 if int(cur.fetchone()["c"] or 0) > 0 else 0
    except Exception:
        duplicate_flag = 0

    supervisor_id = None
    if assignment and assignment.get("supervisor_id"):
        try:
            supervisor_id = int(assignment.get("supervisor_id"))
        except Exception:
            supervisor_id = None
    if assignment and assignment.get("coverage_node_id"):
        coverage_node_id = int(assignment.get("coverage_node_id"))

    coverage_node_name = None
    if coverage_node_id:
        node = cov.get_node(int(coverage_node_id))
        coverage_node_name = node.get("name") if node else None

    qa_flags = []
    if gps_missing_flag:
        qa_flags.append("GPS_MISSING")
    if duplicate_flag:
        qa_flags.append("DUPLICATE_FACILITY_DAY")
    qa_flags_str = ",".join(qa_flags) if qa_flags else None

    if existing_survey_id:
        survey_id = int(existing_survey_id)
        source_value = "offline_sync" if is_offline_sync else "edit"
        sync_source_value = "OFFLINE_SYNC" if is_offline_sync else None
        synced_at_value = now_iso() if is_offline_sync else None
        _update_survey_dynamic(
            survey_id=survey_id,
            facility_id=facility_id,
            template_id=template_id,
            project_id=(int(project_id) if project_id else None),
            survey_type=template_name,
            enumerator_name=enumerator_name,
            enumerator_code=(enumerator_code if enumerator_code else None),
            respondent_email=respondent_email or None,
            enumerator_id=(int(enumerator_id) if enumerator_id is not None else None),
            assignment_id=(int(assignment_id) if assignment_id is not None else None),
            supervisor_id=supervisor_id,
            gps=gps,
            coverage_node_id=coverage_node_id,
            coverage_node_name=coverage_node_name,
            qa_flags=qa_flags_str,
            gps_missing_flag=gps_missing_flag,
            duplicate_flag=duplicate_flag,
            created_by=enumerator_name,
            source=source_value,
            consent_obtained=consent_obtained,
            consent_timestamp=consent_timestamp,
            consent_signature=consent_signature or None,
            consent_signature_ts=consent_signature_ts,
            attestation_text=attestation_text,
            attestation_timestamp=attestation_timestamp,
            client_uuid=client_uuid or None,
            client_created_at=client_created_at or None,
            sync_source=sync_source_value,
            synced_at=synced_at_value,
        )
    else:
        source_value = "offline_sync" if is_offline_sync else "share-link"
        sync_source_value = "OFFLINE_SYNC" if is_offline_sync else None
        synced_at_value = now_iso() if is_offline_sync else None
        survey_id = _insert_survey_dynamic(
            facility_id=facility_id,
            template_id=template_id,
            project_id=(int(project_id) if project_id else None),
            survey_type=template_name,
            enumerator_name=enumerator_name,
            enumerator_code=(enumerator_code if enumerator_code else None),
            respondent_email=respondent_email or None,
            enumerator_id=(int(enumerator_id) if enumerator_id is not None else None),
            assignment_id=(int(assignment_id) if assignment_id is not None else None),
            supervisor_id=supervisor_id,
            gps=gps,
            coverage_node_id=coverage_node_id,
            coverage_node_name=coverage_node_name,
            qa_flags=qa_flags_str,
            gps_missing_flag=gps_missing_flag,
            duplicate_flag=duplicate_flag,
            created_by=enumerator_name,
            source=source_value,
            consent_obtained=consent_obtained,
            consent_timestamp=consent_timestamp,
            consent_signature=consent_signature or None,
            consent_signature_ts=consent_signature_ts,
            attestation_text=attestation_text,
            attestation_timestamp=attestation_timestamp,
            client_uuid=client_uuid or None,
            client_created_at=client_created_at or None,
            sync_source=sync_source_value,
            synced_at=synced_at_value,
        )

    questions = tpl.get_template_questions(int(template_id))
    missing_required = []

    validation_errors = []
    answer_rows = []
    for row in questions:
        qid = row[0]
        qtext = row[1]
        qtype = row[2]
        is_required = row[4] if len(row) > 4 else 0
        vjson = row[6] if len(row) > 6 else None
        validation = _parse_validation_json(vjson)
        field = f"q_{qid}"
        qtype = (qtype or "TEXT").upper()

        if qtype == "MULTI_CHOICE":
            values = form.getlist(field) if hasattr(form, "getlist") else []
            answer = ", ".join([v.strip() for v in values if v.strip()])
        else:
            answer = (form.get(field) or "").strip()

        if int(is_required) == 1 and not answer:
            missing_required.append(qtext)
        if answer:
            choices = None
            if qtype in ("SINGLE_CHOICE", "DROPDOWN", "MULTI_CHOICE"):
                choices = [c[2] for c in q_choices(qid)]
            err = _validate_answer(qtype, answer, validation, choices=choices)
            if err:
                validation_errors.append(f"{qtext}: {err}")

        if answer:
            answer_rows.append((qid, qtext, answer))

    if missing_required:
        raise ValueError("Required questions missing: " +
                         "; ".join(missing_required))
    if validation_errors:
        raise ValueError("Validation errors: " + "; ".join(validation_errors))

    if existing_survey_id:
        try:
            with get_conn() as conn:
                conn.execute("DELETE FROM survey_answers WHERE survey_id=?", (int(survey_id),))
                conn.commit()
        except Exception:
            pass

    answer_source_value = "OFFLINE_SYNC" if is_offline_sync else None
    for qid, qtext, answer in answer_rows:
        _insert_answer_dynamic(
            survey_id=survey_id,
            template_question_id=int(qid) if "template_question_id" in answers_cols() else None,
            question_text=qtext,
            answer_value=answer,
            answer_source=answer_source_value,
        )

    _complete_survey(survey_id)
    if assignment and facility_id_selected:
        try:
            if existing_survey_id and prior_facility_id and int(prior_facility_id) != int(facility_id_selected):
                with get_conn() as conn:
                    conn.execute(
                        """
                        UPDATE assignment_facilities
                        SET status='PENDING', done_survey_id=NULL
                        WHERE assignment_id=? AND facility_id=? AND done_survey_id=?
                        """,
                        (int(assignment.get("id")), int(prior_facility_id), int(survey_id)),
                    )
                    conn.commit()
            enum.mark_assignment_facility_done(int(assignment.get("id")), int(facility_id_selected), int(survey_id))
        except Exception:
            pass
    return survey_id


# ---------------------------
# Landing Page
# ---------------------------
@app.route("/")
def landing():
    if getattr(g, "user", None):
        return redirect(url_for("ui_dashboard"))
    demo_url = None

    # Try to find one active template for demo
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, share_token
            FROM survey_templates
            WHERE COALESCE(is_active, 1) = 1
            ORDER BY id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()

    if row:
        tid = int(row["id"])
        token = row["share_token"]
        if not token:
            token = ensure_share_token(tid)
        project_id = row_get(row, "project_id")
        if project_id:
            demo_url = request.host_url.rstrip("/") + url_for(
                "fill_form_project", project_id=int(project_id), token=token
            )
        else:
            demo_url = request.host_url.rstrip("/") + url_for(
                "fill_form", token=token)
    else:
        # If no template exists yet, send user to dashboard
        demo_url = url_for("ui_home")

    env_label = "LIVE" if APP_ENV in ("production", "live") else ("PILOT" if APP_ENV == "pilot" else "DEVELOPMENT")
    env_badge = f"Environment: {env_label}"
    subscribed = request.args.get("subscribed") == "1"
    sub_error = request.args.get("sub_error") or ""
    show_dashboard = bool(getattr(g, "user", None))
    return render_template(
        "landing.html",
        demo_url=demo_url,
        env_badge=env_badge,
        app_version=APP_VERSION,
        subscribed=subscribed,
        sub_error=sub_error,
        show_dashboard=show_dashboard,
    )


@app.route("/about")
def about_page():
    return render_template_string(
        """
        <html>
        <head>
          <meta name="viewport" content="width=device-width, initial-scale=1"/>
          <link rel="preconnect" href="https://fonts.googleapis.com">
          <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
          <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Poppins:wght@400;500;600;700;800&display=swap" rel="stylesheet">
          <style>
            :root{
              --font-heading:"Poppins", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
              --font-body:"Inter", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
              --text:#0f1222;
              --muted:#6b7280;
            }
            body{margin:0; font-family:var(--font-body); color:var(--text); background:#ffffff;}
            h1{font-family:var(--font-heading); margin:0 0 10px;}
          </style>
        </head>
        <body>
          <div style="max-width:820px; margin:48px auto; padding:0 20px;">
            <h1>About HurkField</h1>
            <p>HurkField is a research operations platform for field data collection.</p>
            <p>It helps teams design structured forms, coordinate enumerators, and review submissions with clear QA visibility.</p>
            <p>Built for NGOs, academic research, and institutional studies.</p>
            <p style="margin-top:24px; color:var(--muted);">{{version}}</p>
          </div>
        </body>
        </html>
        """,
        version=APP_VERSION,
    )


@app.route("/newsletter/subscribe", methods=["POST"])
def newsletter_subscribe():
    email = (request.form.get("email") or "").strip().lower()
    source = (request.form.get("source") or "landing").strip()
    redirect_to = request.form.get("redirect_to") or url_for("landing")

    if not email or "@" not in email or "." not in email:
        return redirect(f"{redirect_to}?sub_error=Please%20enter%20a%20valid%20email")

    with get_conn() as conn:
        conn.execute(
            "INSERT INTO newsletter_subscribers (email, source, created_at) VALUES (?, ?, ?)",
            (email, source, datetime.now().isoformat(timespec="seconds")),
        )
        conn.commit()

    return redirect(f"{redirect_to}?subscribed=1")


@app.route("/privacy")
def privacy_page():
    return render_template_string(
        """
        <html>
        <head>
          <meta name="viewport" content="width=device-width, initial-scale=1"/>
          <link rel="preconnect" href="https://fonts.googleapis.com">
          <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
          <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Poppins:wght@400;500;600;700;800&display=swap" rel="stylesheet">
          <style>
            :root{
              --font-heading:"Poppins", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
              --font-body:"Inter", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
              --text:#0f1222;
              --muted:#6b7280;
            }
            body{margin:0; font-family:var(--font-body); color:var(--text); background:#ffffff;}
            h1{font-family:var(--font-heading); margin:0 0 10px;}
          </style>
        </head>
        <body>
          <div style="max-width:820px; margin:48px auto; padding:0 20px;">
            <h1>Privacy Notice</h1>
            <p>HurkField stores the data collected by supervisors and enumerators for their projects.</p>
            <p>Data is not sold and remains under the control of the supervising organization.</p>
            <p>Enumerators submit data on behalf of their project supervisors. Supervisors are responsible for consent and ethical handling.</p>
            <p style="margin-top:24px; color:var(--muted);">{{version}}</p>
          </div>
        </body>
        </html>
        """,
        version=APP_VERSION,
    )


@app.route("/terms")
def terms_page():
    return render_template_string(
        """
        <html>
        <head>
          <meta name="viewport" content="width=device-width, initial-scale=1"/>
          <link rel="preconnect" href="https://fonts.googleapis.com">
          <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
          <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Poppins:wght@400;500;600;700;800&display=swap" rel="stylesheet">
          <style>
            :root{
              --font-heading:"Poppins", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
              --font-body:"Inter", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
              --text:#0f1222;
              --muted:#6b7280;
            }
            body{margin:0; font-family:var(--font-body); color:var(--text); background:#ffffff;}
            h1{font-family:var(--font-heading); margin:0 0 10px;}
          </style>
        </head>
        <body>
          <div style="max-width:820px; margin:48px auto; padding:0 20px;">
            <h1>Terms of Use</h1>
            <p>HurkField provides tools for field data collection and supervision.</p>
            <p>Organizations are responsible for lawful data collection, consent, and compliance.</p>
            <p>Use of the service indicates acceptance of these terms.</p>
          </div>
        </body>
        </html>
        """
    )


@app.route("/how-it-works")
def how_it_works_page():
    return render_template_string(
        """
        <html>
        <head>
          <meta name="viewport" content="width=device-width, initial-scale=1"/>
          <link rel="preconnect" href="https://fonts.googleapis.com">
          <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
          <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Poppins:wght@400;500;600;700;800&display=swap" rel="stylesheet">
          <style>
            :root{
              --font-heading:"Poppins", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
              --font-body:"Inter", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
              --text:#0f1222;
              --muted:#6b7280;
            }
            body{margin:0; font-family:var(--font-body); color:var(--text); background:#ffffff;}
            h1{font-family:var(--font-heading); margin:0 0 10px;}
            .step{margin:16px 0}
          </style>
        </head>
        <body>
          <div style="max-width:820px; margin:48px auto; padding:0 20px;">
            <h1>How HurkField Works</h1>
            <div class="step"><b>1. Create a project</b><div class="muted">Define the study scope and expected submissions.</div></div>
            <div class="step"><b>2. Build a template</b><div class="muted">Add questions, validation, and GPS if needed.</div></div>
            <div class="step"><b>3. Share links</b><div class="muted">Send links or QR codes to enumerators.</div></div>
            <div class="step"><b>4. Collect data</b><div class="muted">Drafts, resume, and submit in the field.</div></div>
            <div class="step"><b>5. Review QA</b><div class="muted">Check flags and anomalies before reporting.</div></div>
            <div class="step"><b>6. Export</b><div class="muted">Download clean CSV/JSON datasets.</div></div>
          </div>
        </body>
        </html>
        """
    )


@app.route("/for-institutions")
def for_institutions_page():
    return render_template_string(
        """
        <html>
        <head>
          <meta name="viewport" content="width=device-width, initial-scale=1"/>
          <link rel="preconnect" href="https://fonts.googleapis.com">
          <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
          <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Poppins:wght@400;500;600;700;800&display=swap" rel="stylesheet">
          <style>
            :root{
              --font-heading:"Poppins", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
              --font-body:"Inter", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
              --text:#0f1222;
              --muted:#6b7280;
            }
            body{margin:0; font-family:var(--font-body); color:var(--text); background:#ffffff;}
            h1{font-family:var(--font-heading); margin:0 0 10px;}
          </style>
        </head>
        <body>
          <div style="max-width:820px; margin:48px auto; padding:0 20px;">
            <h1>For Institutions</h1>
            <p>HurkField helps NGOs, governments, and universities run reliable field research at scale.</p>
            <ul>
              <li>Audit-ready data with QA visibility</li>
              <li>Enumerator oversight and coverage tracking</li>
              <li>CSV/JSON exports for reporting</li>
            </ul>
          </div>
        </body>
        </html>
        """
    )


# ---------------------------
# API (MVP)
# ---------------------------
@app.route("/api")
def api_root():
    return jsonify(
        {
            "name": "HurkField Collect API",
            "ui": "/ui",
            "endpoints": [
                "GET /facilities",
                "POST /facilities",
                "GET /facilities/<id>",
                "GET /surveys?status=&enumerator=&template_id=",
                "GET /surveys/<id>",
                "GET /qa/alerts",
                "GET /api/v1/facilities",
                "GET /api/v1/surveys",
                "GET /api/v1/qa/alerts",
                "GET /api/v1/projects",
            ],
        }
    )


@app.route("/api/v1/facilities", methods=["GET"])
def api_v1_facilities():
    project_id = request.args.get("project_id") or ""
    project_id = int(project_id) if str(project_id).isdigit() else None
    params = []
    where = []
    if project_id is not None:
        where.append("s.project_id=?")
        params.append(int(project_id))
    if "deleted_at" in surveys_cols():
        where.append("s.deleted_at IS NULL")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT DISTINCT f.id, f.name
            FROM facilities f
            LEFT JOIN surveys s ON s.facility_id = f.id
            {where_sql}
            ORDER BY f.name ASC
            LIMIT 200
            """,
            tuple(params),
        )
        rows = cur.fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/v1/surveys", methods=["GET"])
def api_v1_surveys():
    status = request.args.get("status", "")
    enumerator = request.args.get("enumerator", "")
    template_id = request.args.get("template_id", "")
    project_id = request.args.get("project_id", "")
    sup_id = current_supervisor_id()
    rows = sup.filter_surveys(
        status=status,
        enumerator=enumerator,
        template_id=template_id,
        project_id=project_id,
        supervisor_id=str(sup_id) if sup_id else "",
        limit=200,
    )
    out = []
    for (sid, facility_name, tplid, survey_type, enum, st, created_at) in rows:
        out.append(
            {
                "id": sid,
                "facility_name": facility_name,
                "template_id": tplid,
                "survey_type": survey_type,
                "enumerator_name": enum,
                "status": st,
                "created_at": created_at,
            }
        )
    return jsonify(out)


@app.route("/api/v1/qa/alerts", methods=["GET"])
def api_v1_qa_alerts():
    project_id = request.args.get("project_id") or ""
    sup_id = current_supervisor_id()
    alerts = sup.qa_alerts_dashboard(limit=100, project_id=project_id, supervisor_id=str(sup_id) if sup_id else "")
    return jsonify([a.__dict__ for a in alerts])


@app.route("/api/v1/projects", methods=["GET"])
def api_v1_projects():
    is_admin = bool(ADMIN_KEY and request.args.get("key") == ADMIN_KEY)
    org_id = current_org_id() if (REQUIRE_SUPERVISOR_KEY and not is_admin) else None
    projects = prj.list_projects(200, organization_id=org_id)
    orgs = prj.list_organizations(200)
    org_map = {int(o.get("id")): o.get("name") for o in orgs}
    org_filter = request.args.get("org_id") or ""
    org_filter = int(org_filter) if str(org_filter).isdigit() else None
    if org_id is not None:
        org_filter = org_id
    out = []
    for p in projects:
        pid = int(p["id"])
        overview = prj.project_overview(pid)
        out.append(
            {
                "id": pid,
                "name": p.get("name"),
                "status": p.get("status"),
                "assignment_mode": p.get("assignment_mode"),
                "is_test_project": int(p.get("is_test_project") or 0),
                "is_live_project": int(p.get("is_live_project") or 0),
                "created_at": p.get("created_at"),
                "metrics": overview,
            }
        )
    return jsonify(out)


@app.route("/facilities", methods=["GET", "POST"])
def facilities():
    if request.method == "POST":
        data = request.get_json(force=True, silent=True) or {}
        name = (data.get("name") or "").strip()
        if not name:
            return jsonify({"error": "name is required"}), 400
        fid = get_or_create_facility_by_name(name)
        return jsonify({"id": fid, "name": name}), 201

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name FROM facilities ORDER BY id DESC LIMIT 200")
        rows = cur.fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/facilities/<int:fid>", methods=["GET"])
def facility_one(fid):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM facilities WHERE id=? LIMIT 1", (int(fid),))
        r = cur.fetchone()
    if not r:
        return jsonify({"error": "not found"}), 404
    return jsonify(dict(r))


@app.route("/surveys", methods=["GET"])
def surveys():
    status = request.args.get("status", "")
    enumerator = request.args.get("enumerator", "")
    template_id = request.args.get("template_id", "")
    project_id = request.args.get("project_id", "")
    sup_id = current_supervisor_id()
    rows = sup.filter_surveys(
        status=status,
        enumerator=enumerator,
        template_id=template_id,
        project_id=project_id,
        supervisor_id=str(sup_id) if sup_id else "",
        limit=100,
    )
    out = []
    for (sid, facility_name, tplid, survey_type, enum, st, created_at) in rows:
        out.append(
            {
                "id": sid,
                "facility_name": facility_name,
                "template_id": tplid,
                "survey_type": survey_type,
                "enumerator_name": enum,
                "status": st,
                "created_at": created_at,
            }
        )
    return jsonify(out)


@app.route("/surveys/<int:sid>", methods=["GET"])
def survey_one(sid):
    header, answers, qa = sup.get_survey_details(int(sid))
    if not header:
        return jsonify({"error": "Survey not found"}), 404
    return jsonify({"header": header, "answers": answers, "qa": qa.__dict__})


@app.route("/qa/alerts", methods=["GET"])
def qa_alerts():
    project_id = request.args.get("project_id") or ""
    sup_id = current_supervisor_id()
    alerts = sup.qa_alerts_dashboard(limit=100, project_id=project_id, supervisor_id=str(sup_id) if sup_id else "")
    return jsonify([a.__dict__ for a in alerts])


@app.route("/facilities/suggest", methods=["GET"])
def facilities_suggest():
    q = (request.args.get("q") or "").strip()
    project_id = request.args.get("project_id") or ""
    project_id = int(project_id) if str(project_id).isdigit() else None
    params = []
    where = []
    if project_id is not None:
        where.append("s.project_id=?")
        params.append(int(project_id))
    if "deleted_at" in surveys_cols():
        where.append("s.deleted_at IS NULL")
    if q:
        where.append("LOWER(f.name) LIKE LOWER(?)")
        params.append(f"%{q}%")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT DISTINCT f.name AS name
            FROM facilities f
            LEFT JOIN surveys s ON s.facility_id = f.id
            {where_sql}
            ORDER BY f.name ASC
            LIMIT 50
            """,
            tuple(params),
        )
        rows = cur.fetchall()
    return jsonify([r["name"] for r in rows])


@app.route("/surveys/duplicate_check", methods=["GET"])
def duplicate_check():
    facility_name = (request.args.get("facility_name") or "").strip()
    enumerator_name = (request.args.get("enumerator_name") or "").strip()
    project_id = request.args.get("project_id") or ""
    project_id = int(project_id) if str(project_id).isdigit() else None
    if not facility_name or not enumerator_name:
        return jsonify({"duplicate": False})
    params = [facility_name, enumerator_name, now_iso()]
    where = "LOWER(f.name)=LOWER(?) AND COALESCE(s.enumerator_name,'')=? AND date(s.created_at)=date(?)"
    if "deleted_at" in surveys_cols():
        where += " AND s.deleted_at IS NULL"
    if project_id is not None:
        where += " AND s.project_id=?"
        params.append(int(project_id))
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM surveys s
            JOIN facilities f ON f.id = s.facility_id
            WHERE {where}
            """,
            tuple(params),
        )
        dup = int(cur.fetchone()["c"] or 0) > 0
    return jsonify({"duplicate": dup})


@app.route("/api/assignments/resolve", methods=["GET"])
def resolve_assignment():
    code = (request.args.get("code") or "").strip()
    project_id = request.args.get("project_id") or ""
    template_id = request.args.get("template_id") or ""
    project_id = int(project_id) if str(project_id).isdigit() else None
    template_id = int(template_id) if str(template_id).isdigit() else None

    if not code or project_id is None:
        return jsonify({"ok": False, "error": "Missing code or project."}), 400

    assignment_id = None
    assignment = None
    enumerator = None
    validation = None
    try:
        validation = prj.validate_enumerator_code(code)
    except Exception:
        validation = None

    if validation and validation.get("ok"):
        if project_id is not None and int(validation.get("project_id")) != int(project_id):
            return jsonify({"ok": False, "error": "Code does not match this project."}), 400
        assignment_id = int(validation.get("assignment_id"))
        enumerator = enum.get_enumerator(int(validation.get("enumerator_id")))
        assignment = enum.get_assignment(int(assignment_id)) if assignment_id else None
        if not assignment and enumerator:
            assignment = enum.get_assignment_for_enumerator(project_id, int(enumerator.get("id")), template_id=template_id)
            assignment_id = int(assignment.get("id")) if assignment else assignment_id
        if enumerator and not _enumerator_is_active(enumerator):
            return jsonify({"ok": False, "error": "Enumerator is inactive."}), 403
        if assignment and not _assignment_is_active(assignment):
            return jsonify({"ok": False, "error": "Assignment is inactive."}), 403
    else:
        # Try direct assignment code match (code_full) even if project_tag is missing in DB
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT id, enumerator_id, project_id
                    FROM enumerator_assignments
                    WHERE LOWER(code_full)=LOWER(?)
                    LIMIT 1
                    """,
                    (code,),
                )
                row = cur.fetchone()
            if row:
                if project_id is not None and row["project_id"] and int(row["project_id"]) != int(project_id):
                    return jsonify({"ok": False, "error": "Code does not match this project."}), 400
                assignment_id = int(row["id"])
                enumerator = enum.get_enumerator(int(row["enumerator_id"]))
                assignment = enum.get_assignment(int(assignment_id)) if assignment_id else None
        except Exception:
            pass
        if assignment_id is None and enumerator is None:
            enumerator = enum.get_enumerator_by_code(project_id, code)
            if not enumerator or (enumerator.get("status") or "ACTIVE").upper() != "ACTIVE":
                return jsonify({"ok": False, "error": "Enumerator code not found."}), 404
            assignment = enum.get_assignment_for_enumerator(project_id, int(enumerator.get("id")), template_id=template_id)
            assignment_id = int(assignment.get("id")) if assignment else None
            if enumerator and not _enumerator_is_active(enumerator):
                return jsonify({"ok": False, "error": "Enumerator is inactive."}), 403
            if assignment and not _assignment_is_active(assignment):
                return jsonify({"ok": False, "error": "Assignment is inactive."}), 403
    if not assignment_id:
        return jsonify({"ok": False, "error": "No assignment found for this code."}), 404

    facilities = enum.list_assignment_facilities(assignment_id)

    done_count = len([f for f in facilities if (f.get("status") or "").upper() == "DONE"])
    total_count = len(facilities)
    target = assignment.get("target_facilities_count") if assignment else None
    if target is None:
        target = total_count if total_count else None

    coverage = None
    if assignment and assignment.get("coverage_node_id"):
        node = cov.get_node(int(assignment.get("coverage_node_id")))
        if node:
            coverage = {"id": node.get("id"), "name": node.get("name")}

    project = prj.get_project(int(project_id))
    allow_unlisted = int(project.get("allow_unlisted_facilities") or 0) if project else 0

    return jsonify(
        {
            "ok": True,
            "enumerator": {
                "id": enumerator.get("id") if enumerator else None,
                "name": enumerator.get("name") if enumerator else (validation.get("enumerator_name") if validation else ""),
                "code": (validation.get("code_full") if validation else (enumerator.get("code") if enumerator else "")),
            },
            "assignment": {
                "id": assignment_id,
                "coverage": coverage,
                "target": target,
                "completed": done_count,
                "total": total_count,
            } if assignment_id else None,
            "facilities": [
                {
                    "id": f.get("facility_id"),
                    "name": f.get("facility_name") or "—",
                    "status": f.get("status"),
                    "done_survey_id": f.get("done_survey_id"),
                }
                for f in facilities
            ],
            "allow_unlisted": allow_unlisted,
            "project": {"id": project.get("id"), "name": project.get("name")} if project else None,
        }
    )


# ---------------------------
# Enumerator Share Link UI
# ---------------------------
@app.route("/f/<token>/draft", methods=["GET", "POST", "DELETE"])
@app.route("/p/<int:project_id>/f/<token>/draft", methods=["GET", "POST", "DELETE"])
def share_link_draft(token, project_id=None):
    if not ENABLE_SERVER_DRAFTS:
        return jsonify({"error": "Server drafts disabled."}), 404

    template_row = get_template_by_token(token)
    if not template_row:
        return jsonify({"error": "This form link is inactive."}), 404

    if project_id and int(row_get(template_row, "project_id") or 0) != int(project_id):
        return jsonify({"error": "This form link is inactive."}), 404

    if int(row_get(template_row, "is_active", 1) or 1) != 1:
        return jsonify({"error": "This form link is inactive."}), 403

    template_id = int(template_row["id"])

    assign_id = (request.args.get("assign_id") or "").strip()
    edit_id = (request.args.get("edit_id") or "").strip()
    qs_bits = []
    if assign_id:
        qs_bits.append(f"assign_id={assign_id}")
    if edit_id:
        qs_bits.append(f"edit_id={edit_id}")
    assign_q = f"&{'&'.join(qs_bits)}" if qs_bits else ""

    if request.method == "GET":
        draft_key = (request.args.get("draft") or "").strip()
        if not draft_key:
            return jsonify({"error": "Draft key required."}), 400
        row = fetch_server_draft(token, draft_key)
        if not row:
            return jsonify({"error": "Draft not found."}), 404
        try:
            payload = json.loads(row.get("data_json") or "{}")
        except Exception:
            return jsonify({"error": "Draft is corrupted. Start fresh."}), 400
        return jsonify(
            {
                "draft_key": row.get("draft_key"),
                "data": payload,
                "filled_count": int(row.get("filled_count") or 0),
                "updated_at": row.get("updated_at"),
                "resume_url": request.host_url.rstrip("/")
                + share_path_for_template_row(template_row, token)
                + "?draft="
                + row.get("draft_key")
                + assign_q,
            }
        )

    if request.method == "DELETE":
        draft_key = (request.args.get("draft") or "").strip()
        if not draft_key:
            return jsonify({"error": "Draft key required."}), 400
        delete_server_draft(token, draft_key)
        return jsonify({"ok": True})

    data = request.get_json(silent=True) or {}
    draft_key = (data.get("draft_key") or "").strip()
    draft_payload = data.get("data") or {}
    filled_count = int(data.get("filled_count") or 0)

    if not isinstance(draft_payload, dict):
        return jsonify({"error": "Draft data invalid."}), 400

    if not draft_key:
        draft_key = secrets.token_urlsafe(12)

    save_server_draft(token, template_id, draft_key,
                      draft_payload, filled_count)

    return jsonify(
        {
            "draft_key": draft_key,
            "resume_url": request.host_url.rstrip("/")
            + share_path_for_template_row(template_row, token)
            + "?draft="
            + draft_key
            + assign_q,
        }
    )


@app.route("/f/<token>", methods=["GET", "POST"])
@app.route("/p/<int:project_id>/f/<token>", methods=["GET", "POST"], endpoint="fill_form_project")
def fill_form(token, project_id=None, review_mode: bool = False):
    template_row = get_template_by_token(token)
    if not template_row:
        return render_template_string("<h2>Form link inactive</h2><p>This form link is inactive.</p>"), 404
    preview_mode = request.args.get("preview") == "1"

    tpl_project_id = row_get(template_row, "project_id")
    if project_id and tpl_project_id and int(tpl_project_id) != int(project_id):
        return render_template_string("<h2>Form link inactive</h2><p>This form link is inactive.</p>"), 404

    if not project_id and tpl_project_id:
        return redirect(share_path_for_template_row(template_row, token))

    if PROJECT_REQUIRED and not project_id and not tpl_project_id:
        return render_template_string(
            "<h2>Project required</h2><p>This form must be opened from a project-specific link.</p>"
        ), 400

    # Project status gate (no submissions for Draft/Archived) unless preview
    if not preview_mode:
        proj_to_check = project_id or tpl_project_id
        if proj_to_check:
            project = prj.get_project(int(proj_to_check))
            if project and (project.get("status") or "").upper() != "ACTIVE":
                return render_template_string(
                    "<h2>Project inactive</h2><p>This project is not accepting submissions.</p>"
                ), 403

        if int(row_get(template_row, "is_active", 1) or 1) != 1:
            return render_template_string("<h2>Form link inactive</h2><p>This form link is inactive.</p>"), 403

    template_id = int(template_row["id"])
    questions = tpl.get_template_questions(template_id)

    assignment_mode = "OPTIONAL"
    template_mode = (row_get(template_row, "assignment_mode") or "INHERIT").strip().upper()
    if template_mode and template_mode != "INHERIT":
        assignment_mode = template_mode
    elif project_id:
        project = prj.get_project(int(project_id))
        assignment_mode = (project.get("assignment_mode") or "OPTIONAL").strip().upper() if project else "OPTIONAL"

    require_enum_code = int(
        row_get(template_row, "require_enumerator_code", 0) or 0)
    enable_gps = int(row_get(template_row, "enable_gps", 0) or 0)
    enable_coverage = int(row_get(template_row, "enable_coverage", 0) or 0)
    coverage_scheme_id = row_get(template_row, "coverage_scheme_id")
    enable_consent = int(row_get(template_row, "enable_consent", 0) or 0)
    enable_attestation = int(row_get(template_row, "enable_attestation", 0) or 0)
    collect_email = int(row_get(template_row, "collect_email", 0) or 0)
    allow_edit_response = int(row_get(template_row, "allow_edit_response", 0) or 0)
    show_summary_charts = int(row_get(template_row, "show_summary_charts", 0) or 0)

    assign_id = request.values.get("assign_id") or ""
    prefill_email = (request.values.get("respondent_email") or "").strip().lower()
    edit_id = (request.values.get("edit_id") or "").strip()
    assign_id = int(assign_id) if str(assign_id).isdigit() else None
    edit_id = int(edit_id) if str(edit_id).isdigit() else None
    edit_survey = None
    if edit_id:
        if allow_edit_response != 1:
            return render_template_string(
                "<h2>Edit disabled</h2><p>This form does not allow edits after submit.</p>"
            ), 403
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM surveys WHERE id=? LIMIT 1", (int(edit_id),))
            edit_survey = cur.fetchone()
        edit_survey = row_to_dict(edit_survey) if edit_survey else None
        if not edit_survey:
            return render_template_string("<h2>Response not found</h2><p>This response was not found.</p>"), 404
        if edit_survey.get("template_id") and int(edit_survey.get("template_id")) != int(template_id):
            return render_template_string("<h2>Response mismatch</h2><p>This response does not match this form.</p>"), 404
        if project_id and edit_survey.get("project_id") and int(edit_survey.get("project_id")) != int(project_id):
            return render_template_string("<h2>Response mismatch</h2><p>This response does not match this project.</p>"), 404
        if not assign_id and edit_survey.get("assignment_id"):
            assign_id = int(edit_survey.get("assignment_id"))

    assignment = enum.get_assignment(assign_id) if assign_id else None
    assigned_enumerator = None
    assigned_node = None
    assigned_facilities = []
    assignment_progress = {"completed": 0, "total": 0, "target": None}
    allow_unlisted = 0

    if assignment:
        if assignment.get("project_id") and project_id and int(assignment.get("project_id")) != int(project_id):
            return render_template_string("<h2>Form link inactive</h2><p>This form link is inactive.</p>"), 404
        if assignment.get("template_id") and int(assignment.get("template_id")) != int(template_id):
            return render_template_string("<h2>Form link inactive</h2><p>This form link is inactive.</p>"), 404
        assigned_enumerator = enum.get_enumerator(int(assignment.get("enumerator_id")))
        if assignment.get("coverage_node_id"):
            assigned_node = cov.get_node(int(assignment.get("coverage_node_id")))
        try:
            assigned_facilities = enum.list_assignment_facilities(int(assignment.get("id")))
            assignment_progress["completed"] = len([f for f in assigned_facilities if (f.get("status") or "").upper() == "DONE"])
            assignment_progress["total"] = len(assigned_facilities)
            assignment_progress["target"] = assignment.get("target_facilities_count") or (
                assignment_progress["total"] if assignment_progress["total"] else None
            )
        except Exception:
            assigned_facilities = []
    else:
        if assignment_mode in ("REQUIRED_PROJECT", "REQUIRED_TEMPLATE"):
            return render_template_string(
                "<h2>Assignment required</h2><p>This form requires an assignment link from your supervisor.</p>"
            ), 403
    if assignment_mode == "REQUIRED_TEMPLATE" and assignment:
        if not assignment.get("template_id") or int(assignment.get("template_id")) != int(template_id):
            return render_template_string(
                "<h2>Assignment required</h2><p>This form requires a template-specific assignment link.</p>"
            ), 403

    err = ""
    ok_msg = ""
    survey_id = None

    sticky = {}
    sticky_multi = {}

    if assigned_enumerator:
        sticky["enumerator_name"] = assigned_enumerator.get("name") or ""
        sticky["enumerator_code"] = assigned_enumerator.get("code") or ""
    if assigned_node:
        sticky["coverage_node_id"] = str(assigned_node.get("id"))
    if prefill_email and not (request.form.get("respondent_email") or "").strip():
        sticky["respondent_email"] = prefill_email

    if edit_survey and request.method != "POST":
        if edit_survey.get("facility_id") and assigned_facilities:
            sticky["facility_id"] = str(edit_survey.get("facility_id"))
        elif edit_survey.get("facility_id"):
            fname = get_facility_name_by_id(int(edit_survey.get("facility_id")))
            sticky["facility_name"] = fname or ""
        if edit_survey.get("enumerator_name"):
            sticky["enumerator_name"] = edit_survey.get("enumerator_name") or ""
        if edit_survey.get("enumerator_code"):
            sticky["enumerator_code"] = edit_survey.get("enumerator_code") or ""
        if edit_survey.get("respondent_email"):
            sticky["respondent_email"] = edit_survey.get("respondent_email") or ""
        if edit_survey.get("coverage_node_id"):
            sticky["coverage_node_id"] = str(edit_survey.get("coverage_node_id"))
        if edit_survey.get("consent_obtained") is not None:
            sticky["consent_obtained"] = "YES" if int(edit_survey.get("consent_obtained")) == 1 else "NO"
        if edit_survey.get("consent_signature"):
            sticky["consent_signature"] = edit_survey.get("consent_signature") or ""
        if edit_survey.get("attestation_text"):
            sticky["attestation_confirm"] = "on"

        # preload answers
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT template_question_id, question, answer FROM survey_answers WHERE survey_id=? ORDER BY id ASC",
                    (int(edit_id),),
                )
                rows = cur.fetchall()
            by_qid = {}
            by_text = {}
            for r in rows:
                if r["template_question_id"] is not None:
                    by_qid[int(r["template_question_id"])] = r["answer"]
                if r["question"]:
                    by_text[str(r["question"])] = r["answer"]
            for row in questions:
                qid = row[0]
                qtext = row[1]
                qtype = (row[2] or "TEXT").upper()
                val = by_qid.get(int(qid)) if by_qid else None
                if val is None:
                    val = by_text.get(str(qtext))
                if val is None:
                    continue
                if qtype == "MULTI_CHOICE":
                    sticky_multi[f"q_{qid}"] = [v.strip() for v in str(val).split(",") if v.strip()]
                else:
                    sticky[f"q_{qid}"] = str(val)
        except Exception:
            pass

    if project_id:
        try:
            project = prj.get_project(int(project_id))
            allow_unlisted = int(project.get("allow_unlisted_facilities") or 0) if project else 0
        except Exception:
            allow_unlisted = 0

    if request.method == "POST":
        intent = (request.form.get("intent") or "submit").strip().lower()
        # Preserve inputs if validation fails
        sticky["facility_name"] = (
            request.form.get("facility_name") or "").strip()
        sticky["facility_id"] = (
            request.form.get("facility_id") or "").strip()
        sticky["enumerator_name"] = (
            request.form.get("enumerator_name") or "").strip()
        sticky["enumerator_code"] = (
            request.form.get("enumerator_code") or "").strip()
        sticky["respondent_email"] = (
            request.form.get("respondent_email") or "").strip()
        sticky["coverage_node_id"] = (
            request.form.get("coverage_node_id") or "").strip()
        sticky["consent_obtained"] = (
            request.form.get("consent_obtained") or "").strip()
        sticky["consent_signature"] = (
            request.form.get("consent_signature") or "").strip()
        sticky["attestation_confirm"] = (
            request.form.get("attestation_confirm") or "").strip()

        for row in questions:
            qid = row[0]
            qtype = row[2] if len(row) > 2 else "TEXT"
            field = f"q_{qid}"
            qt = (qtype or "TEXT").upper()
            if qt == "MULTI_CHOICE":
                sticky_multi[field] = request.form.getlist(field)
            else:
                sticky[field] = (request.form.get(field) or "").strip()

        log_assignment_error = False
        if assignment_mode in ("REQUIRED_PROJECT", "REQUIRED_TEMPLATE") and not assignment:
            err = "Assignment required. Please use the link provided by your supervisor."
            log_assignment_error = True
        elif assignment_mode == "REQUIRED_TEMPLATE" and assignment and (
            not assignment.get("template_id") or int(assignment.get("template_id")) != int(template_id)
        ):
            err = "Assignment required for this form. Please use the correct assignment link."
            log_assignment_error = True
        elif intent != "submit":
            err = "Draft saved on device. Use Submit to finalize."
        else:
            try:
                survey_id = save_survey_from_share_link(
                    template_row,
                    request.form,
                    existing_survey_id=edit_id if edit_id else None,
                )
                server_draft_key = (request.form.get(
                    "server_draft_key") or "").strip()
                if ENABLE_SERVER_DRAFTS and server_draft_key:
                    delete_server_draft(token, server_draft_key)
                assign_id = (request.form.get("assign_id") or "").strip()
                if project_id:
                    return redirect(url_for("form_success_project", project_id=int(project_id), token=token, sid=survey_id, assign_id=assign_id))
                return redirect(url_for("form_success", token=token, sid=survey_id, assign_id=assign_id))
            except ValueError as e:
                err = str(e)
                log_submission_error(
                    template_id=int(template_id),
                    project_id=int(project_id) if project_id else None,
                    survey_id=None,
                    error_type="validation",
                    error_message=err,
                    context={
                        "assign_id": request.form.get("assign_id") or "",
                        "facility_name": sticky.get("facility_name"),
                        "enumerator_name": sticky.get("enumerator_name"),
                        "enumerator_code": sticky.get("enumerator_code"),
                        "coverage_node_id": sticky.get("coverage_node_id"),
                    },
                )
            except Exception as e:
                log_submission_error(
                    template_id=int(template_id),
                    project_id=int(project_id) if project_id else None,
                    survey_id=None,
                    error_type="system",
                    error_message=str(e),
                    context={
                        "assign_id": request.form.get("assign_id") or "",
                        "facility_name": sticky.get("facility_name"),
                        "enumerator_name": sticky.get("enumerator_name"),
                        "enumerator_code": sticky.get("enumerator_code"),
                        "coverage_node_id": sticky.get("coverage_node_id"),
                    },
                )
                err = "Something went wrong submitting this form. Please try again or contact your supervisor."
        if log_assignment_error:
            log_submission_error(
                template_id=int(template_id),
                project_id=int(project_id) if project_id else None,
                survey_id=None,
                error_type="validation",
                error_message=err,
                context={
                    "assign_id": request.form.get("assign_id") or "",
                    "facility_name": sticky.get("facility_name"),
                    "enumerator_name": sticky.get("enumerator_name"),
                    "enumerator_code": sticky.get("enumerator_code"),
                    "coverage_node_id": sticky.get("coverage_node_id"),
                },
            )

    # Coverage (optional)
    coverage_block = ""
    coverage_nodes = []
    if enable_coverage and coverage_scheme_id:
        try:
            coverage_nodes = cov.list_nodes(int(coverage_scheme_id), limit=5000)
        except Exception:
            coverage_nodes = []

        assigned_label = ""
        if assigned_node:
            # best-effort label (no recursion needed for locked state)
            assigned_label = assigned_node.get("name") or ""

        coverage_block = f"""
        <div class="card">
          <div class="card-header">
            <div>
              <div class="section-title">Coverage location</div>
              <div class="section-sub">Select the area you are covering (as assigned by your supervisor).</div>
            </div>
          </div>
          <div class="field-group">
            <label>Coverage node <span class="req">Required</span></label>
            <div class="muted hint">Choose the closest administrative area.</div>
            {"<div class='muted' style='font-weight:700'>" + assigned_label + "</div>" if assigned_node else ""}
            {"<div id='coverageSelector' class='row' style='gap:10px; flex-wrap:wrap; margin-top:6px;'></div>" if not assigned_node else ""}
            <input type="hidden" name="coverage_node_id" data-required="1" value="{sticky.get('coverage_node_id','')}" />
          </div>
        </div>
        """

    # GPS
    gps_js = ""
    gps_block = ""
    if enable_gps:
        gps_block = """
        <div class="card">
          <div class="row top">
            <div>
              <div class="h3">Location verification</div>
              <div class="muted">Capture GPS to confirm you are at the facility location (if enabled by the supervisor).</div>
            </div>
            <button type="button" class="btn" onclick="captureGPS()">Capture GPS</button>
          </div>

          <div id="gps_status" class="muted" style="margin-top:10px">GPS not captured yet.</div>

          <input type="hidden" id="gps_lat" name="gps_lat" />
          <input type="hidden" id="gps_lng" name="gps_lng" />
          <input type="hidden" id="gps_accuracy" name="gps_accuracy" />
          <input type="hidden" id="gps_timestamp" name="gps_timestamp" />
        </div>
        """

        gps_js = """
        <script>
          function captureGPS(){
            const s = document.getElementById("gps_status");
            s.innerText = "Capturing GPS…";
            if(!navigator.geolocation){
              s.innerText = "Geolocation not supported on this device/browser.";
              return;
            }
            navigator.geolocation.getCurrentPosition(
              function(pos){
                document.getElementById("gps_lat").value = pos.coords.latitude;
                document.getElementById("gps_lng").value = pos.coords.longitude;
                document.getElementById("gps_accuracy").value = pos.coords.accuracy;
                document.getElementById("gps_timestamp").value = new Date().toISOString();
                s.innerText = "GPS captured successfully.";
              },
              function(err){
                const msg = err && err.message ? err.message : "Unknown error";
                s.innerText = "GPS capture failed—continue without GPS if optional. (" + msg + ")";
              },
              { enableHighAccuracy: true, timeout: 15000, maximumAge: 0 }
            );
          }
        </script>
        """

    consent_block = ""
    if enable_consent or enable_attestation:
        consent_block = f"""
        <div class="card">
          <div class="card-header">
            <div>
              <div class="section-title">Consent & attestation</div>
              <div class="section-sub">Complete ethics requirements before submission.</div>
            </div>
          </div>
          {"""
            <div class='field-group'>
              <label>Consent obtained? <span class="req">Required</span></label>
              <div class="muted hint">Ask the respondent for consent before proceeding.</div>
              <label class="switch" style="display:inline-flex; align-items:center; gap:10px; margin-top:6px;">
                <input type="checkbox" id="consentToggle" name="consent_toggle" """ + ("checked" if sticky.get("consent_obtained") == "YES" else "") + """>
                <span class="slider"></span>
                <span class="muted">Yes</span>
              </label>
              <input type="hidden" name="consent_obtained" id="consentObtainedHidden" value=""" + ("YES" if sticky.get("consent_obtained") == "YES" else "NO") + """>
              <div id="consentSignatureWrap" style="margin-top:12px; display:none;">
                <div class="muted" style="margin-bottom:8px;">Signature (required if consent is Yes)</div>
                <div style="border:1px dashed var(--border); border-radius:16px; background:#fff; padding:10px;">
                  <canvas id="consentSignaturePad" width="600" height="200" style="width:100%; height:200px; border-radius:10px; background:#fafafa;"></canvas>
                </div>
                <div class="row" style="margin-top:8px; gap:8px;">
                  <button type="button" class="btn sm" id="consentClearBtn">Clear</button>
                </div>
                <input type="hidden" name="consent_signature" id="consentSignatureInput" value="{{ sticky.get('consent_signature','') }}"/>
                {% if sticky.get('consent_signature') %}
                  <div class="muted" style="margin-top:8px;">Existing signature loaded.</div>
                {% endif %}
              </div>
            </div>
          """ if enable_consent else ""}
          {"<div class='field-group'><div class=\"row\" data-required=\"1\" style=\"gap:10px\"><label class=\"row\" style=\"gap:10px\"><input type=\"checkbox\" name=\"attestation_confirm\" style=\"width:auto\" " + ("checked" if sticky.get("attestation_confirm") else "") + "><span>I confirm this submission is accurate and collected ethically.</span></label></div><input type=\"hidden\" name=\"attestation_text\" value=\"I confirm this submission is accurate and collected ethically.\" /></div>" if enable_attestation else ""}
        </div>
        """

    # Build question blocks
    # -----------------------
    # Section grouping
    # -----------------------
    def is_section_marker(text: str) -> bool:
        t = (text or "").strip()
        return t.startswith("## ") or t.upper().startswith("[SECTION]")

    def is_media_marker(text: str) -> bool:
        t = (text or "").strip()
        return t.upper().startswith("[IMAGE] ") or t.upper().startswith("[VIDEO] ")

    def parse_media_marker(text: str) -> dict:
        raw = (text or "").strip()
        upper = raw.upper()
        kind = "IMAGE" if upper.startswith("[IMAGE] ") else "VIDEO"
        payload = raw[len("[IMAGE] "):] if kind == "IMAGE" else raw[len("[VIDEO] "):]
        parts = payload.split("|", 1)
        url = parts[0].strip()
        caption = parts[1].strip() if len(parts) > 1 else ""
        return {"kind": kind, "url": url, "caption": caption}

    def parse_section_marker(text: str) -> dict:
        t = (text or "").strip()
        if t.upper().startswith("[SECTION]"):
            payload = t[len("[SECTION]"):].strip()
            title = payload
            desc = ""
            if "|" in payload:
                title, desc = payload.split("|", 1)
            return {"title": title.strip() or "Section", "desc": desc.strip()}
        if t.startswith("## "):
            rest = t[3:].strip()
            if "\n" in rest:
                title, desc = rest.split("\n", 1)
                return {"title": title.strip() or "Section", "desc": desc.strip()}
            return {"title": rest.strip() or "Section", "desc": ""}
        return {"title": t.strip() or "Section", "desc": ""}

    sections = []
    current = {"title": "Form", "desc": "", "blocks": []}

    # Build HTML inputs per question, grouped by section
    idx = 0
    total_q = 0

    # count actual questions (exclude section/media markers)
    for row in questions:
        qtext = row[1]
        if not is_section_marker(qtext) and not is_media_marker(qtext):
            total_q += 1

    for row in questions:
        qid = row[0]
        qtext = row[1]
        qtype = row[2] if len(row) > 2 else "TEXT"
        is_required = row[4] if len(row) > 4 else 0
        help_text = row[5] if len(row) > 5 else None
        validation = _parse_validation_json(row[6]) if len(row) > 6 else {}
        # Handle section header rows
        if is_section_marker(qtext):
            # push existing section if it has content
            if current["blocks"]:
                sections.append(current)
            parsed = parse_section_marker(qtext)
            current = {
                "title": parsed.get("title") or "Section",
                "desc": parsed.get("desc") or "",
                "blocks": [],
            }
            continue
        if is_media_marker(qtext):
            media = parse_media_marker(qtext)
            if media["kind"] == "IMAGE":
                block = f"""
      <div class="q">
        <div class="q-title"><b>Image</b></div>
        <div class="q-input" style="margin-top:8px">
          <img src="{html.escape(media['url'])}" alt="Form image" style="max-width:100%; border-radius:12px; border:1px solid var(--border);" />
          {f"<div class='muted' style='margin-top:6px'>{html.escape(media['caption'])}</div>" if media['caption'] else ""}
        </div>
      </div>
    """
            else:
                url = media["url"]
                is_youtube = "youtube.com" in url or "youtu.be" in url
                is_video_file = any(url.lower().endswith(ext) for ext in (".mp4", ".webm", ".ogg"))
                if is_youtube:
                    # basic embed
                    embed_url = url.replace("watch?v=", "embed/")
                    block = f"""
      <div class="q">
        <div class="q-title"><b>Video</b></div>
        <div class="q-input" style="margin-top:8px">
          <div style="position:relative; padding-bottom:56.25%; height:0; overflow:hidden; border-radius:12px; border:1px solid var(--border);">
            <iframe src="{html.escape(embed_url)}" style="position:absolute; top:0; left:0; width:100%; height:100%;" frameborder="0" allowfullscreen></iframe>
          </div>
          {f"<div class='muted' style='margin-top:6px'>{html.escape(media['caption'])}</div>" if media['caption'] else ""}
        </div>
      </div>
    """
                elif is_video_file:
                    block = f"""
      <div class="q">
        <div class="q-title"><b>Video</b></div>
        <div class="q-input" style="margin-top:8px">
          <video controls style="width:100%; border-radius:12px; border:1px solid var(--border);">
            <source src="{html.escape(url)}" />
          </video>
          {f"<div class='muted' style='margin-top:6px'>{html.escape(media['caption'])}</div>" if media['caption'] else ""}
        </div>
      </div>
    """
                else:
                    block = f"""
      <div class="q">
        <div class="q-title"><b>Video</b></div>
        <div class="q-input" style="margin-top:8px">
          <a href="{html.escape(url)}" target="_blank" rel="noopener">Open video</a>
          {f"<div class='muted' style='margin-top:6px'>{html.escape(media['caption'])}</div>" if media['caption'] else ""}
        </div>
      </div>
    """
            current["blocks"].append(block)
            continue

        idx += 1
        qtype = (qtype or "TEXT").upper()
        req = int(is_required) == 1
        req_label = " <span style='color:#b00'>(Required)</span>" if req else ""
        req_attr = "data-required='1'" if req else ""
        help_html = f"<div class='muted' style='margin-top:6px'>{html.escape(str(help_text))}</div>" if help_text else ""

        name = f"q_{qid}"

        # NOTE: for simplicity, not implementing sticky values here; your draft feature already restores.
        if qtype == "LONGTEXT":
            attrs = ""
            if isinstance(validation.get("min_length"), int):
                attrs += f" minlength='{int(validation.get('min_length'))}'"
            if isinstance(validation.get("max_length"), int):
                attrs += f" maxlength='{int(validation.get('max_length'))}'"
            if validation.get("pattern"):
                attrs += f" pattern='{html.escape(str(validation.get('pattern')))}'"
            input_html = f"<textarea name='{name}' rows='4' style='width:100%' {req_attr}{attrs}></textarea>"
        elif qtype == "YESNO":
            input_html = f"""
          <div class="row yesno" {req_attr}>
            <label><input type="radio" name="{name}" value="YES"> Yes</label>
            <label style="margin-left:12px"><input type="radio" name="{name}" value="NO"> No</label>
          </div>
        """
        elif qtype == "NUMBER":
            attrs = ""
            if isinstance(validation.get("min_value"), (int, float)):
                attrs += f" min='{validation.get('min_value')}'"
            if isinstance(validation.get("max_value"), (int, float)):
                attrs += f" max='{validation.get('max_value')}'"
            input_html = f"<input name='{name}' type='number' step='any' style='width:100%' {req_attr}{attrs}/>"
        elif qtype == "DATE":
            input_html = f"<input name='{name}' type='date' style='width:100%' {req_attr}/>"
        elif qtype == "EMAIL":
            attrs = ""
            if validation.get("pattern"):
                attrs += f" pattern='{html.escape(str(validation.get('pattern')))}'"
            input_html = f"<input name='{name}' type='email' style='width:100%' {req_attr}{attrs}/>"
        elif qtype == "PHONE":
            attrs = ""
            if validation.get("pattern"):
                attrs += f" pattern='{html.escape(str(validation.get('pattern')))}'"
            input_html = f"<input name='{name}' type='tel' style='width:100%' {req_attr}{attrs}/>"
        elif qtype in ("SINGLE_CHOICE", "DROPDOWN", "MULTI_CHOICE"):
            choices = q_choices(qid)
            if not choices:
                attrs = ""
                if isinstance(validation.get("min_length"), int):
                    attrs += f" minlength='{int(validation.get('min_length'))}'"
                if isinstance(validation.get("max_length"), int):
                    attrs += f" maxlength='{int(validation.get('max_length'))}'"
                if validation.get("pattern"):
                    attrs += f" pattern='{html.escape(str(validation.get('pattern')))}'"
                input_html = f"<input name='{name}' type='text' style='width:100%' {req_attr}{attrs}/>"
            else:
                if qtype == "DROPDOWN":
                    opts = "".join(
                        [f"<option value='{c[2]}'>{c[2]}</option>" for c in choices])
                    input_html = f"<select name='{name}' style='width:100%' {req_attr}><option value=''></option>{opts}</select>"
                elif qtype == "MULTI_CHOICE":
                    items = []
                    for c in choices:
                        items.append(
                            f"<label class='opt block'><input type='checkbox' name='{name}' value='{c[2]}'> {c[2]}</label>")
                    input_html = f"<div class='multi' {req_attr}>" + \
                        "".join(items) + "</div>"
                else:
                    items = []
                    for c in choices:
                        items.append(
                            f"<label class='opt block'><input type='radio' name='{name}' value='{c[2]}'> {c[2]}</label>")
                    input_html = f"<div class='single' {req_attr}>" + \
                        "".join(items) + "</div>"
        else:
            attrs = ""
            if isinstance(validation.get("min_length"), int):
                attrs += f" minlength='{int(validation.get('min_length'))}'"
            if isinstance(validation.get("max_length"), int):
                attrs += f" maxlength='{int(validation.get('max_length'))}'"
            if validation.get("pattern"):
                attrs += f" pattern='{html.escape(str(validation.get('pattern')))}'"
            input_html = f"<input name='{name}' type='text' style='width:100%' {req_attr}{attrs}/>"

        block = f"""
      <div class="q">
        <div class="q-title"><b>{qtext}</b>{req_label}</div>
        {help_html}
        <div class="q-input" style="margin-top:8px">{input_html}</div>
        <div class="missing-note">This question is required.</div>
      </div>
    """
        current["blocks"].append(block)

    # push final section
    if current["blocks"]:
        sections.append(current)

    # Render section HTML as pages
    page_html = []
    num_pages = len(sections)
    step_raw = (request.args.get("step") or "").strip()
    try:
        step_num = int(step_raw) if step_raw else 1
    except Exception:
        step_num = 1
    if review_mode and num_pages:
        step_num = num_pages
    step_num = max(1, min(step_num, num_pages if num_pages > 0 else 1))
    step_index = step_num - 1

    if not review_mode:
        sec = sections[step_index] if sections else {
            "title": "Form", "desc": "", "blocks": []}
        sec_title = html.escape(sec.get("title") or "Form")
        sec_desc = html.escape(sec.get("desc") or "")
        sec_desc_html = f"<div class='muted' style='margin-top:-6px; margin-bottom:12px'>{sec_desc}</div>" if sec_desc else ""
        page_html.append(
            f"""
        <div class="page" data-page="{step_index}">
          <div class="card">
            <h3 style="margin:0 0 10px 0">{sec_title}</h3>
            {sec_desc_html}
            {''.join(sec['blocks'])}
          </div>
        </div>
        """
        )
    else:
        for idx, sec in enumerate(sections):
            sec_title = html.escape(sec.get("title") or "Section")
            sec_desc = html.escape(sec.get("desc") or "")
            sec_desc_html = f"<div class='muted' style='margin-top:-6px; margin-bottom:12px'>{sec_desc}</div>" if sec_desc else ""
            page_html.append(
                f"""
        <div class="page" data-page="{idx}">
          <div class="card">
            <h3 style="margin:0 0 10px 0">{sec_title}</h3>
            {sec_desc_html}
            {''.join(sec['blocks'])}
          </div>
        </div>
        """
            )

    q_pages_html = "\\n".join(page_html)

    template_desc = row_get(template_row, "description", "") or ""
    sensitive_notice = ""
    if int(row_get(template_row, "is_sensitive", 0) or 0) == 1:
        sensitive_notice = "Sensitive data — handle responses with care."
    project_notice = ""
    project_name = ""
    if project_id:
        project = prj.get_project(int(project_id))
        if project:
            project_name = project.get("name") or ""
            if int(project.get("is_test_project") or 0) == 1:
                project_notice = "Test project — submissions are for practice only."
            elif int(project.get("is_live_project") or 0) == 1:
                project_notice = "Live data collection is active."
    if project_id:
        base_url = url_for("fill_form_project",
                           project_id=int(project_id), token=token)
        review_url = url_for("fill_form_project_review",
                             project_id=int(project_id), token=token)
        sync_url = url_for("fill_form_project_sync", project_id=int(project_id), token=token)
        sync_center_url = url_for("fill_form_project_sync_center", project_id=int(project_id), token=token)
    else:
        base_url = url_for("fill_form", token=token)
        review_url = url_for("fill_form_review", token=token)
        sync_url = url_for("fill_form_sync", token=token)
        sync_center_url = url_for("fill_form_sync_center", token=token)

    query_bits = []
    if assign_id:
        query_bits.append(f"assign_id={assign_id}")
    if edit_id:
        query_bits.append(f"edit_id={edit_id}")
    qs = "&".join(query_bits)
    extra_q = f"&{qs}" if qs else ""
    next_step_url = f"{base_url}?step={step_num + 1}{extra_q}" if step_num < num_pages else ""
    prev_step_url = f"{base_url}?step={step_num - 1}{extra_q}" if step_num > 1 else ""
    edit_step_url = f"{base_url}?step={num_pages}{extra_q}" if num_pages else base_url
    if qs:
        base_url = f"{base_url}?{qs}"
        review_url = f"{review_url}?{qs}"

    code_gate = True if (require_enum_code or assignment_mode in ("REQUIRED_PROJECT", "REQUIRED_TEMPLATE")) else False
    if assigned_facilities:
        required_names = ["facility_id", "enumerator_name"]
    else:
        required_names = ["facility_name", "enumerator_name"]
    if code_gate:
        required_names.append("enumerator_code")
    if enable_coverage and coverage_scheme_id:
        required_names.append("coverage_node_id")
    if enable_consent:
        required_names.append("consent_obtained")
    if enable_attestation:
        required_names.append("attestation_confirm")
    if collect_email:
        required_names.append("respondent_email")
    for row in questions:
        qid = row[0]
        qtext = row[1]
        is_required = row[4] if len(row) > 4 else 0
        if not is_section_marker(qtext) and not is_media_marker(qtext) and int(is_required) == 1:
            required_names.append(f"q_{qid}")

    assigned_coverage = assigned_node.get("name") if assigned_node else ""

    offline_config = {
        "syncUrl": sync_url,
        "queueKey": f"{token}:{project_id or 'solo'}",
        "syncCenterUrl": sync_center_url,
    }
    offline_config_json = json.dumps(offline_config)

    return render_template_string(
        """
        <html>
        <head>
          <title>{{template_name}} — HurkField</title>
          <meta name="viewport" content="width=device-width, initial-scale=1"/>
          <link rel="preconnect" href="https://fonts.googleapis.com">
          <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
          <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Poppins:wght@400;500;600;700;800&display=swap" rel="stylesheet">

          <style>
            :root{
              --bg:#fbfbfd; --card:#ffffff; --text:#0f172a; --muted:#667085;
              --border:#e5e7eb; --soft:#f2f4f7; --danger:#b42318;
              --accent:#111827; --accent-soft:#eef2ff;
              --primary:#7C3AED;
              --primary-500:#8B5CF6;
              --shadow: 0 10px 30px rgba(2,6,23,.06);
              --font-heading:"Poppins", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
              --font-body:"Inter", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
            }
            body{
              font-family: var(--font-body);
              background:
                radial-gradient(900px 260px at 10% -10%, rgba(124,58,237,.12), transparent 60%),
                var(--bg);
              color:var(--text);
              margin:0; padding:0;
            }
            .page-wrap{max-width:980px; margin:0 auto; padding:18px 16px 90px;}
            .stack{display:flex; flex-direction:column; gap:14px;}
            .card{
              background:var(--card);
              border:1px solid var(--border);
              border-radius:22px;
              padding:22px;
              box-shadow:0 16px 36px rgba(2,6,23,.08);
            }
            .hero{
              background:
                radial-gradient(220px 120px at 10% 0%, rgba(124,58,237,.18), transparent 60%),
                linear-gradient(135deg, rgba(124,58,237,.10), rgba(15,23,42,.02));
              border-color: rgba(124,58,237,.25);
            }
            .hero-top{display:flex; gap:16px; align-items:flex-start; justify-content:space-between; flex-wrap:wrap;}
            .eyebrow{font-family:var(--font-heading); font-size:12px; letter-spacing:.3px; text-transform:uppercase; color:#667085; font-weight:700;}
            .pill{
              background:var(--accent-soft);
              color:var(--accent);
              border:1px solid rgba(17,24,39,.12);
              padding:8px 12px;
              border-radius:999px;
              font-size:12px;
              font-weight:800;
            }
            .h1{font-family:var(--font-heading); font-size:22px; margin:0; letter-spacing:-.2px;}
            .h3{font-family:var(--font-heading); font-size:16px; font-weight:800; margin:0;}
            .muted{color:var(--muted); line-height:1.65;}
            .err{
              border-color: rgba(180,35,24,.25);
              background: rgba(180,35,24,.06);
            }
            label{font-family:var(--font-heading); font-weight:800;}
            input[type="text"],
            input[type="email"],
            input[type="password"],
            input[type="number"],
            input[type="tel"],
            input[type="url"],
            input[type="search"],
            input[type="date"],
            input[type="time"],
            input[type="datetime-local"],
            textarea,
            select{
              width:100%;
              padding:12px 14px;
              border-radius:16px;
              border:1px solid rgba(124,58,237,.22);
              background:linear-gradient(180deg, #ffffff 0%, #f7f8fc 100%);
              color:#0f172a;
              font-size:15px;
              outline:none;
              box-shadow: inset 0 1px 0 rgba(255,255,255,.85);
              transition:border-color .18s ease, box-shadow .18s ease, background .18s ease;
            }
            input[type="text"]::placeholder,
            input[type="email"]::placeholder,
            input[type="password"]::placeholder,
            input[type="number"]::placeholder,
            input[type="tel"]::placeholder,
            input[type="url"]::placeholder,
            input[type="search"]::placeholder,
            textarea::placeholder{
              color:#98a4b5;
            }
            input[type="text"]:focus,
            input[type="email"]:focus,
            input[type="password"]:focus,
            input[type="number"]:focus,
            input[type="tel"]:focus,
            input[type="url"]:focus,
            input[type="search"]:focus,
            input[type="date"]:focus,
            input[type="time"]:focus,
            input[type="datetime-local"]:focus,
            textarea:focus,
            select:focus{
              border-color:rgba(124,58,237,.7);
              box-shadow:0 0 0 4px rgba(124,58,237,.14);
            }
            textarea{resize:vertical;}
            input[type="file"]{
              border:1px solid rgba(124,58,237,.24);
              background:#f8f8fc;
              border-radius:14px;
              padding:10px 12px;
            }
            .card-header{display:flex; justify-content:space-between; gap:12px; align-items:center; flex-wrap:wrap;}
            .section-title{font-family:var(--font-heading); font-size:16px; font-weight:800;}
            .section-sub{font-size:13px; color:var(--muted);}
            .field-group{margin-top:14px;}
            .q{
              border:1px solid rgba(148,163,184,.45);
              background:#ffffff;
              border-radius:18px;
              padding:16px;
              margin-top:12px;
            }
            .q-head{display:flex; gap:12px; align-items:flex-start; justify-content:space-between;}
            .q-no{
              min-width:88px;
              font-weight:800;
              color:#344054;
              background:#fff;
              border:1px solid rgba(148,163,184,.35);
              border-radius:999px;
              padding:6px 10px;
              text-align:center;
            }
            .q-title{font-family:var(--font-heading); font-weight:900; flex:1;}
            .q-input{margin-top:12px;}
            .row{display:flex; gap:12px; flex-wrap:wrap;}
            .row.top{justify-content:space-between; align-items:center;}
            .opt{
              display:flex; align-items:center; gap:10px;
              padding:10px 12px;
              border:1px solid var(--border);
              background:#fff;
              border-radius:16px;
              justify-content:flex-start;
            }
            .opt.block{width:100%;}
            .opt input[type="radio"],
            .opt input[type="checkbox"]{
              width:auto;
              margin:0;
            }
            .req{
              display:inline-block;
              margin-left:8px;
              padding:4px 8px;
              border-radius:999px;
              font-size:12px;
              border:1px solid rgba(180,35,24,.25);
              color:var(--danger);
              background: rgba(180,35,24,.06);
              vertical-align:middle;
            }
            .missing{
              border-color: rgba(180,35,24,.35) !important;
              box-shadow: 0 0 0 4px rgba(180,35,24,.10) !important;
            }
            .missing.pulse{
              animation: missingPulse .35s ease-in-out;
            }
            @keyframes missingPulse{
              0%{transform:translateX(0)}
              25%{transform:translateX(-3px)}
              50%{transform:translateX(3px)}
              75%{transform:translateX(-2px)}
              100%{transform:translateX(0)}
            }
            .missing-note{
              margin-top:10px;
              color: var(--danger);
              font-weight: 800;
              display:none;
            }
            .missing-note.show{display:block;}

            .btnbar{
              position:fixed;
              left:0; right:0; bottom:0;
              background: rgba(251,251,253,.92);
              backdrop-filter: blur(10px);
              border-top:1px solid var(--border);
              padding:12px 16px;
              box-shadow:0 -10px 30px rgba(2,6,23,.08);
            }
            .btnwrap{max-width:980px; margin:0 auto; display:flex; gap:12px; align-items:center; justify-content:space-between;}
            .btn{
              font-family:var(--font-heading);
              padding:12px 18px;
              border-radius:14px;
              border:1px solid var(--border);
              background:#fff;
              font-weight:600;
              cursor:pointer;
              transition:all 0.3s ease;
              white-space:nowrap;
            }
            .btn.primary{
              background:linear-gradient(135deg, var(--primary), var(--primary-500));
              color:#fff;
              border:none;
              box-shadow:0 12px 30px rgba(124,58,237,.35);
            }
            .btn:hover{
              border-color:var(--primary);
              box-shadow:0 4px 12px rgba(124,58,237,.15);
            }
            .btn.primary:hover{
              box-shadow:0 16px 40px rgba(124,58,237,.45);
              transform:translateY(-2px);
            }
            .btn:disabled{
              opacity:.65;
              cursor:not-allowed;
              transform:none;
              box-shadow:none;
            }
            .btn.sm{
              padding:8px 12px;
              border-radius:10px;
              font-size:12px;
            }
            .switch{
              position:relative;
              width:46px;
              height:26px;
            }
            .switch input{display:none;}
            .switch .slider{
              position:absolute;
              inset:0;
              background:var(--border);
              border-radius:999px;
              transition:all .2s ease;
            }
            .switch .slider:before{
              content:"";
              position:absolute;
              width:20px;
              height:20px;
              left:3px;
              top:3px;
              background:#fff;
              border-radius:50%;
              transition:all .2s ease;
              box-shadow:0 2px 6px rgba(0,0,0,.2);
            }
            .switch input:checked + .slider{
              background:var(--primary);
            }
            .switch input:checked + .slider:before{
              transform:translateX(20px);
            }
            canvas{touch-action:none;}
            .scroll-fab{
              position:fixed;
              right:18px;
              bottom:18px;
              width:44px;
              height:44px;
              border-radius:999px;
              border:1px solid var(--border);
              background:#fff;
              box-shadow:0 12px 28px rgba(15,18,34,.15);
              display:flex;
              align-items:center;
              justify-content:center;
              cursor:pointer;
              z-index:400;
              transition:transform .15s ease, opacity .15s ease;
              opacity:.85;
            }
            .scroll-fab:hover{transform:translateY(-2px); opacity:1;}
            .scroll-fab span{font-size:18px; font-weight:800; color:var(--text);}
            @media (max-width: 700px){
              .scroll-fab{right:12px; bottom:12px;}
            }
            .info-tip{
              display:flex;
              align-items:center;
              gap:10px;
              padding:10px 12px;
              border-radius:14px;
              border:1px dashed rgba(124,58,237,.35);
              background:rgba(124,58,237,.06);
              font-size:13px;
              color:var(--text);
            }
            .offline-card{
              border-style:dashed;
              background:rgba(124,58,237,.06);
            }
            .offline-head{
              display:flex;
              justify-content:space-between;
              align-items:flex-start;
              gap:12px;
              flex-wrap:wrap;
            }
            .offline-title{font-weight:800;}
            .offline-sub{font-size:13px; color:var(--muted); margin-top:4px;}
            .offline-status{
              padding:6px 10px;
              border-radius:999px;
              font-size:11px;
              font-weight:800;
              text-transform:uppercase;
              letter-spacing:.08em;
            }
            .offline-status.online{
              background:rgba(16,185,129,.12);
              color:#047857;
              border:1px solid rgba(16,185,129,.35);
            }
            .offline-status.offline{
              background:rgba(239,68,68,.12);
              color:#b91c1c;
              border:1px solid rgba(239,68,68,.35);
            }
            .offline-actions{
              display:flex;
              gap:8px;
              flex-wrap:wrap;
              margin-top:12px;
            }
            .offline-list{
              margin-top:12px;
              display:grid;
              gap:10px;
            }
            .offline-item{
              display:flex;
              justify-content:space-between;
              align-items:center;
              gap:10px;
              padding:10px 12px;
              border-radius:12px;
              border:1px solid var(--border);
              background:#fff;
            }
            .offline-item .meta{color:var(--muted); font-size:12px;}
            .offline-pill{
              padding:4px 8px;
              border-radius:999px;
              font-size:11px;
              font-weight:700;
              border:1px solid var(--border);
            }
            .offline-pill.pending{
              background:rgba(124,58,237,.12);
              color:#5b21b6;
              border-color:rgba(124,58,237,.35);
            }
            .offline-pill.error{
              background:rgba(239,68,68,.12);
              color:#b91c1c;
              border-color:rgba(239,68,68,.35);
            }
            .offline-pill.syncing{
              background:rgba(59,130,246,.12);
              color:#1d4ed8;
              border-color:rgba(59,130,246,.35);
            }
            .tip-icon{
              font-size:16px;
            }
            .profile-panel{
              border:1px solid var(--border);
              border-radius:16px;
              padding:12px;
              background:var(--soft);
              margin-bottom:12px;
            }
            .activity-widget{
              display:flex;
              gap:10px;
              align-items:center;
              flex-wrap:wrap;
            }
            .hint{font-size:13px;}
            .hide{display:none;}
            .modal{
              position:fixed;
              inset:0;
              background:rgba(15,23,42,.45);
              backdrop-filter: blur(4px);
              display:none;
              align-items:center;
              justify-content:center;
              z-index:200;
              padding:18px;
            }
            .modal.show{display:flex;}
            .modal-card{
              width:min(720px, 100%);
              background:#fff;
              border-radius:20px;
              border:1px solid var(--border);
              padding:22px;
              box-shadow:0 20px 50px rgba(2,6,23,.2);
            }
            .modal-title{font-size:18px; font-weight:900; margin:0;}
            .modal-sub{color:var(--muted); margin-top:6px;}
            .modal-section{
              border:1px solid var(--border);
              border-radius:16px;
              padding:14px;
              margin-top:14px;
              background:var(--surface);
            }
            .modal-actions{
              display:flex;
              gap:10px;
              margin-top:14px;
              flex-wrap:wrap;
            }
            .modal-footer{
              display:flex;
              justify-content:space-between;
              align-items:center;
              gap:10px;
              margin-top:18px;
              flex-wrap:wrap;
            }
            body.modal-open{overflow:hidden;}
            @media (max-width: 900px){
              .page-wrap{padding:16px 14px 110px;}
              .btnwrap{flex-direction:column; align-items:flex-start; gap:10px;}
              .btnbar{padding:10px 14px;}
            }
            @media (max-width: 700px){
              .card{padding:16px;}
              .hero-top{flex-direction:column; align-items:flex-start;}
              .row{gap:10px;}
              .btn{width:auto;}
              .activity-widget{width:100%; justify-content:space-between;}
            }
          </style>
        </head>

        <body>
          <div class="page-wrap">
            <div class="stack">

              <div class="card hero">
                <div class="hero-top">
                  <div>
                    <div class="eyebrow">HurkField Enumerator Form</div>
                    <h1 class="h1" style="margin-top:6px">{{template_name}}</h1>
                    {% if template_desc %}
                      <div class="muted" style="margin-top:10px">{{template_desc}}</div>
                    {% endif %}
                {% if sensitive_notice %}
                  <div class="muted" style="margin-top:10px; font-weight:700;">{{sensitive_notice}}</div>
                {% endif %}
                {% if project_notice %}
                  <div class="muted" style="margin-top:10px; font-weight:700;">{{project_notice}}</div>
                {% endif %}
                <div class="muted" style="margin-top:12px">
                  Complete the form carefully. Required questions are marked.
                </div>
                <div class="muted hint" id="draftStatus" style="margin-top:8px">Draft: not saved yet.</div>
              </div>
                  <div class="row" style="gap:8px; flex-wrap:wrap; justify-content:flex-end;">
                    {% if preview_mode %}
                      <a class="btn sm" href="{{ back_to_builder }}">Back to builder</a>
                    {% endif %}
                    <div class="pill">Questions: {{ total_q }}</div>
                  </div>
                </div>
              </div>

              {% if err %}
                <div class="card err">
                  <b>Submission error:</b> {{err}}
                  <div class="muted" style="margin-top:6px">
                    Please review missing required fields and submit again.
                  </div>
                </div>
              {% endif %}

              <div class="card offline-card" id="offlineSyncCard" style="display:none"></div>

              <form method="POST" id="openfieldForm" class="stack" action="{{ form_action }}">
                <input type="hidden" name="server_draft_key" id="serverDraftKeyInput" value="" />
                <input type="hidden" name="intent" id="submitIntent" value="submit" />
                <input type="hidden" name="assign_id" value="{{ assign_id }}" />
                <input type="hidden" name="client_uuid" id="clientUuidInput" value="" />
                <input type="hidden" name="client_created_at" id="clientCreatedAtInput" value="" />
                <input type="hidden" name="sync_source" id="syncSourceInput" value="" />
                {% if edit_id %}
                <input type="hidden" name="edit_id" value="{{ edit_id }}" />
                {% endif %}

                <div class="card">
                  <div class="card-header">
                    <div>
                      <div class="section-title">Enumerator details</div>
                      <div class="section-sub">These details help the supervisor verify the submission.</div>
                    </div>
                  </div>

                  {% if code_gate and not assigned_enumerator %}
                  <div class="info-tip" style="margin-bottom:12px">
                    <span class="tip-icon">🔐</span>
                    <span>Enter your enumerator code to unlock your assignment and facility list.</span>
                  </div>
                  <div class="field-group">
                    <label>Enumerator Code <span class="req">Required</span></label>
                    <div class="muted hint">Provided by your supervisor/team lead.</div>
                    <div class="row" style="gap:10px; align-items:center;">
                      <input id="enumCodeInput" name="enumerator_code" type="text" value="{{ sticky.get('enumerator_code','') }}" placeholder="e.g., LG-IKJ-012" data-required="1" {{ 'readonly' if assigned_locked else '' }} />
                      <button class="btn sm" type="button" id="verifyCodeBtn">Verify</button>
                    </div>
                    <div class="muted hint" id="codeStatus" style="margin-top:6px"></div>
                  </div>
                  {% endif %}

                  <div class="profile-panel" id="enumProfilePanel" style="{{ 'display:block' if assigned_enumerator else 'display:none' }}">
                    <div style="font-weight:800; margin-bottom:8px">Enumerator profile</div>
                    <div class="row" style="gap:16px; flex-wrap:wrap;">
                      <div>
                        <div class="muted">Name</div>
                        <div id="enumProfileName" style="font-weight:700">{{ assigned_enumerator.get('name') if assigned_enumerator else '' }}</div>
                      </div>
                      <div>
                        <div class="muted">Project</div>
                        <div id="enumProfileProject" style="font-weight:700">{{ project_name }}</div>
                      </div>
                      <div>
                        <div class="muted">Coverage</div>
                        <div id="enumProfileCoverage" style="font-weight:700">{{ assigned_coverage or '—' }}</div>
                      </div>
                      <div>
                        <div class="muted">Progress</div>
                        <div id="enumProfileProgress" style="font-weight:700">
                          {% if assignment_progress.target %}
                            {{ assignment_progress.completed }}/{{ assignment_progress.target }}
                          {% else %}
                            {{ assignment_progress.completed }}/{{ assignment_progress.total }}
                          {% endif %}
                        </div>
                      </div>
                    </div>
                  </div>
                  {% if code_gate and assigned_enumerator %}
                    <input type="hidden" name="enumerator_code" value="{{ sticky.get('enumerator_code','') }}" />
                  {% endif %}

                  <div class="field-group" id="facilitySelectGroup" style="{{ 'display:block' if assigned_facilities else 'display:none' }}">
                    <label>Facility <span class="req">Required</span></label>
                    <div class="muted hint">Select from your assigned list.</div>
                    <select name="facility_id" id="facilitySelect" {% if assigned_facilities %}data-required="1"{% endif %}>
                      <option value=""></option>
                      {% for f in assigned_facilities %}
                        <option value="{{ f.get('facility_id') }}" {% if sticky.get('facility_id') and (sticky.get('facility_id')|int) == (f.get('facility_id')|int) %}selected{% endif %}>
                          {{ f.get('facility_name') or '—' }}{% if f.get('status') and f.get('status').upper() == 'DONE' %} (Done){% endif %}
                        </option>
                      {% endfor %}
                    </select>
                    <div class="muted" id="facilityDuplicateHint" style="margin-top:6px; display:none;"></div>
                  </div>

                  <div class="field-group" id="facilityTextGroup" style="{{ 'display:none' if assigned_facilities else 'display:block' }}">
                    <label>Facility Name <span class="req">Required</span></label>
                    <div class="muted hint">Enter the facility name exactly as it appears on signage or official records.</div>
                    <input name="facility_name" type="text" list="facilityList" value="{{ sticky.get('facility_name','') }}" placeholder="e.g., Rejuva Clinic" {% if not assigned_facilities %}data-required="1"{% endif %} />
                    <datalist id="facilityList"></datalist>
                    <div class="muted" id="facilityDuplicateHintText" style="margin-top:6px; display:none;"></div>
                  </div>

                  <div class="field-group">
                    <label>Enumerator Name <span class="req">Required</span></label>
                    <div class="muted hint">Enter your full name.</div>
                    <input id="enumNameInput" name="enumerator_name" type="text" value="{{ sticky.get('enumerator_name','') }}" placeholder="Your full name" data-required="1" {{ 'readonly' if assigned_locked else '' }} />
                  </div>

                  {% if collect_email %}
                    {% if hide_email %}
                    <div class="field-group">
                      <label>Email <span class="req">Required</span></label>
                      <div class="muted hint">Captured from the link provided by your supervisor.</div>
                      <div class="pill">{{ sticky.get('respondent_email','') }}</div>
                      <input type="hidden" name="respondent_email" value="{{ sticky.get('respondent_email','') }}" data-required="1" />
                    </div>
                    {% else %}
                    <div class="field-group">
                      <label>Email <span class="req">Required</span></label>
                      <div class="muted hint">Used to prevent duplicate responses if enabled.</div>
                      <input name="respondent_email" type="email" value="{{ sticky.get('respondent_email','') }}" placeholder="you@example.com" data-required="1" />
                    </div>
                    {% endif %}
                  {% endif %}

                  {% if require_enum_code and not code_gate %}
                  <div class="field-group">
                    <label>Enumerator Code <span class="req">Required</span></label>
                    <div class="muted hint">Provided by your supervisor/team lead.</div>
                    <input name="enumerator_code" type="text" value="{{ sticky.get('enumerator_code','') }}" placeholder="e.g., LG-IKJ-012" data-required="1" {{ 'readonly' if assigned_locked else '' }} />
                  </div>
                  {% endif %}
                </div>

                {{ coverage_block|safe }}

                {{ gps_block|safe }}

                {{ consent_block|safe }}

                <div class="card">
                  <div style="display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap;">
                    <div>
                      <div style="font-weight:800">Progress</div>
                      <div class="muted" id="pageLabel">{{ 'Review' if review_mode else ('Section ' ~ current_step ~ ' of ' ~ num_pages) }}</div>
                    </div>
                    <div class="activity-widget">
                      <div class="muted" id="todayStats">Today: 0 submissions on this device</div>
                      <button class="btn sm" type="button" id="resetDayStatsBtn">Reset day stats</button>
                    </div>
                  </div>

                  <div style="margin-top:10px; height:10px; background:#eee; border-radius:999px; overflow:hidden;">
                    <div id="progressBar" style="height:10px; width:{{progress_pct}}%; background:#111;"></div>
                  </div>
                </div>

                {{ q_pages_html|safe }}

                <div class="card" id="reviewSection" style="{{ 'display:block;' if review_mode else 'display:none;' }}">
                  <div class="section-title">Review</div>
                  <div class="muted" style="margin-top:6px">Confirm details before submission.</div>

                  <div class="q" style="margin-top:14px">
                    <div class="q-title">Summary</div>
                    <div class="q-input" id="reviewSummaryBlock" style="margin-top:8px"></div>
                  </div>

                  <div class="q" style="margin-top:14px">
                    <div class="q-title">Key details</div>
                    <div class="q-input" id="reviewKeyDetails" style="margin-top:8px"></div>
                  </div>
                </div>

                <div class="card">
                  <div style="display:flex; justify-content:space-between; gap:12px; flex-wrap:wrap;">
                    <button type="button" class="btn" id="prevPageBtn" style="{{ 'display:none;' if review_mode else '' }}">Back</button>
                    <button type="button" class="btn primary" id="nextPageBtn" style="{{ 'display:none;' if review_mode else '' }}">Next</button>
                    <button type="button" class="btn" id="reviewBackBtn" style="{{ '' if review_mode else 'display:none;' }}">Back to edit</button>
                  </div>
                  <div class="muted" style="margin-top:10px" id="navHint">{{ 'Review the summary, then submit.' if review_mode else 'Use Next to continue.' }}</div>
                </div>

              </form>
            </div>
          </div>
          <button class="scroll-fab" id="scrollFab">
            <span id="scrollFabIcon">↓</span>
          </button>

          <div class="btnbar">
              <div class="btnwrap">
                <div>
                  <div class="muted hint" id="submitHint">Review required questions before submitting.</div>
                  <div class="muted hint" id="reviewSummary"></div>
                </div>
                <div class="row" style="justify-content:flex-end">
                  <button class="btn" id="saveDraftBtn" type="button">Save Draft</button>
                  <button class="btn" id="clearDraftBtn" type="button">Clear Draft</button>
                  {% if enable_server_drafts %}
                  <button class="btn" id="saveServerDraftBtn" type="button">Save Draft (Server)</button>
                  {% endif %}
                  <button class="btn primary" id="submitBtn" type="submit" form="openfieldForm">Submit</button>
                </div>
              </div>
            </div>

          <div class="modal" id="draftModal" aria-hidden="true">
            <div class="modal-card">
              <div class="modal-title">Resume draft?</div>
              <div class="modal-sub">A saved draft was found. Choose how you want to continue.</div>

              <div class="modal-section" id="localDraftSection" style="display:none">
                <div style="font-weight:800">Device draft</div>
                <div class="muted" id="localDraftMeta">Saved locally.</div>
                <div class="modal-actions">
                  <button class="btn" type="button" id="resumeLocalBtn">Resume device draft</button>
                </div>
              </div>

              <div class="modal-section" id="serverDraftSection" style="display:none">
                <div style="font-weight:800">Server draft</div>
                <div class="muted" id="serverDraftMeta">Saved on the server.</div>
                <div class="modal-actions">
                  <button class="btn" type="button" id="resumeServerBtn">Resume server draft</button>
                  <button class="btn sm" type="button" id="copyResumeLinkBtn">Copy resume link</button>
                </div>
                <div class="muted" id="serverDraftLink" style="margin-top:8px; word-break:break-all; display:none;"></div>
              </div>

              <div class="modal-footer">
                <label class="row" style="gap:8px">
                  <input type="checkbox" id="draftDontAsk" />
                  <span class="muted">Don't ask again</span>
                </label>
                <button class="btn sm" type="button" id="resetDraftPrefBtn">Reset preference</button>
              </div>

              <div class="modal-actions">
                <button class="btn" type="button" id="startFreshBtn">Start fresh</button>
              </div>
            </div>
          </div>

          <script>
            (function(){
              const fab = document.getElementById("scrollFab");
              const icon = document.getElementById("scrollFabIcon");
              if(!fab || !icon) return;
              function atBottom(){
                return (window.innerHeight + window.scrollY) >= (document.body.scrollHeight - 40);
              }
              function update(){
                if(window.scrollY < 40){
                  fab.dataset.dir = "down";
                  icon.textContent = "↓";
                } else if(atBottom()){
                  fab.dataset.dir = "up";
                  icon.textContent = "↑";
                } else {
                  fab.dataset.dir = "up";
                  icon.textContent = "↑";
                }
              }
              update();
              window.addEventListener("scroll", update, {passive:true});
              fab.addEventListener("click", ()=>{
                const dir = fab.dataset.dir || "up";
                if(dir === "down"){
                  window.scrollTo({ top: document.body.scrollHeight, behavior: "smooth" });
                } else {
                  window.scrollTo({ top: 0, behavior: "smooth" });
                }
              });
            })();

            (function(){
              const toggleEl = document.getElementById("consentToggle");
              const hidden = document.getElementById("consentObtainedHidden");
              const wrap = document.getElementById("consentSignatureWrap");
              const canvas = document.getElementById("consentSignaturePad");
              const input = document.getElementById("consentSignatureInput");
              const clearBtn = document.getElementById("consentClearBtn");
              if(!wrap || !canvas || !input) return;

              function toggle(){
                if(hidden && toggleEl){
                  hidden.value = toggleEl.checked ? "YES" : "NO";
                }
                if(toggleEl && toggleEl.checked){
                  wrap.style.display = "block";
                } else {
                  wrap.style.display = "none";
                  input.value = "";
                }
              }
              if(toggleEl) toggleEl.addEventListener("change", toggle);
              toggle();

              const ctx = canvas.getContext("2d");
              let drawing = false;

              if(input.value){
                const img = new Image();
                img.onload = ()=>{ ctx.drawImage(img, 0, 0, canvas.width, canvas.height); };
                img.src = input.value;
              }

              function pos(e){
                const rect = canvas.getBoundingClientRect();
                const clientX = e.touches ? e.touches[0].clientX : e.clientX;
                const clientY = e.touches ? e.touches[0].clientY : e.clientY;
                return { x: clientX - rect.left, y: clientY - rect.top };
              }
              function start(e){
                drawing = true;
                const p = pos(e);
                ctx.beginPath();
                ctx.moveTo(p.x, p.y);
                e.preventDefault();
              }
              function move(e){
                if(!drawing) return;
                const p = pos(e);
                ctx.lineTo(p.x, p.y);
                ctx.strokeStyle = "#111827";
                ctx.lineWidth = 2;
                ctx.lineCap = "round";
                ctx.stroke();
                input.value = canvas.toDataURL("image/png");
                e.preventDefault();
              }
              function end(e){
                drawing = false;
                e.preventDefault();
              }
              canvas.addEventListener("mousedown", start);
              canvas.addEventListener("mousemove", move);
              window.addEventListener("mouseup", end);
              canvas.addEventListener("touchstart", start, {passive:false});
              canvas.addEventListener("touchmove", move, {passive:false});
              canvas.addEventListener("touchend", end);

              if(clearBtn){
                clearBtn.addEventListener("click", ()=>{
                  ctx.clearRect(0,0,canvas.width,canvas.height);
                  input.value = "";
                });
              }
            })();
            (function(){
              const form = document.getElementById("openfieldForm");
              const btn  = document.getElementById("submitBtn");
              const hint = document.getElementById("submitHint");
              const reviewSummary = document.getElementById("reviewSummary");
              const saveDraftBtn = document.getElementById("saveDraftBtn");
              const clearDraftBtn = document.getElementById("clearDraftBtn");
              const saveServerDraftBtn = document.getElementById("saveServerDraftBtn");
              const draftStatus = document.getElementById("draftStatus");
              const serverDraftKeyInput = document.getElementById("serverDraftKeyInput");
              const resetDayStatsBtn = document.getElementById("resetDayStatsBtn");
              const submitIntent = document.getElementById("submitIntent");

              const serverDraftsEnabled = {{ "true" if enable_server_drafts else "false" }};
              const gpsRequired = {{ "true" if enable_gps else "false" }};
              const isReview = {{ "true" if review_mode else "false" }};
              const currentStep = {{ current_step }};
              const totalSteps = {{ num_pages }};
              const nextStepUrl = "{{ next_step_url }}";
              const prevStepUrl = "{{ prev_step_url }}";
              const reviewUrl = "{{ review_url }}";
              const editStepUrl = "{{ edit_step_url }}";
              const requiredNames = {{ required_names_json|safe }};

              let locked = false;

              // Draft key is unique per template link (so drafts don't mix)
              const basePath = window.location.pathname.replace(/\/review$/, "");
              const qsParams = new URLSearchParams(window.location.search);
              const assignId = qsParams.get("assign_id") || "";
              const editId = qsParams.get("edit_id") || "";
              const draftParams = new URLSearchParams();
              if(assignId) draftParams.set("assign_id", assignId);
              if(editId) draftParams.set("edit_id", editId);
              const draftPath = draftParams.toString() ? `${basePath}?${draftParams.toString()}` : basePath;
              const draftKey = "openfield_draft_" + draftPath;
              const draftPrefKey = draftKey + "_pref";
              const serverDraftKeyStore = draftKey + "_server_key";

              const draftModal = document.getElementById("draftModal");
              const localDraftSection = document.getElementById("localDraftSection");
              const serverDraftSection = document.getElementById("serverDraftSection");
              const localDraftMeta = document.getElementById("localDraftMeta");
              const serverDraftMeta = document.getElementById("serverDraftMeta");
              const serverDraftLink = document.getElementById("serverDraftLink");
              const resumeLocalBtn = document.getElementById("resumeLocalBtn");
              const resumeServerBtn = document.getElementById("resumeServerBtn");
              const copyResumeLinkBtn = document.getElementById("copyResumeLinkBtn");
              const startFreshBtn = document.getElementById("startFreshBtn");
              const resetDraftPrefBtn = document.getElementById("resetDraftPrefBtn");
              const draftDontAsk = document.getElementById("draftDontAsk");

              let localDraftData = null;
              let serverDraftData = null;
              let serverDraftKey = "";
              let serverDraftResumeUrl = "";

              // -----------------------
              // Multi-step pages
              // -----------------------
              const pages = Array.from(document.querySelectorAll(".page"));
              let pageIndex = Math.max(0, currentStep - 1);

              const prevPageBtn = document.getElementById("prevPageBtn");
              const nextPageBtn = document.getElementById("nextPageBtn");
              const reviewBackBtn = document.getElementById("reviewBackBtn");
              const pageLabel = document.getElementById("pageLabel");
              const progressBar = document.getElementById("progressBar");
              const navHint = document.getElementById("navHint");
              const reviewSection = document.getElementById("reviewSection");
              const reviewSummaryBlock = document.getElementById("reviewSummaryBlock");
              const reviewKeyDetails = document.getElementById("reviewKeyDetails");
              const facilityInput = form.querySelector("[name='facility_name']");
              const facilitySelect = document.getElementById("facilitySelect");
              const facilityList = document.getElementById("facilityList");
              const duplicateHint = document.getElementById("facilityDuplicateHint");
              const duplicateHintText = document.getElementById("facilityDuplicateHintText");
              const projectId = {{ project_id if project_id is not none else "null" }};
              const templateId = {{ template_id if template_id is not none else "null" }};
              const codeGate = {{ "true" if code_gate else "false" }};
              const assignedLocked = {{ "true" if assigned_locked else "false" }};
              const allowUnlisted = {{ "true" if allow_unlisted else "false" }};
              const enumCodeInput = document.getElementById("enumCodeInput");
              const verifyCodeBtn = document.getElementById("verifyCodeBtn");
              const codeStatus = document.getElementById("codeStatus");
              const enumProfilePanel = document.getElementById("enumProfilePanel");
              const enumProfileName = document.getElementById("enumProfileName");
              const enumProfileProject = document.getElementById("enumProfileProject");
              const enumProfileCoverage = document.getElementById("enumProfileCoverage");
              const enumProfileProgress = document.getElementById("enumProfileProgress");
              const enumNameInput = document.getElementById("enumNameInput");
              const facilitySelectGroup = document.getElementById("facilitySelectGroup");
              const facilityTextGroup = document.getElementById("facilityTextGroup");

              // -----------------------
              // Daily counter (device-based)
              // -----------------------
              const dayKey = new Date().toISOString().slice(0,10); // YYYY-MM-DD
              const counterKey = "openfield_daycount_" + draftPath + "_" + dayKey;
              const lastKey = "openfield_lastsubmit_" + draftPath;

              const todayStats = document.getElementById("todayStats");

              function getTodayCount(){
                try { return parseInt(localStorage.getItem(counterKey) || "0", 10) || 0; } catch(e){ return 0; }
              }
              function getLastSubmit(){
                try { return localStorage.getItem(lastKey) || ""; } catch(e){ return ""; }
              }
              function renderTodayStats(){
                if(!todayStats) return;
                const c = getTodayCount();
                const last = getLastSubmit();
                todayStats.innerText = last ? `Today: ${c} submissions • Last: ${last}` : `Today: ${c} submissions on this device`;
              }
              function resetDayStats(){
                try{
                  localStorage.removeItem(counterKey);
                  localStorage.removeItem(lastKey);
                }catch(e){}
                renderTodayStats();
              }

              function setStatus(msg){
                if(draftStatus) draftStatus.innerText = msg;
              }

              function clearMissing(){
                form.querySelectorAll(".missing").forEach(el => el.classList.remove("missing"));
                const notes = form.querySelectorAll(".missing-note");
                notes.forEach(n => n.classList.remove("show"));
              }

              function markMissing(container){
                container.classList.add("missing");
                container.classList.remove("pulse");
                void container.offsetWidth;
                container.classList.add("pulse");
                const note = container.closest(".q")?.querySelector(".missing-note") || container.querySelector(".missing-note");
                if(note) note.classList.add("show");
              }

              function countMissingRequired(scopeEl){
                const scope = scopeEl || form;
                const requiredEls = Array.from(scope.querySelectorAll("[data-required='1']"));
                let missing = 0;

                for(const el of requiredEls){
                  const tag = el.tagName.toLowerCase();
                  if(el.classList.contains("row") || el.classList.contains("multi") || el.classList.contains("single") || el.classList.contains("yesno")){
                    const nameInput = el.querySelector("input");
                    if(!nameInput) continue;
                    const name = nameInput.getAttribute("name");
                    if(!name) continue;
                    const checked = form.querySelectorAll(`input[name="${CSS.escape(name)}"]:checked`);
                    if(checked.length === 0) missing += 1;
                    continue;
                  }
                  if(tag === "select"){
                    const val = (el.value || "").trim();
                    if(!val) missing += 1;
                    continue;
                  }
                  const val = (el.value || "").trim();
                  if(!val) missing += 1;
                }
                return missing;
              }

              function gpsMissing(){
                if(!gpsRequired) return false;
                const lat = form.querySelector("[name='gps_lat']");
                const lng = form.querySelector("[name='gps_lng']");
                return !(lat && lat.value && lng && lng.value);
              }

              function updateReviewSummary(){
                if(!reviewSummary) return;
                const missing = countMissingRequired();
                const gpsMiss = gpsMissing();
                if(missing === 0 && !gpsMiss){
                  reviewSummary.innerText = "";
                  return;
                }
                const parts = [];
                if(missing > 0){
                  parts.push(`${missing} required question${missing === 1 ? "" : "s"} missing`);
                }
                if(gpsMiss){
                  parts.push("GPS missing");
                }
                reviewSummary.innerText = parts.join(" • ");
              }

              function renderReview(data){
                if(!reviewSummaryBlock || !reviewKeyDetails) return;
                const missing = countMissingRequired();
                const gpsMiss = gpsMissing();
                const parts = [];
                parts.push(missing > 0 ? `${missing} required question${missing === 1 ? "" : "s"} missing` : "All required questions answered");
                if(gpsRequired){
                  parts.push(gpsMiss ? "GPS missing" : "GPS captured");
                }
                const filledCount = estimateFilledCount(data || captureDraft());
                parts.push(`${filledCount} fields filled`);
                reviewSummaryBlock.innerText = parts.join(" • ");

                let facility = "—";
                if(facilitySelect && facilitySelect.value){
                  const opt = facilitySelect.options[facilitySelect.selectedIndex];
                  facility = opt ? (opt.text || facilitySelect.value) : facilitySelect.value;
                }else{
                  facility = (form.querySelector("[name='facility_name']") || {}).value || "—";
                }
                const enumerator = (form.querySelector("[name='enumerator_name']") || {}).value || "—";
                const code = (form.querySelector("[name='enumerator_code']") || {}).value || "—";
                const coverageInput = form.querySelector("[name='coverage_node_id']");
                const coverageText = coverageInput && coverageInput.dataset && coverageInput.dataset.coverageLabel
                  ? coverageInput.dataset.coverageLabel
                  : "—";
                const lines = [
                  `Facility: ${facility}`,
                  `Enumerator: ${enumerator}`,
                ];
                if(code && code !== "—") lines.push(`Enumerator code: ${code}`);
                if(coverageInput){
                  lines.push(`Coverage: ${coverageText}`);
                }
                reviewKeyDetails.innerHTML = lines.map(l => `<div>${l}</div>`).join("");
              }

              async function loadFacilitySuggestions(q){
                if(!facilityList || !facilityInput) return;
                if(facilitySelectGroup && facilitySelectGroup.style.display !== "none") return;
                const params = new URLSearchParams();
                if(q) params.set("q", q);
                if(projectId){
                  params.set("project_id", String(projectId));
                }
                try{
                  const res = await fetch(`/facilities/suggest?${params.toString()}`);
                  if(!res.ok) return;
                  const items = await res.json();
                  facilityList.innerHTML = (items || []).map(n=>`<option value="${n}"></option>`).join("");
                }catch(e){}
              }

              async function checkDuplicate(){
                const hintEl = (facilitySelectGroup && facilitySelectGroup.style.display !== "none") ? duplicateHint : duplicateHintText;
                if(!hintEl) return;
                let facility = "";
                if(facilitySelect && facilitySelect.value){
                  const opt = facilitySelect.options[facilitySelect.selectedIndex];
                  facility = opt ? (opt.text || facilitySelect.value) : facilitySelect.value;
                }else if(facilityInput && facilityInput.value){
                  facility = facilityInput.value.trim();
                }
                const enumerator = (form.querySelector("[name='enumerator_name']") || {}).value || "";
                if(!facility || !enumerator){
                  hintEl.style.display = "none";
                  return;
                }
                const params = new URLSearchParams({
                  facility_name: facility,
                  enumerator_name: enumerator,
                });
                if(projectId){
                  params.set("project_id", String(projectId));
                }
                try{
                  const res = await fetch(`/surveys/duplicate_check?${params.toString()}`);
                  if(!res.ok) return;
                  const data = await res.json();
                  if(data && data.duplicate){
                    hintEl.innerText = "Heads up: This facility already has a submission from you today. You can still continue.";
                    hintEl.style.display = "block";
                  }else{
                    hintEl.style.display = "none";
                  }
                }catch(e){}
              }

              function setFormLocked(lockedState){
                if(!form) return;
                const controls = Array.from(form.querySelectorAll("input, select, textarea, button"));
                controls.forEach(el => {
                  const keepEnabled = el === enumCodeInput || el === verifyCodeBtn;
                  if(keepEnabled) return;
                  if(lockedState){
                    el.setAttribute("disabled", "disabled");
                  }else{
                    el.removeAttribute("disabled");
                  }
                });
              }

              function setFacilityMode(useSelect, facilities){
                if(useSelect){
                  if(facilitySelectGroup) facilitySelectGroup.style.display = "block";
                  if(facilityTextGroup) facilityTextGroup.style.display = "none";
                  if(facilitySelect){
                    facilitySelect.setAttribute("data-required", "1");
                    const opts = (facilities || []).map(f => {
                      const done = (f.status || "").toUpperCase() === "DONE";
                      const label = `${f.name || "—"}${done ? " (Done)" : ""}`;
                      return `<option value="${f.id}">${label}</option>`;
                    }).join("");
                    facilitySelect.innerHTML = `<option value=""></option>` + opts;
                  }
                  if(facilityInput){
                    facilityInput.removeAttribute("data-required");
                  }
                }else{
                  if(facilitySelectGroup) facilitySelectGroup.style.display = "none";
                  if(facilityTextGroup) facilityTextGroup.style.display = "block";
                  if(facilitySelect){
                    facilitySelect.removeAttribute("data-required");
                  }
                  if(facilityInput){
                    facilityInput.setAttribute("data-required", "1");
                  }
                }
              }

              async function verifyEnumeratorCode(){
                if(!enumCodeInput || !projectId) return;
                const code = (enumCodeInput.value || "").trim();
                if(!code){
                  if(codeStatus) codeStatus.innerText = "Enter your code to continue.";
                  return;
                }
                if(codeStatus) codeStatus.innerText = "Verifying…";
                try{
                  const params = new URLSearchParams({
                    code,
                    project_id: String(projectId),
                    template_id: templateId ? String(templateId) : "",
                  });
                  const res = await fetch(`/api/assignments/resolve?${params.toString()}`);
                  const data = await res.json();
                  if(!res.ok || !data.ok){
                    if(codeStatus) codeStatus.innerText = data.error || "Enumerator code not found.";
                    return;
                  }
                  if(codeStatus) codeStatus.innerText = "Code verified.";
                  if(enumProfilePanel) enumProfilePanel.style.display = "block";
                  if(enumProfileName) enumProfileName.innerText = data.enumerator?.name || "";
                  if(enumProfileProject) enumProfileProject.innerText = data.project?.name || "";
                  if(enumProfileCoverage) enumProfileCoverage.innerText = data.assignment?.coverage?.name || "—";
                  if(enumProfileProgress){
                    const target = data.assignment?.target;
                    const completed = data.assignment?.completed || 0;
                    const total = data.assignment?.total || 0;
                    enumProfileProgress.innerText = target ? `${completed}/${target}` : `${completed}/${total}`;
                  }
                  if(enumNameInput){
                    enumNameInput.value = data.enumerator?.name || enumNameInput.value;
                    enumNameInput.setAttribute("readonly", "readonly");
                  }
                  enumCodeInput.setAttribute("readonly", "readonly");
                  const assignInput = form.querySelector("[name='assign_id']");
                  if(assignInput && data.assignment?.id){
                    assignInput.value = data.assignment.id;
                  }
                  const hasFacilities = Array.isArray(data.facilities) && data.facilities.length > 0;
                  if(hasFacilities){
                    setFacilityMode(true, data.facilities);
                  }else{
                    if(allowUnlisted){
                      setFacilityMode(false);
                    }else{
                      if(codeStatus) codeStatus.innerText = "No facilities assigned yet. Contact your supervisor.";
                      setFacilityMode(true, []);
                      return;
                    }
                  }
                  setFormLocked(false);
                }catch(e){
                  if(codeStatus) codeStatus.innerText = "Verification failed. Try again.";
                }
              }

              function currentPageEl(){
                if(pages.length === 1) return pages[0];
                return pages[pageIndex] || null;
              }

              function currentPageMissingCount(){
                const current = currentPageEl();
                if(!current) return 0;
                return countMissingRequired(current);
              }

              function updateSubmitState(){
                if(!btn) return;
                if(!isReview){
                  btn.style.display = "none";
                  btn.disabled = true;
                  return;
                }
                btn.style.display = "";
                const missing = countMissingRequired();
                btn.disabled = false;
                if(missing > 0){
                  if(hint) hint.innerText = `Review required questions before submitting. ${missing} required questions missing.`;
                }
              }

              function buildCoverageSelector(nodes){
                const root = document.getElementById("coverageSelector");
                if(!root || !Array.isArray(nodes) || nodes.length === 0){
                  return;
                }
                const input = form.querySelector("[name='coverage_node_id']");
                const byParent = {};
                const byId = {};
                nodes.forEach(n=>{
                  byId[n.id] = n;
                  const pid = n.parent_id || 0;
                  if(!byParent[pid]) byParent[pid] = [];
                  byParent[pid].push(n);
                });

                const preselectId = input ? parseInt(input.value || "0", 10) : 0;
                const chain = [];
                if(preselectId && byId[preselectId]){
                  let cur = byId[preselectId];
                  while(cur){
                    chain.unshift(cur.id);
                    cur = cur.parent_id ? byId[cur.parent_id] : null;
                  }
                }

                function renderLevel(parentId, level){
                  const options = byParent[parentId || 0] || [];
                  if(options.length === 0) return;
                  const select = document.createElement("select");
                  select.className = "coverage-level";
                  select.innerHTML = `<option value="">Select</option>` + options.map(o=>`<option value="${o.id}">${o.name}</option>`).join("");
                  select.addEventListener("change", ()=>{
                    // remove deeper selects
                    while(select.nextSibling) select.nextSibling.remove();
                    const val = parseInt(select.value || "0", 10);
                    if(input){
                      input.value = val ? String(val) : "";
                      input.dataset.coverageLabel = val && byId[val] ? buildLabel(byId[val]) : "";
                    }
                    if(val){
                      renderLevel(val, level + 1);
                    }
                    updateReviewSummary();
                  });
                  root.appendChild(select);
                  return select;
                }

                function buildLabel(node){
                  const parts = [];
                  let cur = node;
                  let guard = 0;
                  while(cur && guard < 10){
                    parts.unshift(cur.name || "");
                    cur = cur.parent_id ? byId[cur.parent_id] : null;
                    guard += 1;
                  }
                  return parts.filter(Boolean).join(" / ");
                }

                // render initial chain if preselected
                let parentId = 0;
                if(chain.length > 0){
                  chain.forEach((nodeId, idx)=>{
                    const select = renderLevel(parentId, idx);
                    if(select){
                      select.value = String(nodeId);
                      parentId = nodeId;
                    }
                  });
                  if(input && byId[chain[chain.length - 1]]){
                    input.dataset.coverageLabel = buildLabel(byId[chain[chain.length - 1]]);
                  }
                  // continue to next level if children exist
                  renderLevel(parentId, chain.length);
                }else{
                  renderLevel(0, 0);
                }
              }

              function firstScrollable(el){
                // If input itself is visible, scroll to it; else scroll to its parent question card.
                const q = el.closest(".q") || el;
                return q;
              }

              function focusFirstInput(container){
                const target = container.querySelector("input, textarea, select, button");
                if(target && typeof target.focus === "function"){
                  target.focus({ preventScroll: true });
                }
              }

              function validateRequired(){
                clearMissing();

                // required single inputs (text/textarea/select)
                const requiredEls = Array.from(form.querySelectorAll("[data-required='1']"));

                // For multi/radio wrappers, they are also marked data-required
                // We'll validate wrappers separately
                let firstBad = null;

                for(const el of requiredEls){
                  const tag = el.tagName.toLowerCase();

                  // Wrapper validation (radio/multi)
                  if(el.classList.contains("row") || el.classList.contains("multi") || el.classList.contains("single")){
                    const nameInput = el.querySelector("input");
                    if(!nameInput) continue;
                    const name = nameInput.getAttribute("name");
                    if(!name) continue;

                    const checked = form.querySelectorAll(`input[name="${CSS.escape(name)}"]:checked`);
                    if(checked.length === 0){
                      markMissing(el);
                      if(!firstBad) firstBad = el;
                    }
                    continue;
                  }

                  // Normal input/textarea/select
                  if(tag === "select"){
                    const val = (el.value || "").trim();
                    if(!val){
                      markMissing(el);
                      if(!firstBad) firstBad = el;
                    }
                    continue;
                  }

                  const val = (el.value || "").trim();
                  if(!val){
                    markMissing(el);
                    if(!firstBad) firstBad = el;
                  }
                }

                if(firstBad){
                  updateReviewSummary();
                  hint.innerText = "Missing required fields. Please complete the highlighted items.";
                  const target = firstScrollable(firstBad);
                  target.scrollIntoView({ behavior: "smooth", block: "center" });
                  focusFirstInput(target);
                  return false;
                }

                updateReviewSummary();
                return true;
              }

              function showPage(i){
                if(pages.length === 0){
                  updateSubmitState();
                  return;
                }
                pageIndex = Math.max(0, i);
                pages.forEach((p)=> p.style.display = "block");
                if(pageLabel){
                  pageLabel.innerText = isReview ? "Review" : `Section ${currentStep} of ${totalSteps}`;
                }
                if(prevPageBtn) prevPageBtn.disabled = !prevStepUrl;
                if(nextPageBtn){
                  nextPageBtn.innerText = (currentStep === totalSteps) ? "Review & Submit" : "Next";
                }
                updateSubmitState();
                window.scrollTo({ top: 0, behavior: "smooth" });
              }

              function validateCurrentPageRequired(){
                // only validate required fields inside current page
                clearMissing();
                const current = currentPageEl();
                if(!current) return true;

                const requiredEls = Array.from(current.querySelectorAll("[data-required='1']"));
                let firstBad = null;

                for(const el of requiredEls){
                  const tag = el.tagName.toLowerCase();

                  // wrapper groups
                  if(el.classList.contains("row") || el.classList.contains("yesno") || el.classList.contains("multi") || el.classList.contains("single")){
                    const nameInput = el.querySelector("input");
                    if(!nameInput) continue;
                    const name = nameInput.getAttribute("name");
                    const checked = form.querySelectorAll(`input[name="${CSS.escape(name)}"]:checked`);
                    if(checked.length === 0){
                      markMissing(el);
                      if(!firstBad) firstBad = el;
                    }
                    continue;
                  }

                  if(tag === "select"){
                    const val = (el.value || "").trim();
                    if(!val){
                      markMissing(el);
                      if(!firstBad) firstBad = el;
                    }
                    continue;
                  }

                  const val = (el.value || "").trim();
                  if(!val){
                    markMissing(el);
                    if(!firstBad) firstBad = el;
                  }
                }

                if(firstBad){
                  updateReviewSummary();
                  hint.innerText = "Missing required fields in this section. Please complete the highlighted items.";
                  const target = firstScrollable(firstBad);
                  target.scrollIntoView({ behavior:"smooth", block:"center" });
                  focusFirstInput(target);
                  return false;
                }

                updateReviewSummary();
                return true;
              }

              // -----------------------
              // Draft: capture & apply
              // -----------------------
              function captureDraft(){
                const data = {
                  ts: new Date().toISOString(),
                  fields: {},
                  checks: {},
                  radios: {},
                  selects: {},
                };

                // text/number/date/email/tel/textarea
                const inputs = form.querySelectorAll("input[name], textarea[name]");
                inputs.forEach(el => {
                  const name = el.getAttribute("name");
                  if(!name) return;

                  const type = (el.getAttribute("type") || "").toLowerCase();
                  if(type === "hidden" && !["gps_lat","gps_lng","gps_accuracy","gps_timestamp"].includes(name)){
                    return;
                  }
                  if(type === "checkbox"){
                    if(!data.checks[name]) data.checks[name] = [];
                    if(el.checked) data.checks[name].push(el.value);
                    return;
                  }

                  if(type === "radio"){
                    if(el.checked) data.radios[name] = el.value;
                    return;
                  }

                  // default
                  data.fields[name] = el.value;
                });

                // selects
                const selects = form.querySelectorAll("select[name]");
                selects.forEach(el => {
                  const name = el.getAttribute("name");
                  if(!name) return;
                  data.selects[name] = el.value;
                });

                // GPS hidden fields
                ["gps_lat","gps_lng","gps_accuracy","gps_timestamp"].forEach(n=>{
                  const el = form.querySelector(`[name="${n}"]`);
                  if(el) data.fields[n] = el.value;
                });

                data.filled_count = estimateFilledCount(data);
                return data;
              }

              function estimateFilledCount(data){
                let count = 0;
                Object.values(data.fields || {}).forEach(val => {
                  if(String(val || "").trim()) count += 1;
                });
                Object.values(data.selects || {}).forEach(val => {
                  if(String(val || "").trim()) count += 1;
                });
                Object.values(data.radios || {}).forEach(val => {
                  if(String(val || "").trim()) count += 1;
                });
                Object.values(data.checks || {}).forEach(vals => {
                  if(Array.isArray(vals) && vals.length) count += 1;
                });
                return count;
              }

              function applyDraft(data, opts){
                if(!data || !data.fields) return;
                const source = (opts && opts.source) || "device";

                // fields
                Object.entries(data.fields).forEach(([name, val]) => {
                  const el = form.querySelector(`[name="${CSS.escape(name)}"]`);
                  if(el) el.value = val ?? "";
                });

                // selects
                if(data.selects){
                  Object.entries(data.selects).forEach(([name, val]) => {
                    const el = form.querySelector(`select[name="${CSS.escape(name)}"]`);
                    if(el) el.value = val ?? "";
                  });
                }

                // radios
                if(data.radios){
                  Object.entries(data.radios).forEach(([name, val]) => {
                    const radio = form.querySelector(`input[type="radio"][name="${CSS.escape(name)}"][value="${CSS.escape(val)}"]`);
                    if(radio) radio.checked = true;
                  });
                }

                // checkboxes
                if(data.checks){
                  Object.entries(data.checks).forEach(([name, vals]) => {
                    const list = Array.isArray(vals) ? vals : [];
                    const boxes = form.querySelectorAll(`input[type="checkbox"][name="${CSS.escape(name)}"]`);
                    boxes.forEach(b => {
                      b.checked = list.includes(b.value);
                    });
                  });
                }

                if(source === "server"){
                  if(opts && opts.draftKey){
                    serverDraftKey = opts.draftKey;
                    if(serverDraftKeyInput) serverDraftKeyInput.value = serverDraftKey;
                  }
                  setStatus("Draft restored from server (" + (data.ts || "unknown time") + ").");
                }else{
                  setStatus("Draft restored from this device (" + (data.ts || "unknown time") + ").");
                }
                updateReviewSummary();
                updateSubmitState();
              }

              function clearHiddenDraftInputs(){
                const existing = form.querySelectorAll("input[data-draft-hidden='1']");
                existing.forEach(el => el.remove());
              }

              function ensureHiddenInput(name, value, isRequired){
                const input = document.createElement("input");
                input.type = "hidden";
                input.name = name;
                input.value = value ?? "";
                input.setAttribute("data-draft-hidden", "1");
                if(isRequired){
                  input.setAttribute("data-required", "1");
                }
                form.appendChild(input);
              }

              function applyDraftToHiddenInputs(data){
                if(!data) return;
                clearHiddenDraftInputs();
                const requiredSet = new Set(requiredNames || []);
                const created = new Set();

                Object.entries(data.fields || {}).forEach(([name, val]) => {
                  ensureHiddenInput(name, val ?? "", requiredSet.has(name));
                  created.add(name);
                });
                Object.entries(data.selects || {}).forEach(([name, val]) => {
                  ensureHiddenInput(name, val ?? "", requiredSet.has(name));
                  created.add(name);
                });
                Object.entries(data.radios || {}).forEach(([name, val]) => {
                  if(val){
                    ensureHiddenInput(name, val, requiredSet.has(name));
                    created.add(name);
                  }
                });
                Object.entries(data.checks || {}).forEach(([name, vals]) => {
                  const list = Array.isArray(vals) ? vals : [];
                  list.forEach(val => ensureHiddenInput(name, val, requiredSet.has(name)));
                  if(list.length) created.add(name);
                  if(list.length === 0 && requiredSet.has(name)){
                    ensureHiddenInput(name, "", true);
                  }
                });
                requiredSet.forEach(name => {
                  if(!created.has(name)){
                    ensureHiddenInput(name, "", true);
                  }
                });
              }

              function saveDraft(silent){
                try{
                  const draft = captureDraft();
                  localStorage.setItem(draftKey, JSON.stringify(draft));
                  if(!silent) setStatus("Draft saved (" + draft.ts + ").");
                  return true;
                }catch(e){
                  if(!silent) setStatus("Draft could not be saved (storage unavailable).");
                  return false;
                }
              }

              function loadLocalDraft(){
                try{
                  const raw = localStorage.getItem(draftKey);
                  if(!raw) return null;
                  const data = JSON.parse(raw);
                  return data;
                }catch(e){
                  setStatus("Draft is corrupted. Start fresh.");
                  try{ localStorage.removeItem(draftKey); }catch(err){}
                  return null;
                }
              }

              function clearDraft(){
                try{
                  localStorage.removeItem(draftKey);
                  setStatus("Draft cleared.");
                }catch(e){
                  setStatus("Could not clear draft.");
                }
              }

              function openDraftModal(){
                if(!draftModal) return;
                draftModal.classList.add("show");
                draftModal.setAttribute("aria-hidden", "false");
                document.body.classList.add("modal-open");
              }

              function closeDraftModal(){
                if(!draftModal) return;
                draftModal.classList.remove("show");
                draftModal.setAttribute("aria-hidden", "true");
                document.body.classList.remove("modal-open");
              }

              function formatDraftMeta(ts, filled){
                const when = ts ? new Date(ts).toLocaleString() : "unknown time";
                const count = Number.isFinite(filled) ? filled : 0;
                return `Saved: ${when} • Filled: ${count} fields`;
              }

              function updateDraftSections(){
                if(localDraftSection){
                  localDraftSection.style.display = localDraftData ? "block" : "none";
                  if(localDraftMeta && localDraftData){
                    const count = Number.isFinite(localDraftData.filled_count)
                      ? localDraftData.filled_count
                      : estimateFilledCount(localDraftData);
                    localDraftMeta.innerText = formatDraftMeta(localDraftData.ts, count);
                  }
                }
                if(serverDraftSection){
                  serverDraftSection.style.display = serverDraftData ? "block" : "none";
                  if(serverDraftMeta && serverDraftData){
                    const count = Number.isFinite(serverDraftData.filled_count)
                      ? serverDraftData.filled_count
                      : estimateFilledCount(serverDraftData.data || {});
                    serverDraftMeta.innerText = formatDraftMeta(serverDraftData.updated_at, count);
                  }
                  if(serverDraftLink){
                    serverDraftLink.style.display = serverDraftResumeUrl ? "block" : "none";
                    serverDraftLink.innerText = serverDraftResumeUrl || "";
                  }
                }
              }

              function storeDraftPreference(value){
                if(draftDontAsk && draftDontAsk.checked){
                  try{ localStorage.setItem(draftPrefKey, value); }catch(e){}
                }
              }

              function resetDraftPreference(){
                try{ localStorage.removeItem(draftPrefKey); }catch(e){}
              }

              async function fetchServerDraftByKey(key){
                if(!serverDraftsEnabled || !key) return null;
                try{
                  const res = await fetch(`${draftPath}/draft?draft=${encodeURIComponent(key)}`);
                  if(!res.ok){
                    const data = await res.json().catch(()=>({}));
                    if(data && data.error){
                      setStatus(data.error);
                    }
                    return null;
                  }
                  const data = await res.json();
                  return data;
                }catch(e){
                  return null;
                }
              }

              async function loadServerDraft(){
                if(!serverDraftsEnabled) return null;
                const urlKey = new URLSearchParams(window.location.search).get("draft");
                const storedKey = (()=>{ try{ return localStorage.getItem(serverDraftKeyStore) || ""; }catch(e){ return ""; } })();
                const key = urlKey || storedKey;
                if(!key) return null;

                const data = await fetchServerDraftByKey(key);
                if(!data || !data.data) return null;

                serverDraftKey = data.draft_key || key;
                serverDraftResumeUrl = data.resume_url || "";
                try{ localStorage.setItem(serverDraftKeyStore, serverDraftKey); }catch(e){}
                return data;
              }

              async function saveServerDraft(){
                if(!serverDraftsEnabled){
                  return;
                }
                const draft = captureDraft();
                const payload = {
                  draft_key: serverDraftKey || "",
                  data: draft,
                  filled_count: draft.filled_count || 0,
                };
                try{
                  const res = await fetch(`${draftPath}/draft`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(payload),
                  });
                  if(!res.ok){
                    setStatus("Server draft could not be saved.");
                    return;
                  }
                  const data = await res.json();
                  serverDraftKey = data.draft_key;
                  serverDraftResumeUrl = data.resume_url || "";
                  try{ localStorage.setItem(serverDraftKeyStore, serverDraftKey); }catch(e){}
                  if(serverDraftLink){
                    serverDraftLink.style.display = "block";
                    serverDraftLink.innerText = serverDraftResumeUrl;
                  }
                  setStatus("Server draft saved. Keep your resume link.");
                }catch(e){
                  setStatus("Server draft could not be saved.");
                }
              }

              async function copyToClipboard(text){
                try{
                  if(navigator.clipboard && window.isSecureContext){
                    await navigator.clipboard.writeText(text);
                    return true;
                  }
                }catch(e){}
                try{
                  const ta = document.createElement("textarea");
                  ta.value = text;
                  ta.style.position = "fixed";
                  ta.style.left = "-9999px";
                  ta.style.top = "-9999px";
                  document.body.appendChild(ta);
                  ta.focus();
                  ta.select();
                  const ok = document.execCommand("copy");
                  document.body.removeChild(ta);
                  return ok;
                }catch(e){
                  return false;
                }
              }

              async function initDraftFlow(){
                localDraftData = loadLocalDraft();
                serverDraftData = await loadServerDraft();
                updateDraftSections();

                if(isReview){
                  const source = localDraftData || (serverDraftData && serverDraftData.data) || null;
                  if(source){
                    applyDraftToHiddenInputs(source);
                    renderReview(source);
                  }else{
                    applyDraftToHiddenInputs({ fields:{}, selects:{}, radios:{}, checks:{} });
                    renderReview({ fields:{}, selects:{}, radios:{}, checks:{} });
                    setStatus("Draft not found. Please go back and complete the form.");
                  }
                  return;
                }

                const pref = (()=>{ try{ return localStorage.getItem(draftPrefKey) || ""; }catch(e){ return ""; } })();
                if(pref === "fresh"){
                  return;
                }
                if(pref === "resume_local" && localDraftData){
                  applyDraft(localDraftData, { source: "device" });
                  return;
                }
                if(pref === "resume_server" && serverDraftData){
                  applyDraft(serverDraftData.data, { source: "server", draftKey: serverDraftKey });
                  return;
                }
                if(localDraftData || serverDraftData){
                  openDraftModal();
                }
              }

              // Restore draft on load (prompted)
              renderTodayStats();
              showPage(pageIndex);
              updateReviewSummary();
              updateSubmitState();
              initDraftFlow();
              if(!{{ "true" if assigned_locked else "false" }}){
                buildCoverageSelector({{ coverage_nodes_json|safe }});
              }else{
                const input = form.querySelector("[name='coverage_node_id']");
                if(input){
                  input.dataset.coverageLabel = "{{ assigned_node.get('name') if assigned_node else '' }}";
                }
              }

              // Auto-save (debounced)
              let t = null;
              form.addEventListener("input", () => {
                if(hint.innerText.includes("Missing required")){
                  hint.innerText = "Review required questions before submitting.";
                }
                updateReviewSummary();
                updateSubmitState();
                if(isReview){
                  const source = localDraftData || (serverDraftData && serverDraftData.data);
                  if(source){
                    applyDraftToHiddenInputs(source);
                    renderReview(source);
                  }
                }
                if(t) clearTimeout(t);
                t = setTimeout(()=>saveDraft(true), 700);
              });

              if(facilityInput){
                facilityInput.addEventListener("input", (e)=>{
                  const q = (e.target.value || "").trim();
                  if(q.length >= 2){
                    loadFacilitySuggestions(q);
                  }
                });
                facilityInput.addEventListener("blur", ()=>checkDuplicate());
              }
              const enumInput = form.querySelector("[name='enumerator_name']");
              if(enumInput){
                enumInput.addEventListener("blur", ()=>checkDuplicate());
              }
              if(facilitySelect){
                facilitySelect.addEventListener("change", ()=>checkDuplicate());
              }

              if(codeGate && !assignedLocked){
                setFormLocked(true);
                if(codeStatus) codeStatus.innerText = "Enter your enumerator code to unlock the form.";
                if(verifyCodeBtn){
                  verifyCodeBtn.addEventListener("click", ()=>verifyEnumeratorCode());
                }
                if(enumCodeInput){
                  enumCodeInput.addEventListener("keydown", (e)=>{
                    if(e.key === "Enter"){
                      e.preventDefault();
                      verifyEnumeratorCode();
                    }
                  });
                }
              }

              // Manual buttons
              if(saveDraftBtn){
                saveDraftBtn.addEventListener("click", ()=>saveDraft(false));
              }
              if(clearDraftBtn){
                clearDraftBtn.addEventListener("click", ()=>{
                  if(confirm("Clear saved draft for this form on this device?")){
                    clearDraft();
                  }
                });
              }
              if(saveServerDraftBtn){
                saveServerDraftBtn.addEventListener("click", ()=>saveServerDraft());
              }
              if(resetDayStatsBtn){
                resetDayStatsBtn.addEventListener("click", ()=>{
                  if(confirm("Reset today's submission stats for this form on this device?")){
                    resetDayStats();
                  }
                });
              }

              if(resumeLocalBtn){
                resumeLocalBtn.addEventListener("click", ()=>{
                  if(localDraftData){
                    applyDraft(localDraftData, { source: "device" });
                    storeDraftPreference("resume_local");
                  }
                  closeDraftModal();
                });
              }
              if(resumeServerBtn){
                resumeServerBtn.addEventListener("click", ()=>{
                  if(serverDraftData && serverDraftData.data){
                    applyDraft(serverDraftData.data, { source: "server", draftKey: serverDraftKey });
                    storeDraftPreference("resume_server");
                  }
                  closeDraftModal();
                });
              }
              if(startFreshBtn){
                startFreshBtn.addEventListener("click", ()=>{
                  storeDraftPreference("fresh");
                  closeDraftModal();
                });
              }
              if(resetDraftPrefBtn){
                resetDraftPrefBtn.addEventListener("click", ()=>{
                  resetDraftPreference();
                  if(draftDontAsk) draftDontAsk.checked = false;
                  setStatus("Draft preference reset.");
                });
              }
              if(copyResumeLinkBtn){
                copyResumeLinkBtn.addEventListener("click", async ()=>{
                  if(serverDraftResumeUrl){
                    const ok = await copyToClipboard(serverDraftResumeUrl);
                    setStatus(ok ? "Resume link copied." : "Could not copy resume link.");
                  }
                });
              }

              if(prevPageBtn){
                prevPageBtn.addEventListener("click", ()=>{
                  if(prevStepUrl){
                    window.location.href = prevStepUrl;
                  }
                });
              }
              if(reviewBackBtn){
                reviewBackBtn.addEventListener("click", ()=>{
                  if(editStepUrl){
                    window.location.href = editStepUrl;
                  }
                });
              }
              if(nextPageBtn){
                nextPageBtn.addEventListener("click", ()=>{
                  // Validate only this section before moving forward
                  const ok = validateCurrentPageRequired();
                  if(!ok) return;

                  if(currentStep < totalSteps){
                    if(nextStepUrl) window.location.href = nextStepUrl;
                    return;
                  }
                  if(reviewUrl) window.location.href = reviewUrl;
                });
              }

              // Submit handler: validate, lock, save, then submit
              form.addEventListener("submit", function(e){
                if(locked){
                  e.preventDefault();
                  return false;
                }
                if(btn && btn.disabled){
                  e.preventDefault();
                  return false;
                }
                if(!isReview){
                  e.preventDefault();
                  return false;
                }
                if(submitIntent) submitIntent.value = "submit";

                const ok = validateRequired();
                if(!ok){
                  e.preventDefault();
                  return false;
                }

                if(!navigator.onLine && window.OPENFIELD_OFFLINE_QUEUE){
                  e.preventDefault();
                  window.OPENFIELD_OFFLINE_QUEUE.queueFromForm();
                  return false;
                }

                // Save right before submit (best effort)
                saveDraft(true);

                locked = true;
                btn.disabled = true;
                btn.innerText = "Submitting…";
                hint.innerText = "Please wait. Do not close this page.";
              });

              // If page is unloaded mid-way, best-effort save
              window.addEventListener("beforeunload", ()=>{
                saveDraft(true);
              });
            })();
          </script>

          {{ gps_js|safe }}
          <script>
            window.OPENFIELD_OFFLINE_CONFIG = {{ offline_config_json|safe }};
          </script>
          <script src="{{ url_for('static', filename='offline.js') }}"></script>
        </body>
        </html>
        """,
        template_name=template_row["name"],
        template_desc=template_desc,
        sensitive_notice=sensitive_notice,
        project_notice=project_notice,
        q_pages_html=q_pages_html,
        num_pages=num_pages,
        current_step=step_num,
        progress_pct=(round((step_num / num_pages) * 100) if num_pages else 0),
        err=err,
        total_q=total_q,
        preview_mode=preview_mode,
        back_to_builder=(url_for("ui_template_manage", template_id=template_id) + key_qs()),
        require_enum_code=require_enum_code,
        coverage_block=coverage_block,
        gps_block=gps_block,
        consent_block=consent_block,
        gps_js=gps_js,
        sticky=sticky,
        enable_server_drafts=ENABLE_SERVER_DRAFTS,
        enable_gps=enable_gps,
        enable_coverage=enable_coverage,
        review_mode=review_mode,
        next_step_url=next_step_url,
        prev_step_url=prev_step_url,
        review_url=review_url,
        edit_step_url=edit_step_url,
        required_names_json=json.dumps(required_names),
        coverage_nodes_json=json.dumps(coverage_nodes or []),
        form_action=base_url,
        assign_id=(assign_id or ""),
        assigned_locked=True if assigned_enumerator else False,
        assigned_enumerator=assigned_enumerator,
        assigned_facilities=assigned_facilities,
        assignment_progress=assignment_progress,
        project_name=project_name,
        assigned_coverage=assigned_coverage,
        code_gate=code_gate,
        allow_unlisted=allow_unlisted,
        template_id=template_id,
        assigned_node=assigned_node,
        project_id=project_id,
        collect_email=collect_email,
        prefill_email=prefill_email,
        hide_email=True if collect_email and prefill_email else False,
        edit_id=edit_id,
        offline_config_json=offline_config_json,
    )


@app.route("/f/<token>/sync", methods=["POST"])
@app.route("/p/<int:project_id>/f/<token>/sync", methods=["POST"], endpoint="fill_form_project_sync")
def fill_form_sync(token, project_id=None):
    template_row = get_template_by_token(token)
    if not template_row:
        return jsonify({"ok": False, "error": "Form link inactive."}), 404

    tpl_project_id = row_get(template_row, "project_id")
    if project_id and tpl_project_id and int(tpl_project_id) != int(project_id):
        return jsonify({"ok": False, "error": "Form link inactive."}), 404

    if PROJECT_REQUIRED and not project_id and not tpl_project_id:
        return jsonify({"ok": False, "error": "Project-specific link required."}), 400

    # Project status gate
    proj_to_check = project_id or tpl_project_id
    if proj_to_check:
        project = prj.get_project(int(proj_to_check))
        if project and (project.get("status") or "").upper() != "ACTIVE":
            return jsonify({"ok": False, "error": "Project inactive."}), 403

    if int(row_get(template_row, "is_active", 1) or 1) != 1:
        return jsonify({"ok": False, "error": "Form link inactive."}), 403

    payload = request.get_json(silent=True) or {}
    submission = payload.get("submission") or payload.get("fields") or payload
    if not isinstance(submission, dict):
        return jsonify({"ok": False, "error": "Invalid payload."}), 400

    submission["sync_source"] = "OFFLINE_SYNC"

    form_data = MultiDict()
    for key, val in submission.items():
        if isinstance(val, list):
            for item in val:
                form_data.add(key, str(item))
        elif val is None:
            continue
        else:
            form_data.add(key, str(val))

    try:
        survey_id = save_survey_from_share_link(template_row, form_data)
        return jsonify({"ok": True, "survey_id": survey_id})
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception:
        return jsonify({"ok": False, "error": "Sync failed."}), 500


@app.route("/f/<token>/sync-center", methods=["GET"])
@app.route("/p/<int:project_id>/f/<token>/sync-center", methods=["GET"], endpoint="fill_form_project_sync_center")
def fill_form_sync_center(token, project_id=None):
    template_row = get_template_by_token(token)
    if not template_row:
        return render_template_string("<h2>Form link inactive</h2><p>This form link is inactive.</p>"), 404

    tpl_project_id = row_get(template_row, "project_id")
    if project_id and tpl_project_id and int(tpl_project_id) != int(project_id):
        return render_template_string("<h2>Form link inactive</h2><p>This form link is inactive.</p>"), 404

    if PROJECT_REQUIRED and not project_id and not tpl_project_id:
        return render_template_string(
            "<h2>Project required</h2><p>This form must be opened from a project-specific link.</p>"
        ), 400

    if int(row_get(template_row, "is_active", 1) or 1) != 1:
        return render_template_string("<h2>Form link inactive</h2><p>This form link is inactive.</p>"), 403

    if project_id:
        base_url = url_for("fill_form_project", project_id=int(project_id), token=token)
        sync_url = url_for("fill_form_project_sync", project_id=int(project_id), token=token)
        sync_center_url = url_for("fill_form_project_sync_center", project_id=int(project_id), token=token)
    else:
        base_url = url_for("fill_form", token=token)
        sync_url = url_for("fill_form_sync", token=token)
        sync_center_url = url_for("fill_form_sync_center", token=token)

    offline_config = {
        "syncUrl": sync_url,
        "queueKey": f"{token}:{project_id or 'solo'}",
        "syncCenterUrl": sync_center_url,
    }

    return render_template_string(
        """
        <html>
        <head>
          <title>Sync Center — HurkField</title>
          <meta name="viewport" content="width=device-width, initial-scale=1"/>
          <link rel="preconnect" href="https://fonts.googleapis.com">
          <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
          <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Poppins:wght@600;700;800&display=swap" rel="stylesheet">
          <style>
            :root{
              --bg:#fbfbfd; --card:#ffffff; --text:#0f172a; --muted:#667085;
              --border:#e5e7eb; --primary:#7C3AED; --shadow:0 10px 30px rgba(2,6,23,.06);
              --font-heading:"Poppins", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
              --font-body:"Inter", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
            }
            body{margin:0; background:var(--bg); font-family:var(--font-body); color:var(--text);}
            .wrap{max-width:880px; margin:0 auto; padding:24px 16px 80px;}
            .card{
              background:var(--card);
              border:1px solid var(--border);
              border-radius:20px;
              padding:20px;
              box-shadow:var(--shadow);
            }
            .hero{
              background:linear-gradient(135deg, rgba(124,58,237,.12), rgba(15,23,42,.02));
              border:1px solid rgba(124,58,237,.2);
              border-radius:22px;
              padding:20px;
              box-shadow:var(--shadow);
            }
            .title{font-family:var(--font-heading); font-size:24px; font-weight:800; margin:0;}
            .muted{color:var(--muted); margin-top:8px; line-height:1.6;}
            .btn{padding:10px 14px; border-radius:12px; border:1px solid var(--border); background:#fff; font-weight:600; cursor:pointer; text-decoration:none; color:inherit;}
            .btn.primary{background:linear-gradient(135deg, var(--primary), #8b5cf6); color:#fff; border:none;}
            .row{display:flex; gap:10px; flex-wrap:wrap; align-items:center; margin-top:12px;}
            .offline-card{
              border:1px dashed rgba(124,58,237,.35);
              background:rgba(124,58,237,.06);
              border-radius:16px;
              padding:12px;
            }
            .offline-head{
              display:flex;
              justify-content:space-between;
              align-items:flex-start;
              gap:12px;
              flex-wrap:wrap;
            }
            .offline-title{font-weight:800;}
            .offline-sub{font-size:13px; color:var(--muted); margin-top:4px;}
            .offline-status{
              padding:6px 10px;
              border-radius:999px;
              font-size:11px;
              font-weight:800;
              text-transform:uppercase;
              letter-spacing:.08em;
            }
            .offline-status.online{
              background:rgba(16,185,129,.12);
              color:#047857;
              border:1px solid rgba(16,185,129,.35);
            }
            .offline-status.offline{
              background:rgba(239,68,68,.12);
              color:#b91c1c;
              border:1px solid rgba(239,68,68,.35);
            }
            .offline-actions{
              display:flex;
              gap:8px;
              flex-wrap:wrap;
              margin-top:12px;
            }
            .offline-list{
              margin-top:12px;
              display:grid;
              gap:10px;
            }
            .offline-item{
              display:flex;
              justify-content:space-between;
              align-items:center;
              gap:10px;
              padding:10px 12px;
              border-radius:12px;
              border:1px solid var(--border);
              background:#fff;
            }
            .offline-item .meta{color:var(--muted); font-size:12px;}
            .offline-pill{
              padding:4px 8px;
              border-radius:999px;
              font-size:11px;
              font-weight:700;
              border:1px solid var(--border);
            }
            .offline-pill.pending{
              background:rgba(124,58,237,.12);
              color:#5b21b6;
              border-color:rgba(124,58,237,.35);
            }
            .offline-pill.error{
              background:rgba(239,68,68,.12);
              color:#b91c1c;
              border-color:rgba(239,68,68,.35);
            }
            .offline-pill.syncing{
              background:rgba(59,130,246,.12);
              color:#1d4ed8;
              border-color:rgba(59,130,246,.35);
            }
          </style>
        </head>
        <body>
          <div class="wrap">
            <div class="hero">
              <div class="title">Offline Sync Center</div>
              <div class="muted">Sync pending submissions from this device when the network is available.</div>
              <div class="row">
                <a class="btn primary" href="{{ base_url }}">Back to form</a>
              </div>
            </div>

            <div class="card" style="margin-top:16px">
              <div class="offline-card" id="offlineSyncCard"></div>
            </div>
          </div>

          <script>
            window.OPENFIELD_OFFLINE_CONFIG = {{ offline_config_json|safe }};
          </script>
          <script src="{{ url_for('static', filename='offline.js') }}"></script>
        </body>
        </html>
        """,
        base_url=base_url,
        offline_config_json=json.dumps(offline_config),
    )


@app.route("/f/<token>/review", methods=["GET"])
def fill_form_review(token):
    return fill_form(token, review_mode=True)


@app.route("/p/<int:project_id>/f/<token>/review", methods=["GET"], endpoint="fill_form_project_review")
def fill_form_project_review(project_id, token):
    return fill_form(token, project_id=project_id, review_mode=True)


@app.route("/f/<token>/success")
def form_success(token):
    return _render_form_success(token)


def _render_form_success(token, project_id=None):
    template_row = get_template_by_token(token)
    if not template_row:
        return render_template_string("<h2>Form link inactive</h2><p>This form link is inactive.</p>"), 404

    confirmation_message = (row_get(template_row, "confirmation_message") or "").strip()
    allow_edit_response = int(row_get(template_row, "allow_edit_response", 0) or 0)
    show_summary_charts = int(row_get(template_row, "show_summary_charts", 0) or 0)

    sid = request.args.get("sid", "").strip()
    assign_id = request.args.get("assign_id") or ""
    base_url = url_for("fill_form_project", project_id=int(project_id), token=token) if project_id else url_for("fill_form", token=token)
    summary_url = url_for("form_summary_project", project_id=int(project_id), token=token) if project_id else url_for("form_summary", token=token)
    if assign_id:
        base_url = f"{base_url}?assign_id={assign_id}"
    base_path = base_url

    edit_url = None
    if allow_edit_response and sid:
        sep = "&" if "?" in base_url else "?"
        edit_url = f"{base_url}{sep}edit_id={sid}"

    return render_template_string(
        """
        <html>
        <head>
          <title>Submission successful — {{app_name}}</title>
          <meta name="viewport" content="width=device-width, initial-scale=1"/>
          <link rel="preconnect" href="https://fonts.googleapis.com">
          <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
          <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Poppins:wght@400;500;600;700;800&display=swap" rel="stylesheet">
          <style>
            :root{
              --font-heading:"Poppins", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
              --font-body:"Inter", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
            }
            body{font-family: var(--font-body); margin:18px; background:#f8f9fb;}
            .wrap{max-width:860px; margin:0 auto;}
            .card{border:1px solid #e5e7eb; border-radius:20px; padding:22px; margin:16px 0; background:#fff; box-shadow:0 10px 30px rgba(2,6,23,.06);}
            .ok{color:#065f46; margin:0; font-family:var(--font-heading);}
            .muted{color:#667085; line-height:1.7;}
            .pill{display:inline-flex; gap:8px; align-items:center; background:#ecfdf3; color:#067647; border:1px solid #abefc6; padding:6px 10px; border-radius:999px; font-size:12px; font-weight:600;}
            .btn{padding:10px 14px; border-radius:12px; border:1px solid #111827; background:#fff; text-decoration:none; display:inline-block; font-weight:600;}
            .btn.primary{background:#111827; color:#fff; border-color:#111827;}
            code{background:#f2f4f7; padding:2px 6px; border-radius:8px;}
            .row{display:flex; gap:12px; flex-wrap:wrap; align-items:center;}
            .stat{display:flex; flex-direction:column; gap:4px; padding:10px 12px; border-radius:12px; background:#f9fafb; border:1px solid #eef2f6;}
          </style>
        </head>
        <body>
          <div class="wrap">
            <div class="card">
              <div class="pill">✅ Submission saved</div>
              <h2 class="ok" style="margin-top:12px">Great work — response received</h2>
              <p class="muted">
                {{ confirmation_message if confirmation_message else ("Your response has been recorded for " ~ template_name ~ ".") }}
              </p>
              {% if sid %}
                <p class="muted">Reference ID: <code>#{{sid}}</code></p>
              {% endif %}

              <div class="row" style="margin-top:12px">
                <div class="stat">
                  <div class="muted">Today’s submissions</div>
                  <div style="font-weight:700" id="todayCount">0</div>
                </div>
                <div class="stat">
                  <div class="muted">Last submitted</div>
                  <div style="font-weight:700" id="lastSubmit">—</div>
                </div>
              </div>

              <div class="row" style="margin-top:14px">
                <a class="btn primary" href="{{ base_url }}">Submit another</a>
                {% if edit_url %}
                <a class="btn" href="{{ edit_url }}">Edit response</a>
                {% endif %}
                {% if show_summary_charts %}
                <a class="btn" href="{{ summary_url }}">View summary</a>
                {% endif %}
                <a class="btn" href="{{ url_for('landing') }}">Back to home</a>
              </div>

              <p class="muted" style="margin-top:14px">
                You can continue with the next facility when you’re ready.
              </p>
              <p class="muted" style="margin-top:6px">
                Redirecting in <span id="redirectCount">3</span>s…
              </p>
            </div>
          </div>

          <script>
            // Clear draft for this form on success
            try { localStorage.removeItem("openfield_draft_" + "{{ base_path }}"); } catch(e) {}
            try { localStorage.removeItem("openfield_draft_" + "{{ base_path }}" + "_pref"); } catch(e) {}
            try { localStorage.removeItem("openfield_draft_" + "{{ base_path }}" + "_server_key"); } catch(e) {}
            (function(){
              const dayKey = new Date().toISOString().slice(0,10);
              const path = "{{ base_path }}";
              const counterKey = "openfield_daycount_" + path + "_" + dayKey;
              const lastKey = "openfield_lastsubmit_" + path;

              try{
                const c = parseInt(localStorage.getItem(counterKey) || "0", 10) || 0;
                localStorage.setItem(counterKey, String(c + 1));
                localStorage.setItem(lastKey, new Date().toLocaleTimeString());
                var today = document.getElementById("todayCount");
                var last = document.getElementById("lastSubmit");
                if(today) today.textContent = String(c + 1);
                if(last) last.textContent = new Date().toLocaleTimeString();
              }catch(e){}

              // Clear draft after success (keep your existing draft clearing too)
              try { localStorage.removeItem("openfield_draft_" + path); } catch(e) {}
            })();
            (function(){
              var seconds = 3;
              var el = document.getElementById("redirectCount");
              var timer = setInterval(function(){
                seconds -= 1;
                if(el) el.textContent = String(Math.max(seconds, 0));
                if(seconds <= 0){
                  clearInterval(timer);
                  window.location.href = "{{ base_url }}";
                }
              }, 1000);
            })();
          </script>
        </body>
        </html>
        """,
        app_name=APP_NAME,
        template_name=template_row["name"],
        confirmation_message=confirmation_message,
        token=token,
        sid=sid,
        base_url=base_url,
        base_path=base_path,
        edit_url=edit_url,
        show_summary_charts=show_summary_charts,
        summary_url=summary_url,
    )


@app.route("/p/<int:project_id>/f/<token>/success")
def form_success_project(project_id, token):
    return _render_form_success(token, project_id=project_id)


@app.route("/f/<token>/summary", methods=["GET"])
@app.route("/p/<int:project_id>/f/<token>/summary", methods=["GET"], endpoint="form_summary_project")
def form_summary(token, project_id=None):
    template_row = get_template_by_token(token)
    if not template_row:
        return render_template_string("<h2>Form link inactive</h2><p>This form link is inactive.</p>"), 404

    if project_id and int(row_get(template_row, "project_id") or 0) != int(project_id):
        return render_template_string("<h2>Form link inactive</h2><p>This form link is inactive.</p>"), 404

    if int(row_get(template_row, "is_active", 1) or 1) != 1:
        return render_template_string("<h2>Form link inactive</h2><p>This form link is inactive.</p>"), 403

    show_summary_charts = int(row_get(template_row, "show_summary_charts", 0) or 0)
    if show_summary_charts != 1:
        return render_template_string("<h2>Summary disabled</h2><p>This form does not publish a summary.</p>"), 403

    template_id = int(template_row["id"])
    summaries = _build_response_summary(template_id)
    base_url = url_for("fill_form_project", project_id=int(project_id), token=token) if project_id else url_for("fill_form", token=token)

    return render_template_string(
        """
        <html>
        <head>
          <title>Response summary — {{ template_name }}</title>
          <meta name="viewport" content="width=device-width, initial-scale=1"/>
          <link rel="preconnect" href="https://fonts.googleapis.com">
          <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
          <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Poppins:wght@400;600;700;800&display=swap" rel="stylesheet">
          <style>
            :root{
              --primary:#7C3AED;
              --primary-500:#8B5CF6;
              --text:#0f172a;
              --muted:#667085;
              --border:#e5e7eb;
              --card:#fff;
              --bg:#f8f9fb;
              --font-heading:"Poppins", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
              --font-body:"Inter", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
            }
            body{margin:0; background:var(--bg); color:var(--text); font-family:var(--font-body);}
            .wrap{max-width:980px; margin:0 auto; padding:24px 16px 80px;}
            .hero{
              background:
                radial-gradient(220px 140px at 10% 10%, rgba(124,58,237,.18), transparent 60%),
                linear-gradient(135deg, rgba(124,58,237,.10), rgba(15,23,42,.02));
              border:1px solid rgba(124,58,237,.25);
              border-radius:24px;
              padding:22px;
              box-shadow:0 18px 46px rgba(15,18,34,.10);
            }
            .h1{font-family:var(--font-heading); font-size:24px; margin:0;}
            .muted{color:var(--muted);}
            .card{
              background:var(--card);
              border:1px solid var(--border);
              border-radius:18px;
              padding:18px;
              margin-top:16px;
              box-shadow:0 12px 28px rgba(15,18,34,.06);
            }
            .q-title{font-weight:700; font-size:15px;}
            .q-meta{color:var(--muted); font-size:12px; margin-top:6px;}
            .bar-row{margin-top:10px;}
            .bar-label{display:flex; justify-content:space-between; font-size:13px;}
            .bar{
              height:8px;
              background:#eef2f7;
              border-radius:999px;
              overflow:hidden;
              margin-top:6px;
            }
            .bar > span{
              display:block;
              height:100%;
              background:linear-gradient(135deg, var(--primary), var(--primary-500));
            }
            .btn{
              display:inline-flex;
              align-items:center;
              gap:8px;
              padding:10px 14px;
              border-radius:12px;
              border:1px solid #111827;
              background:#111827;
              color:#fff;
              text-decoration:none;
              font-weight:600;
            }
            .sample{
              margin-top:10px;
              display:flex;
              flex-direction:column;
              gap:8px;
              font-size:13px;
            }
            .sample div{
              padding:8px 10px;
              border:1px solid var(--border);
              border-radius:12px;
              background:#fafafa;
            }
            .stat-grid{
              display:grid;
              grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
              gap:10px;
              margin-top:10px;
            }
            .stat{
              padding:10px 12px;
              border-radius:12px;
              border:1px solid var(--border);
              background:#fafafa;
              font-size:13px;
            }
          </style>
        </head>
        <body>
          <div class="wrap">
            <div class="hero">
              <div class="h1">Response summary</div>
              <div class="muted" style="margin-top:6px">Live summary of responses for {{ template_name }}.</div>
              <div style="margin-top:12px">
                <a class="btn" href="{{ base_url }}">Back to form</a>
              </div>
            </div>

            {% if summaries %}
              {% for s in summaries %}
                <div class="card">
                  <div class="q-title">{{ s.text }}</div>
                  <div class="q-meta">{{ s.total }} responses</div>

                  {% if s.kind == 'choice' %}
                    {% if s.items %}
                      {% for item in s.items %}
                        <div class="bar-row">
                          <div class="bar-label">
                            <span>{{ item.label }}</span>
                            <span>{{ item.count }} ({{ item.pct }}%)</span>
                          </div>
                          <div class="bar"><span style="width: {{ item.pct }}%"></span></div>
                        </div>
                      {% endfor %}
                    {% else %}
                      <div class="muted" style="margin-top:10px">No responses yet.</div>
                    {% endif %}
                  {% elif s.kind == 'number' %}
                    {% if s.stats %}
                      <div class="stat-grid">
                        <div class="stat"><b>Min</b><div>{{ s.stats.min }}</div></div>
                        <div class="stat"><b>Avg</b><div>{{ s.stats.avg }}</div></div>
                        <div class="stat"><b>Max</b><div>{{ s.stats.max }}</div></div>
                      </div>
                    {% else %}
                      <div class="muted" style="margin-top:10px">No responses yet.</div>
                    {% endif %}
                  {% else %}
                    {% if s.samples %}
                      <div class="sample">
                        {% for sample in s.samples %}
                          <div>{{ sample }}</div>
                        {% endfor %}
                      </div>
                    {% else %}
                      <div class="muted" style="margin-top:10px">No responses yet.</div>
                    {% endif %}
                  {% endif %}
                </div>
              {% endfor %}
            {% else %}
              <div class="card">
                <div class="muted">No questions or responses yet.</div>
              </div>
            {% endif %}
          </div>
        </body>
        </html>
        """,
        template_name=template_row["name"],
        summaries=summaries,
        base_url=base_url,
    )


# ---------------------------
# Supervisor UI
# ---------------------------
@app.route("/ui")
def ui_home():
    gate = admin_gate()
    if gate:
        return gate

    return redirect(url_for("ui_dashboard") + (f"?key={ADMIN_KEY}" if ADMIN_KEY else ""))


@app.route("/ui/dashboard")
def ui_dashboard():
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    is_admin = bool(ADMIN_KEY and request.args.get("key") == ADMIN_KEY)
    org_id = current_org_id() if (REQUIRE_SUPERVISOR_KEY and not is_admin) else None
    if org_id is None:
        try:
            sess_org = session.get("org_id")
            org_id = int(sess_org) if sess_org is not None else None
        except Exception:
            org_id = None
    user = getattr(g, "user", None)
    user_name = ""
    if user:
        user_name = (user.get("full_name") or "").strip()
    if not user_name:
        user_name = (session.get("user_name") or "").strip()
    org_name = ""
    if org_id is not None:
        try:
            org = prj.get_organization(int(org_id))
            if org:
                org_name = org.get("name") or ""
        except Exception:
            org_name = ""
    projects = prj.list_projects(12, organization_id=org_id)
    first_project_id = int(projects[0].get("id")) if projects else None
    total_surveys = 0
    qa_alert_count = 0
    with get_conn() as conn:
        cur = conn.cursor()
        if org_id is not None:
            try:
                cur.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM surveys s
                    JOIN projects p ON p.id = s.project_id
                    WHERE p.organization_id=?
                    """,
                    (int(org_id),),
                )
            except Exception:
                cur.execute("SELECT COUNT(*) AS c FROM surveys WHERE deleted_at IS NULL")
        else:
            cur.execute("SELECT COUNT(*) AS c FROM surveys WHERE deleted_at IS NULL")
        row = cur.fetchone()
        total_surveys = int(row["c"] or 0) if row else 0
    if total_surveys:
        try:
            if org_id is not None:
                # Count alerts across org projects (approx)
                sup_id = current_supervisor_id()
                qa_alert_count = len(
                    sup.qa_alerts_dashboard(limit=50, supervisor_id=str(sup_id) if sup_id else "")
                )
            else:
                sup_id = current_supervisor_id()
                qa_alert_count = len(
                    sup.qa_alerts_dashboard(limit=50, supervisor_id=str(sup_id) if sup_id else "")
                )
        except Exception:
            qa_alert_count = 0
    active_projects = 0
    draft_projects = 0
    archived_projects = 0
    project_rows = []
    recent_projects = []
    for p in projects:
        pid = int(p.get("id"))
        overview = prj.project_overview(pid)
        status = (p.get("status") or "DRAFT").upper()
        if status == "ACTIVE":
            active_projects += 1
        elif status == "DRAFT":
            draft_projects += 1
        else:
            archived_projects += 1
        status_class = (
            "bg-emerald-100 text-emerald-700"
            if status == "ACTIVE"
            else ("bg-amber-100 text-amber-700" if status == "DRAFT" else "bg-slate-200 text-slate-600")
        )
        total = int(overview.get("total_submissions") or 0)
        completed = int(overview.get("completed_submissions") or 0)
        completion_pct = int(round((completed * 100.0 / total), 0)) if total > 0 else 0
        open_href = f"{url_for('ui_project_detail', project_id=pid)}{key_q}"
        project_rows.append(
            f"""
            <tr class="border-b border-slate-100 hover:bg-slate-50">
              <td class="px-3 py-2">
                <a class="font-semibold text-slate-900 hover:text-brand" href="{open_href}">{html.escape((p.get("name") or "Project").strip())}</a>
              </td>
              <td class="px-3 py-2"><span class="rounded-full px-2 py-0.5 text-xs font-semibold {status_class}">{status.title()}</span></td>
              <td class="px-3 py-2 text-slate-600">{total}</td>
              <td class="px-3 py-2 text-slate-600">{completed}</td>
              <td class="px-3 py-2 text-slate-600">{completion_pct}%</td>
            </tr>
            """
        )
        recent_projects.append(
            f"""
            <a href="{open_href}" class="block rounded-xl border border-slate-200 bg-white px-3 py-2 hover:border-brand/30 hover:bg-violet-50 transition">
              <div class="text-sm font-semibold text-slate-900">{html.escape((p.get("name") or "Project").strip())}</div>
              <div class="mt-1 text-xs text-slate-500">{total} submissions · {completed} completed</div>
            </a>
            """
        )

    page_html = f"""
<div class="min-h-screen bg-[#DCD3F8] py-8">
  <style>
    .hf-side-link {{
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 10px 12px;
      border-radius: 12px;
      color: #475569;
      font-weight: 600;
      border: 1px solid transparent;
      transition: all .2s ease;
    }}
    .hf-side-link:hover {{
      background: #F8FAFC;
      border-color: #E2E8F0;
      color: #1E293B;
    }}
    .hf-side-link.active {{
      background: #EEEAFE;
      border-color: #DDD6FE;
      color: #6D28D9;
      box-shadow: 0 8px 18px rgba(124, 58, 237, .14);
    }}
    .hf-side-link .hf-ic {{
      width: 28px;
      height: 28px;
      border-radius: 9px;
      border: 1px solid #E2E8F0;
      background: #FFFFFF;
      color: #64748B;
      display: grid;
      place-items: center;
      flex: none;
    }}
    .hf-side-link.active .hf-ic {{
      border-color: #7C3AED;
      background: #7C3AED;
      color: #FFFFFF;
    }}
    .hf-side-link svg {{
      width: 14px;
      height: 14px;
      stroke: currentColor;
      fill: none;
      stroke-width: 2;
      stroke-linecap: round;
      stroke-linejoin: round;
    }}
  </style>
  <div class="max-w-7xl mx-auto px-4">
    <div class="overflow-hidden rounded-[30px] border border-white/75 bg-white shadow-[0_35px_70px_rgba(74,46,151,0.22)]">
      <div class="grid lg:grid-cols-[220px_1fr_320px]">
        <aside class="border-r border-slate-200 bg-white p-5">
          <div class="mb-2">
            <img
              src="/static/logos/hurkfield.jpeg"
              alt="HurkField logo"
              class="h-16 w-auto max-w-[180px] object-contain drop-shadow-[0_10px_20px_rgba(124,58,237,0.22)]"
            />
            <div class="mt-2 text-sm font-extrabold text-slate-900">HurkField</div>
            <div class="text-[11px] text-slate-500">{html.escape(org_name or 'Workspace')}</div>
          </div>

          <nav class="mt-6 space-y-2 text-sm">
            <a class="hf-side-link active" href="/ui/dashboard{key_q}">
              <span class="hf-ic"><svg viewBox="0 0 24 24"><path d="M3 13h8V3H3zM13 21h8V11h-8zM13 3v6h8V3zM3 21h8v-6H3z"></path></svg></span>
              <span>Dashboard</span>
            </a>
            <a class="hf-side-link" href="/ui/projects{key_q}">
              <span class="hf-ic"><svg viewBox="0 0 24 24"><path d="M3 7h6l2 2h10v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"></path></svg></span>
              <span>Projects</span>
            </a>
            <a class="hf-side-link" href="/ui/templates{key_q}">
              <span class="hf-ic"><svg viewBox="0 0 24 24"><path d="M14 3H6a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path><path d="M14 3v6h6"></path><path d="M8 13h8M8 17h6"></path></svg></span>
              <span>Templates</span>
            </a>
            <a class="hf-side-link" href="/ui/surveys{key_q}">
              <span class="hf-ic"><svg viewBox="0 0 24 24"><path d="M9 11l3 3L22 4"></path><path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11"></path></svg></span>
              <span>Submissions</span>
            </a>
            <a class="hf-side-link" href="/ui/review{key_q}">
              <span class="hf-ic"><svg viewBox="0 0 24 24"><path d="M9 12l2 2 4-4"></path><path d="M21 12a9 9 0 1 1-9-9"></path></svg></span>
              <span>Review</span>
            </a>
            <a class="hf-side-link" href="/ui/qa{key_q}">
              <span class="hf-ic"><svg viewBox="0 0 24 24"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"></path><path d="M9.5 12.5 11 14l3.5-3.5"></path></svg></span>
              <span>QA Alerts</span>
            </a>
            <a class="hf-side-link" href="/ui/onboarding{key_q}">
              <span class="hf-ic"><svg viewBox="0 0 24 24"><path d="M12 2v20"></path><path d="m19 9-7-7-7 7"></path><path d="M5 19h14"></path></svg></span>
              <span>Onboarding</span>
            </a>
            <a class="hf-side-link" href="/ui/admin{key_q}">
              <span class="hf-ic"><svg viewBox="0 0 24 24"><path d="M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6Z"></path><path d="M19.4 15a1.7 1.7 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-1.8-.3 1.7 1.7 0 0 0-1 1.5V21a2 2 0 1 1-4 0v-.2a1.7 1.7 0 0 0-1-1.5 1.7 1.7 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.7 1.7 0 0 0 .3-1.8 1.7 1.7 0 0 0-1.5-1H3a2 2 0 1 1 0-4h.2a1.7 1.7 0 0 0 1.5-1 1.7 1.7 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.7 1.7 0 0 0 1.8.3h.1a1.7 1.7 0 0 0 1-1.5V3a2 2 0 1 1 4 0v.2a1.7 1.7 0 0 0 1 1.5h.1a1.7 1.7 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.7 1.7 0 0 0-.3 1.8v.1a1.7 1.7 0 0 0 1.5 1H21a2 2 0 1 1 0 4h-.2a1.7 1.7 0 0 0-1.5 1z"></path></svg></span>
              <span>Admin</span>
            </a>
          </nav>

          <a class="mt-10 inline-flex items-center gap-2 rounded-lg border border-slate-200 px-3 py-2 text-xs font-semibold text-slate-500 hover:bg-slate-100" href="/logout">◌ Logout</a>
        </aside>

        <main class="bg-[#F3F1FC] p-5 md:p-6">
          <div class="flex flex-wrap items-center justify-between gap-3">
            <div class="relative min-w-[220px] flex-1">
              <input class="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700" type="text" placeholder="Search dashboard..." />
            </div>
            <a
              class="rounded-lg px-4 py-2 text-sm font-semibold"
              href="{(f'/ui/projects/{first_project_id}{key_q}' if first_project_id is not None else f'/ui/projects/new{key_q}')}"
              style="background:#7C3AED;color:#FFFFFF!important;border:1px solid #6D28D9;"
            >Open project</a>
          </div>

          <section class="mt-4 rounded-2xl border border-[#D9CCF8] bg-[#ECE5FF] p-5 shadow-lg">
            <div class="flex items-center justify-between gap-4">
              <div>
                <h2 class="text-2xl font-extrabold text-slate-900">Good day {html.escape((user_name or org_name or 'Research Lead').split()[0])}</h2>
                <p class="mt-1 text-sm text-slate-700">You have {len(projects)} projects, {total_surveys} submissions, and {qa_alert_count} QA alerts.</p>
                <a
                  class="mt-3 inline-flex rounded-md px-3 py-1.5 text-xs font-bold"
                  href="/ui/surveys{key_q}"
                  style="background:#7C3AED;color:#FFFFFF!important;border:1px solid #6D28D9;"
                >Open submissions</a>
              </div>
            </div>
          </section>

          <section class="mt-5">
            <div class="flex items-center justify-between">
              <h3 class="text-sm font-bold text-slate-700">You need to run</h3>
              <a class="text-xs font-semibold text-brand" href="/ui/onboarding{key_q}">View all</a>
            </div>
            <div class="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
              <a class="rounded-xl border border-slate-200 bg-white p-3 shadow-sm hover:shadow" href="/ui/projects/new{key_q}">
                <div class="text-xs font-bold text-slate-700">Create Project</div>
                <div class="mt-1 text-[11px] text-slate-500">Set up a new study scope</div>
              </a>
              <a class="rounded-xl border border-slate-200 bg-white p-3 shadow-sm hover:shadow" href="/ui/templates{key_q}">
                <div class="text-xs font-bold text-slate-700">Build Template</div>
                <div class="mt-1 text-[11px] text-slate-500">Design data collection form</div>
              </a>
              <a class="rounded-xl border border-slate-200 bg-white p-3 shadow-sm hover:shadow" href="/ui/review{key_q}">
                <div class="text-xs font-bold text-slate-700">Review QA</div>
                <div class="mt-1 text-[11px] text-slate-500">{qa_alert_count} alerts currently open</div>
              </a>
              <a class="rounded-xl border border-slate-200 bg-white p-3 shadow-sm hover:shadow" href="/ui/exports{key_q}">
                <div class="text-xs font-bold text-slate-700">Export Data</div>
                <div class="mt-1 text-[11px] text-slate-500">Download CSV and JSON</div>
              </a>
            </div>
          </section>

          <section class="mt-5 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
            <div class="flex items-center justify-between">
              <h3 class="text-sm font-bold text-slate-700">Project progress</h3>
              <a class="text-xs font-semibold text-brand" href="/ui/projects{key_q}">Open projects</a>
            </div>
            <div class="mt-3 overflow-x-auto">
              <table class="min-w-full text-sm">
                <thead>
                  <tr class="bg-slate-50 text-left text-xs uppercase tracking-wide text-slate-500">
                    <th class="px-3 py-2 font-semibold">Project</th>
                    <th class="px-3 py-2 font-semibold">Status</th>
                    <th class="px-3 py-2 font-semibold">Total</th>
                    <th class="px-3 py-2 font-semibold">Completed</th>
                    <th class="px-3 py-2 font-semibold">Progress</th>
                  </tr>
                </thead>
                <tbody>
                  {"".join(project_rows[:7]) if project_rows else "<tr><td colspan='5' class='px-3 py-6 text-center text-slate-500'>No projects yet.</td></tr>"}
                </tbody>
              </table>
            </div>
          </section>
        </main>

        <aside class="border-l border-slate-200 bg-white p-5">
          <div class="flex items-center justify-between">
            <div>
              <div class="text-xs text-slate-500">Hello</div>
              <div class="text-sm font-bold text-slate-900">{html.escape(user_name or org_name or 'User')}</div>
            </div>
            <div class="grid h-9 w-9 place-items-center rounded-full bg-brand text-xs font-bold text-white">{html.escape(((user_name or org_name or 'HF')[0:1]).upper())}</div>
          </div>

          <section class="mt-5 rounded-xl border border-slate-200 bg-slate-50 p-3">
            <div class="flex items-center justify-between">
              <h4 class="text-sm font-bold text-slate-700">Schedule</h4>
              <span class="text-[11px] text-slate-500">Today</span>
            </div>
            <div class="mt-2 grid grid-cols-7 gap-1 text-center text-[11px]">
              <span class="rounded bg-white py-1 text-slate-500">M</span>
              <span class="rounded bg-white py-1 text-slate-500">T</span>
              <span class="rounded bg-brand py-1 font-bold text-white">W</span>
              <span class="rounded bg-white py-1 text-slate-500">T</span>
              <span class="rounded bg-white py-1 text-slate-500">F</span>
              <span class="rounded bg-white py-1 text-slate-500">S</span>
              <span class="rounded bg-white py-1 text-slate-500">S</span>
            </div>
          </section>

          <section class="mt-4">
            <div class="flex items-center justify-between">
              <h4 class="text-sm font-bold text-slate-700">Recent projects</h4>
              <a class="text-[11px] font-semibold text-brand" href="/ui/projects{key_q}">View</a>
            </div>
            <div class="mt-2 space-y-2">
              {"".join(recent_projects[:5]) if recent_projects else "<div class='rounded-xl border border-slate-200 bg-slate-50 px-3 py-4 text-center text-xs text-slate-500'>No projects available.</div>"}
            </div>
          </section>

          <section class="mt-4 rounded-xl border border-slate-200 bg-slate-50 p-3">
            <h4 class="text-sm font-bold text-slate-700">Workspace stats</h4>
            <div class="mt-2 space-y-2 text-xs text-slate-600">
              <div class="flex justify-between"><span>Active projects</span><b>{active_projects}</b></div>
              <div class="flex justify-between"><span>Draft projects</span><b>{draft_projects}</b></div>
              <div class="flex justify-between"><span>Archived projects</span><b>{archived_projects}</b></div>
              <div class="flex justify-between"><span>QA alerts</span><b>{qa_alert_count}</b></div>
            </div>
          </section>
        </aside>
      </div>
    </div>
  </div>
</div>
"""
    return ui_shell("Dashboard", page_html)


@app.route("/ui/onboarding", methods=["GET"])
def ui_onboarding():
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    is_admin = bool(ADMIN_KEY and request.args.get("key") == ADMIN_KEY)
    org_id = current_org_id() if (REQUIRE_SUPERVISOR_KEY and not is_admin) else None
    if org_id is None:
        try:
            sess_org = session.get("org_id")
            org_id = int(sess_org) if sess_org is not None else None
        except Exception:
            org_id = None
    orgs = prj.list_organizations(20)
    org = None
    if org_id is not None:
        try:
            org = prj.get_organization(int(org_id))
        except Exception:
            org = None
    if not org:
        org = orgs[0] if orgs else None

    page_html = f"""
    <div class="card">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div>
          <div class="h1">Institution Onboarding</div>
          <div class="muted">A guided path to your first trusted dataset.</div>
        </div>
        <a class="btn" href="/ui{key_q}">Back to dashboard</a>
      </div>
    </div>

    <div class="card" style="margin-top:16px">
      <div class="h2">Step 1 — Create organization</div>
      <div class="muted">Name and sector establish institutional ownership.</div>
      <div style="margin-top:12px">
        {f"<div><b>{org.get('name')}</b> reported sector: {org.get('sector') or '—'}</div>" if org else "<div class='muted'>No organization yet.</div>"}
        <a class="btn" style="margin-top:12px" href="/ui/organization{key_q}">{'Manage organization' if org else 'Create organization'}</a>
      </div>
    </div>

    <div class="card" style="margin-top:16px">
      <div class="h2">Step 2 — Create project</div>
      <div class="muted">Define a real study with expected submissions.</div>
      <a class="btn" style="margin-top:12px" href="/ui/projects/new{key_q}">Create project</a>
    </div>

    <div class="card" style="margin-top:16px">
      <div class="h2">Step 3 — Build template</div>
      <div class="muted">Add questions, set required fields, enable GPS if needed.</div>
      <a class="btn" style="margin-top:12px" href="/ui/templates{key_q}">Create template</a>
    </div>

    <div class="card" style="margin-top:16px">
      <div class="h2">Step 4 — Share with enumerators</div>
      <div class="muted">Copy the link or QR code and deploy to the field.</div>
      <a class="btn" style="margin-top:12px" href="/ui/templates{key_q}">Open share panel</a>
    </div>

    <div class="card" style="margin-top:16px">
      <div class="h2">Step 5 — First submission moment</div>
      <div class="muted">Review QA signals, preview analytics, and export a sample.</div>
      <a class="btn" style="margin-top:12px" href="/ui/surveys{key_q}">View submissions</a>
    </div>
    """
    return ui_shell("Onboarding", page_html, show_project_switcher=False)


@app.route("/ui/profile", methods=["GET", "POST"])
def ui_profile():
    gate = admin_gate()
    if gate:
        return gate

    user = getattr(g, "user", None)
    if not user:
        return redirect(url_for("ui_login") + f"?next={request.path}")

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    org_id = current_org_id()
    org = prj.get_organization(int(org_id)) if org_id else None

    msg = ""
    err = ""
    if request.method == "POST":
        try:
            full_name = (request.form.get("full_name") or "").strip()
            title = (request.form.get("title") or "").strip()
            phone = (request.form.get("phone") or "").strip()
            org_name = (request.form.get("org_name") or "").strip()
            img_file = request.files.get("profile_image")

            if not full_name:
                raise ValueError("Full name is required.")

            new_image_name = None
            if img_file and (img_file.filename or "").strip():
                raw_name = secure_filename(img_file.filename)
                ext = os.path.splitext(raw_name)[1].lower()
                allowed_exts = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
                if ext not in allowed_exts:
                    raise ValueError("Profile photo must be PNG, JPG, WEBP, or GIF.")
                new_image_name = f"profile_{int(user.get('id'))}_{uuid.uuid4().hex}{ext}"
                img_path = os.path.join(UPLOAD_DIR, new_image_name)
                img_file.save(img_path)

            with get_conn() as conn:
                if new_image_name:
                    conn.execute(
                        "UPDATE users SET full_name=?, title=?, phone=?, profile_image_path=? WHERE id=?",
                        (full_name, title, phone or None, new_image_name, int(user.get("id"))),
                    )
                else:
                    conn.execute(
                        "UPDATE users SET full_name=?, title=?, phone=? WHERE id=?",
                        (full_name, title, phone or None, int(user.get("id"))),
                    )
                if org and org_name and int(user.get("organization_id") or 0) == int(org.get("id")):
                    conn.execute("UPDATE organizations SET name=? WHERE id=?", (org_name, int(org.get("id"))))
                conn.commit()

            session["user_name"] = full_name
            if new_image_name:
                session["user_image"] = new_image_name
            msg = "Profile updated."
            user = dict(user)
            user["full_name"] = full_name
            user["title"] = title
            user["phone"] = phone
            if new_image_name:
                user["profile_image_path"] = new_image_name
            if org and org_name:
                org["name"] = org_name
        except Exception as e:
            err = str(e)

    err_html = html.escape(err) if err else ""
    msg_html = html.escape(msg) if msg else ""
    org_name_val = (org.get("name") or "") if org else ""
    user_image = (user.get("profile_image_path") or "").strip()
    if not user_image:
        user_image = (session.get("user_image") or "").strip()
    user_image_url = f"/uploads/{html.escape(user_image)}" if user_image else ""
    initials_src = (user.get("full_name") or user.get("email") or "OF").strip()
    user_initials = "".join([p[0] for p in initials_src.split()[:2]]).upper() or "OF"
    html_page = f"""
    <div class="card" style="max-width:820px;margin:10px auto 0;">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div>
          <div class="h2">Profile</div>
          <div class="muted">Manage your account details and workspace identity.</div>
        </div>
        <div class="row" style="gap:8px;">
          <a class="btn btn-sm" href="/ui{key_q}">Home</a>
          <a class="btn btn-sm" href="/ui{key_q}">Back to dashboard</a>
        </div>
      </div>
    </div>

    <div class="card" style="max-width:820px;margin:16px auto 0;">
      {f"<div class='card' style='border-color: rgba(46, 204, 113, .35);margin-bottom:12px'>{msg_html}</div>" if msg else ""}
      {f"<div class='card' style='border-color: rgba(231, 76, 60, .35);margin-bottom:12px'><b>Error:</b> {err_html}</div>" if err else ""}
      <form method="POST" class="stack" enctype="multipart/form-data">
        <div class="row" style="gap:14px; align-items:center;">
          <div style="position:relative;width:80px;height:80px;border-radius:18px;overflow:hidden;border:1px solid var(--border);background:var(--surface-2);display:grid;place-items:center;font-weight:800;color:var(--muted);box-shadow:0 10px 24px rgba(124,58,237,.18);">
            {f"<img id='profileImagePreview' src='{user_image_url}' alt='Profile photo' style='width:100%;height:100%;object-fit:cover;' />" if user_image_url else f"<span id='profileImagePreview' style='font-weight:900;font-size:18px'>{html.escape(user_initials[:2])}</span>"}
            <button type="button" id="profileImagePick" aria-label="Upload profile photo" title="Upload profile photo" style="position:absolute;right:-4px;bottom:-4px;width:32px;height:32px;border-radius:12px;border:1px solid rgba(124,58,237,.35);background:var(--primary);color:#fff;font-weight:900;font-size:20px;line-height:1;cursor:pointer;box-shadow:0 10px 18px rgba(124,58,237,.35);">+</button>
          </div>
          <div style="flex:1; min-width:240px">
            <label style="font-weight:800">Profile photo</label>
            <input id="profileImageInput" type="file" name="profile_image" accept="image/png,image/jpeg,image/webp,image/gif" style="display:none;" />
            <div class="muted" style="font-size:12px;margin-top:6px;">Click the + icon to choose PNG, JPG, WEBP, or GIF.</div>
          </div>
        </div>
        <div class="row" style="gap:12px; flex-wrap:wrap;">
          <div style="flex:1; min-width:240px">
            <label style="font-weight:800">Full name</label>
            <input name="full_name" value="{html.escape(user.get('full_name') or '')}" required />
          </div>
          <div style="flex:1; min-width:240px">
            <label style="font-weight:800">Work email</label>
            <input value="{html.escape(user.get('email') or '')}" disabled />
          </div>
        </div>
        <div class="row" style="gap:12px; flex-wrap:wrap;">
          <div style="flex:1; min-width:240px">
            <label style="font-weight:800">Role / Title</label>
            <input name="title" value="{html.escape(user.get('title') or '')}" placeholder="e.g., Research Lead" />
          </div>
          <div style="flex:1; min-width:240px">
            <label style="font-weight:800">Phone</label>
            <input name="phone" value="{html.escape(user.get('phone') or '')}" placeholder="+234..." />
          </div>
        </div>
        <div>
          <label style="font-weight:800">Workspace name</label>
          <input name="org_name" value="{html.escape(org_name_val)}" placeholder="Organization or team name" />
          <div class="muted" style="font-size:12px;margin-top:6px;">This name appears in your dashboard welcome.</div>
        </div>
        <div class="row" style="justify-content:space-between; align-items:center;">
          <a class="btn" href="/logout">Log out</a>
          <button class="btn btn-primary" type="submit">Save changes</button>
        </div>
      </form>
    </div>
    <script>
      (function () {{
        const pick = document.getElementById("profileImagePick");
        const input = document.getElementById("profileImageInput");
        const preview = document.getElementById("profileImagePreview");
        if (!pick || !input || !preview) return;
        pick.addEventListener("click", () => input.click());
        input.addEventListener("change", () => {{
          const file = input.files && input.files[0];
          if (!file) return;
          const url = URL.createObjectURL(file);
          if (preview.tagName === "IMG") {{
            preview.src = url;
            return;
          }}
          const img = document.createElement("img");
          img.id = "profileImagePreview";
          img.alt = "Profile photo";
          img.style.width = "100%";
          img.style.height = "100%";
          img.style.objectFit = "cover";
          img.src = url;
          preview.replaceWith(img);
        }});
      }})();
    </script>
    """
    return ui_shell("Profile", html_page, show_project_switcher=False, nav_variant="profile_only")


@app.route("/settings/profile")
def settings_profile_alias():
    return redirect(url_for("ui_profile"))


@app.route("/settings/security")
def settings_security_alias():
    return redirect(url_for("ui_settings_security"))


@app.route("/settings/sessions")
def settings_sessions_alias():
    return redirect(url_for("ui_settings_sessions"))


@app.route("/ui/settings/profile")
def ui_settings_profile_alias():
    return redirect(url_for("ui_profile"))


@app.route("/ui/settings/security", methods=["GET", "POST"])
def ui_settings_security():
    gate = admin_gate()
    if gate:
        return gate

    user = getattr(g, "user", None)
    if not user:
        return redirect(url_for("ui_login") + f"?next={request.path}")

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    user_id = int(user.get("id"))
    settings = _ensure_user_security_settings(user_id)
    msg = ""
    err = ""

    # Refresh user row with new columns if available
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM users WHERE id=? LIMIT 1", (user_id,))
            row = cur.fetchone()
            if row:
                user = dict(row)
    except Exception:
        pass

    has_pw = bool((user.get("password_hash") or "").strip())
    has_google = bool((user.get("google_sub") or "").strip())

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()
        try:
            if action in ("change_password", "create_password"):
                current = (request.form.get("current_password") or "")
                new_pw = (request.form.get("new_password") or "")
                confirm = (request.form.get("confirm_password") or "")
                if not new_pw or not confirm:
                    raise ValueError("New password and confirmation are required.")
                if new_pw != confirm:
                    raise ValueError("Passwords do not match.")
                if not _password_is_valid(new_pw):
                    raise ValueError("Password must be at least 10 characters and include letters and numbers.")
                if action == "change_password":
                    if not has_pw:
                        raise ValueError("No password is set for this account yet.")
                    if not check_password_hash(user.get("password_hash") or "", current):
                        raise ValueError("Current password is incorrect.")
                with get_conn() as conn:
                    conn.execute(
                        "UPDATE users SET password_hash=?, updated_at=? WHERE id=?",
                        (generate_password_hash(new_pw), now_iso(), user_id),
                    )
                    conn.commit()
                has_pw = True
                next_provider = "both" if has_google else "local"
                try:
                    with get_conn() as conn:
                        conn.execute(
                            "UPDATE users SET auth_provider=?, updated_at=? WHERE id=?",
                            (next_provider, now_iso(), user_id),
                        )
                        conn.commit()
                except Exception:
                    pass
                _log_security_event(user_id, "PASSWORD_CHANGED")
                msg = "Password updated."
            elif action == "disconnect_google":
                if not has_google:
                    raise ValueError("Google is not connected.")
                if not has_pw:
                    raise ValueError("Set a password before disconnecting Google.")
                with get_conn() as conn:
                    conn.execute(
                        "UPDATE users SET google_sub=NULL, auth_provider='local', updated_at=? WHERE id=?",
                        (now_iso(), user_id),
                    )
                    conn.commit()
                has_google = False
                _log_security_event(user_id, "OAUTH_UNLINKED", {"provider": "google"})
                msg = "Google account disconnected."
            elif action == "save_toggles":
                notify_new_login = 1 if request.form.get("notify_new_login") == "on" else 0
                notify_password_change = 1 if request.form.get("notify_password_change") == "on" else 0
                notify_oauth_changes = 1 if request.form.get("notify_oauth_changes") == "on" else 0
                with get_conn() as conn:
                    conn.execute(
                        """
                        INSERT INTO user_security_settings (user_id, notify_new_login, notify_password_change, notify_oauth_changes, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(user_id) DO UPDATE SET
                          notify_new_login=excluded.notify_new_login,
                          notify_password_change=excluded.notify_password_change,
                          notify_oauth_changes=excluded.notify_oauth_changes,
                          updated_at=excluded.updated_at
                        """,
                        (user_id, notify_new_login, notify_password_change, notify_oauth_changes, now_iso()),
                    )
                    conn.commit()
                settings = {
                    "user_id": user_id,
                    "notify_new_login": notify_new_login,
                    "notify_password_change": notify_password_change,
                    "notify_oauth_changes": notify_oauth_changes,
                    "updated_at": now_iso(),
                }
                msg = "Security preferences saved."
            else:
                raise ValueError("Unknown action.")
        except Exception as e:
            err = str(e)

        # reload user after changes
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT * FROM users WHERE id=? LIMIT 1", (user_id,))
                row = cur.fetchone()
                if row:
                    user = dict(row)
        except Exception:
            pass
        has_pw = bool((user.get("password_hash") or "").strip())
        has_google = bool((user.get("google_sub") or "").strip())
        settings = _ensure_user_security_settings(user_id)

    err_html = html.escape(err) if err else ""
    msg_html = html.escape(msg) if msg else ""

    password_block = ""
    if has_pw:
        password_block = f"""
        <form method="POST" class="stack" style="margin-top:10px;">
          <input type="hidden" name="action" value="change_password" />
          <div class="row" style="gap:12px; flex-wrap:wrap;">
            <div style="flex:1; min-width:200px">
              <label style="font-weight:800">Current password</label>
              <input type="password" name="current_password" required />
            </div>
            <div style="flex:1; min-width:200px">
              <label style="font-weight:800">New password</label>
              <input type="password" name="new_password" required />
            </div>
            <div style="flex:1; min-width:200px">
              <label style="font-weight:800">Confirm</label>
              <input type="password" name="confirm_password" required />
            </div>
          </div>
          <div class="row" style="justify-content:flex-end;">
            <button class="btn btn-primary" type="submit">Update password</button>
          </div>
        </form>
        """
    else:
        password_block = f"""
        <div class="muted" style="margin-top:6px;">You currently sign in with Google. Create a password so you can sign in either way.</div>
        <form method="POST" class="stack" style="margin-top:10px;">
          <input type="hidden" name="action" value="create_password" />
          <div class="row" style="gap:12px; flex-wrap:wrap;">
            <div style="flex:1; min-width:220px">
              <label style="font-weight:800">New password</label>
              <input type="password" name="new_password" required />
            </div>
            <div style="flex:1; min-width:220px">
              <label style="font-weight:800">Confirm</label>
              <input type="password" name="confirm_password" required />
            </div>
          </div>
          <div class="row" style="justify-content:flex-end;">
            <button class="btn btn-primary" type="submit">Create password</button>
          </div>
        </form>
        """

    google_block = ""
    if has_google:
        google_block = f"""
        <div class="row" style="justify-content:space-between; align-items:center;">
          <div>
            <div style="font-weight:800;">Google connected</div>
            <div class="muted" style="font-size:12px;">Your Google account can sign in here.</div>
          </div>
          <form method="POST">
            <input type="hidden" name="action" value="disconnect_google" />
            <button class="btn" type="submit">Disconnect Google</button>
          </form>
        </div>
        {"" if has_pw else "<div class='muted' style='font-size:12px;margin-top:8px;color:#b91c1c;'>Set a password before disconnecting Google to avoid lockout.</div>"}
        """
    else:
        google_block = f"""
        <div class="row" style="justify-content:space-between; align-items:center;">
          <div>
            <div style="font-weight:800;">Google not connected</div>
            <div class="muted" style="font-size:12px;">Connect Google for faster sign-in.</div>
          </div>
          <a class="btn btn-primary" href="/auth/google?link=1">Connect Google</a>
        </div>
        """

    toggles_block = f"""
    <form method="POST" class="stack">
      <input type="hidden" name="action" value="save_toggles" />
      <label class="row" style="gap:8px;"><input type="checkbox" name="notify_new_login" {"checked" if int(settings.get("notify_new_login") or 0) == 1 else ""} /> Email me when a new device signs in</label>
      <label class="row" style="gap:8px;"><input type="checkbox" name="notify_password_change" {"checked" if int(settings.get("notify_password_change") or 0) == 1 else ""} /> Email me when my password changes</label>
      <label class="row" style="gap:8px;"><input type="checkbox" name="notify_oauth_changes" {"checked" if int(settings.get("notify_oauth_changes") or 0) == 1 else ""} /> Email me when OAuth connections change</label>
      <div class="row" style="justify-content:flex-end;">
        <button class="btn btn-primary" type="submit">Save preferences</button>
      </div>
    </form>
    """

    html_page = f"""
    <div class="card" style="max-width:900px;margin:10px auto 0;">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div>
          <div class="h2">Security</div>
          <div class="muted">Manage sign-in methods and account safety.</div>
        </div>
        <div class="row" style="gap:8px;">
          <a class="btn btn-sm" href="/ui{key_q}">Home</a>
          <a class="btn btn-sm" href="/ui/settings/sessions{key_q}">Sessions</a>
        </div>
      </div>
    </div>

    <div class="card" style="max-width:900px;margin:16px auto 0;">
      {f"<div class='card' style='border-color: rgba(46, 204, 113, .35);margin-bottom:12px'>{msg_html}</div>" if msg else ""}
      {f"<div class='card' style='border-color: rgba(231, 76, 60, .35);margin-bottom:12px'><b>Error:</b> {err_html}</div>" if err else ""}

      <div class="h3" style="margin:0 0 8px 0;">Password</div>
      {password_block}
    </div>

    <div class="card" style="max-width:900px;margin:12px auto 0;">
      <div class="h3" style="margin:0 0 8px 0;">Google account</div>
      {google_block}
    </div>

    <div class="card" style="max-width:900px;margin:12px auto 0;">
      <div class="h3" style="margin:0 0 8px 0;">Safety alerts</div>
      {toggles_block}
    </div>
    """
    return ui_shell("Security", html_page, show_project_switcher=False)


@app.route("/ui/settings/sessions", methods=["GET", "POST"])
def ui_settings_sessions():
    gate = admin_gate()
    if gate:
        return gate

    user = getattr(g, "user", None)
    if not user:
        return redirect(url_for("ui_login") + f"?next={request.path}")

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    user_id = int(user.get("id"))
    current_raw = (session.get("session_token") or "").strip()
    current_hash = _token_hash(current_raw) if current_raw else ""
    msg = ""
    err = ""

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()
        try:
            if action == "revoke_one":
                sid = int(request.form.get("session_id") or 0)
                with get_conn() as conn:
                    conn.execute(
                        "UPDATE sessions SET revoked_at=? WHERE id=? AND user_id=? AND revoked_at IS NULL",
                        (now_iso(), sid, user_id),
                    )
                    conn.commit()
                _log_security_event(user_id, "SESSION_REVOKED", {"session_id": sid})
                msg = "Session revoked."
            elif action == "revoke_all":
                _revoke_all_sessions(user_id, exclude_raw_token=current_raw)
                _log_security_event(user_id, "LOGOUT_ALL")
                msg = "All other sessions revoked."
            else:
                raise ValueError("Unknown action.")
        except Exception as e:
            err = str(e)

    sessions_rows: list[dict] = []
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, session_token_hash, device_label, ip_address, user_agent, created_at, last_seen_at, revoked_at
                FROM sessions
                WHERE user_id=?
                ORDER BY COALESCE(last_seen_at, created_at) DESC
                LIMIT 100
                """,
                (user_id,),
            )
            sessions_rows = [dict(r) for r in cur.fetchall()]
    except Exception:
        sessions_rows = []

    rows_html = []
    for s in sessions_rows:
        is_current = current_hash and s.get("session_token_hash") == current_hash and not s.get("revoked_at")
        status = "Current" if is_current else ("Revoked" if s.get("revoked_at") else "Active")
        revoke_btn = ""
        if not s.get("revoked_at") and not is_current:
            revoke_btn = f"""
            <form method="POST" style="margin:0;">
              <input type="hidden" name="action" value="revoke_one" />
              <input type="hidden" name="session_id" value="{int(s.get('id'))}" />
              <button class="btn btn-sm" type="submit">Revoke</button>
            </form>
            """
        rows_html.append(
            f"""
            <tr>
              <td style="font-weight:700;">{html.escape(s.get('device_label') or 'Device')}</td>
              <td>{html.escape(s.get('ip_address') or '—')}</td>
              <td>{html.escape(s.get('last_seen_at') or s.get('created_at') or '—')}</td>
              <td>{status}</td>
              <td>{revoke_btn}</td>
            </tr>
            """
        )

    err_html = html.escape(err) if err else ""
    msg_html = html.escape(msg) if msg else ""
    html_page = f"""
    <div class="card" style="max-width:960px;margin:10px auto 0;">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div>
          <div class="h2">Sessions & devices</div>
          <div class="muted">Review and revoke device sessions.</div>
        </div>
        <div class="row" style="gap:8px;">
          <a class="btn btn-sm" href="/ui{key_q}">Home</a>
          <a class="btn btn-sm" href="/ui/settings/security{key_q}">Security</a>
        </div>
      </div>
    </div>

    <div class="card" style="max-width:960px;margin:16px auto 0;">
      {f"<div class='card' style='border-color: rgba(46, 204, 113, .35);margin-bottom:12px'>{msg_html}</div>" if msg else ""}
      {f"<div class='card' style='border-color: rgba(231, 76, 60, .35);margin-bottom:12px'><b>Error:</b> {err_html}</div>" if err else ""}
      <div class="row" style="justify-content:space-between; align-items:center; margin-bottom:8px;">
        <div class="muted" style="font-size:12px;">Keep your account safe by revoking devices you no longer use.</div>
        <form method="POST" style="margin:0;">
          <input type="hidden" name="action" value="revoke_all" />
          <button class="btn" type="submit">Logout all other devices</button>
        </form>
      </div>
      <table class="table">
        <thead>
          <tr>
            <th>Device</th>
            <th>IP</th>
            <th>Last active</th>
            <th>Status</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {''.join(rows_html) if rows_html else "<tr><td colspan='5' class='muted' style='padding:18px'>No session history yet.</td></tr>"}
        </tbody>
      </table>
    </div>
    """
    return ui_shell("Sessions", html_page, show_project_switcher=False)


@app.route("/ui/organization", methods=["GET", "POST"])
def ui_organization():
    gate = admin_gate(allow_supervisor=False)
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    is_admin = bool(ADMIN_KEY and request.args.get("key") == ADMIN_KEY)
    org_id = current_org_id() if (REQUIRE_SUPERVISOR_KEY and not is_admin) else None
    if org_id is None:
        try:
            sess_org = session.get("org_id")
            org_id = int(sess_org) if sess_org is not None else None
        except Exception:
            org_id = None
    msg = ""
    err = ""
    orgs = prj.list_organizations(20)
    org = orgs[0] if orgs else None

    if request.method == "POST":
        try:
            name = (request.form.get("name") or "").strip()
            org_type = (request.form.get("org_type") or "").strip()
            country = (request.form.get("country") or "").strip()
            region = (request.form.get("region") or "").strip()
            sector = (request.form.get("sector") or "").strip()
            size = (request.form.get("size") or "").strip()
            website = (request.form.get("website") or "").strip()
            domain = (request.form.get("domain") or "").strip()
            address = (request.form.get("address") or "").strip()
            if org:
                with get_conn() as conn:
                    conn.execute(
                        """
                        UPDATE organizations
                        SET name=?, org_type=?, country=?, region=?, sector=?, size=?, website=?, domain=?, address=?, updated_at=?
                        WHERE id=?
                        """,
                        (
                            name,
                            org_type,
                            country,
                            region,
                            sector,
                            size,
                            website,
                            domain,
                            address,
                            now_iso(),
                            int(org.get("id")),
                        ),
                    )
                    conn.commit()
                msg = "Organization updated."
            else:
                prj.create_organization(
                    name,
                    sector=sector,
                    org_type=org_type,
                    country=country,
                    region=region,
                    size=size,
                    website=website,
                    domain=domain,
                    address=address,
                )
                msg = "Organization created."
            orgs = prj.list_organizations(20)
            org = orgs[0] if orgs else None
        except Exception as e:
            err = str(e)

    page_html = f"""
    <div class="card">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div>
          <div class="h1">Organization</div>
          <div class="muted">Institution identity and sector for onboarding.</div>
        </div>
        <a class="btn" href="/ui/onboarding{key_q}">Back to onboarding</a>
      </div>
    </div>

    {"<div class='card' style='border-color: rgba(46, 204, 113, .35)'><b>Success:</b> " + msg + "</div>" if msg else ""}
    {(
      "<div class='card' style='border-color: rgba(59, 130, 246, .35)'>"
      "<b>Redirecting:</b> Enumerator added. Taking you to Team in a moment… "
      f"<a href='/ui/org/users?project_id={project_id}{('&key=' + ADMIN_KEY) if ADMIN_KEY else ''}'>Go now</a>"
      "</div>"
      if msg and "Enumerator" in msg else ""
    )}
    {"<div class='card' style='border-color: rgba(231, 76, 60, .35)'><b>Error:</b> " + err + "</div>" if err else ""}

    <div class="card" style="margin-top:16px">
      <details>
        <summary style="cursor:pointer; font-weight:800; color:var(--primary);">What are coverage nodes?</summary>
        <div class="muted" style="margin-top:8px; line-height:1.6">
          Coverage nodes are a structured map of responsibility. Assign enumerators to LGAs or facilities, restrict what they see on the form,
          and power clean regional analytics. Build top-down: Country → State → LGA → Facility.
        </div>
      </details>
    </div>

    <div class="card" style="margin-top:16px">
      <form method="POST" class="stack">
        <div>
          <label style="font-weight:800">Organization name</label>
          <input name="name" placeholder="e.g., Ministry of Health" value="{org.get('name') if org else ''}" />
        </div>
        <div class="row" style="gap:12px; flex-wrap:wrap;">
          <div style="flex:1; min-width:200px">
            <label style="font-weight:800">Organization type</label>
            <select name="org_type">
              <option value="">Select</option>
              <option {'selected' if org and (org.get('org_type') or '') == 'Organization' else ''}>Organization</option>
              <option {'selected' if org and (org.get('org_type') or '') == 'Individual' else ''}>Individual</option>
              <option {'selected' if org and (org.get('org_type') or '') == 'Firm' else ''}>Firm</option>
              <option {'selected' if org and (org.get('org_type') or '') == 'Government' else ''}>Government</option>
            </select>
          </div>
          <div style="flex:1; min-width:200px">
            <label style="font-weight:800">Country</label>
            <input name="country" value="{org.get('country') if org else ''}" />
          </div>
          <div style="flex:1; min-width:200px">
            <label style="font-weight:800">Region</label>
            <input name="region" value="{org.get('region') if org else ''}" />
          </div>
        </div>
        <div class="row" style="gap:12px; flex-wrap:wrap;">
          <div style="flex:1; min-width:200px">
            <label style="font-weight:800">Sector</label>
            <input name="sector" placeholder="Health, Education" value="{org.get('sector') if org else ''}" />
          </div>
          <div style="flex:1; min-width:200px">
            <label style="font-weight:800">Size</label>
            <input name="size" placeholder="1-10, 11-50" value="{org.get('size') if org else ''}" />
          </div>
          <div style="flex:1; min-width:240px">
            <label style="font-weight:800">Website</label>
            <input name="website" placeholder="https://example.org" value="{org.get('website') if org else ''}" />
          </div>
        </div>
        <div class="row" style="gap:12px; flex-wrap:wrap;">
          <div style="flex:1; min-width:240px">
            <label style="font-weight:800">Email domain</label>
            <input name="domain" placeholder="example.org" value="{org.get('domain') if org else ''}" />
          </div>
          <div style="flex:2; min-width:260px">
            <label style="font-weight:800">Address</label>
            <input name="address" placeholder="Office address" value="{org.get('address') if org else ''}" />
          </div>
        </div>
        <div class="row">
          <button class="btn btn-primary" type="submit">{'Update organization' if org else 'Create organization'}</button>
          <a class="btn" href="/ui/onboarding{key_q}">Cancel</a>
        </div>
      </form>
    </div>
    """
    return ui_shell("Organization", page_html, show_project_switcher=False)


@app.route("/ui/org/users", methods=["GET", "POST"])
def ui_org_users():
    gate = admin_gate()
    if gate:
        return gate

    user = getattr(g, "user", None)
    if not user:
        return ui_shell("Users", "<div class='card'><h2>Sign in required</h2></div>", show_project_switcher=False), 403

    org_id = current_org_id()
    if not org_id:
        return ui_shell("Users", "<div class='card'><h2>No organization context</h2></div>", show_project_switcher=False), 400

    role = (user.get("role") or "").upper()
    can_manage_team = role in ("OWNER", "SUPERVISOR")
    smtp_ready = bool(SMTP_HOST and SMTP_FROM)
    msg = ""
    err = ""
    invite_link = ""
    enum_msg = ""
    enum_err = ""

    projects = prj.list_projects(200, organization_id=org_id)
    project_id_raw = request.args.get("project_id")
    project_id = project_id_raw if project_id_raw is not None else ""
    project_id = int(project_id) if str(project_id).isdigit() else None
    template_only_selected = project_id_raw == ""
    if request.method == "POST":
        form_pid = request.form.get("project_id") or ""
        if str(form_pid).isdigit():
            project_id = int(form_pid)
            template_only_selected = False
        else:
            project_id = None
            template_only_selected = True
    if project_id is None and projects and not template_only_selected:
        project_id = int(projects[0].get("id"))
    project_selected = project_id is not None
    if project_selected:
        templates = tpl.list_templates(200, project_id=project_id)
    else:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, name FROM survey_templates WHERE project_id IS NULL AND deleted_at IS NULL ORDER BY id DESC LIMIT 200"
            )
            templates = [dict(r) for r in cur.fetchall()]

    template_options = []
    for t in templates:
        if isinstance(t, dict):
            tid = t.get("id")
            tname = t.get("name")
        else:
            tid = t[0] if len(t) > 0 else None
            tname = t[1] if len(t) > 1 else ""
        if tid is None:
            continue
        template_options.append(f"<option value='{tid}'>{html.escape(tname or 'Template')}</option>")

    template_only_project_id = prj.get_template_only_project_id(org_id)

    if request.method == "POST":
        if not can_manage_team:
            return ("Forbidden: only owners or supervisors can manage users.", 403)
        try:
            action = (request.form.get("action") or "").strip()
            target_id = request.form.get("user_id") or ""
            target_id = int(target_id) if str(target_id).isdigit() else None
            if not target_id:
                if action not in ("invite", "invite_resend", "invite_revoke", "create_supervisor", "create_enumerator"):
                    raise ValueError("Missing user.")
            if action in ("approve", "deactivate", "role", "invite", "invite_resend", "invite_revoke", "create_supervisor", "toggle_supervisor") and not project_selected:
                raise ValueError("Select a project to manage supervisors and team roles.")
            if action == "approve":
                with get_conn() as conn:
                    conn.execute("UPDATE users SET status='ACTIVE' WHERE id=? AND organization_id=?", (target_id, int(org_id)))
                    conn.commit()
                msg = "User approved."
                _log_audit(org_id, int(user.get("id")), "user.approved", "user", target_id, {})
            elif action == "deactivate":
                if int(target_id or 0) == int(user.get("id") or 0):
                    raise ValueError("You cannot deactivate your own account while signed in.")
                with get_conn() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT role FROM users WHERE id=? AND organization_id=? LIMIT 1",
                        (int(target_id), int(org_id)),
                    )
                    target_row = cur.fetchone()
                    target_role = (target_row["role"] or "").upper() if target_row else ""
                    if target_role == "OWNER":
                        cur.execute(
                            """
                            SELECT COUNT(*) AS c
                            FROM users
                            WHERE organization_id=? AND role='OWNER' AND status='ACTIVE'
                            """,
                            (int(org_id),),
                        )
                        owner_count = int(cur.fetchone()["c"] or 0)
                        if owner_count <= 1:
                            raise ValueError("You cannot deactivate the last active owner.")
                    conn.execute(
                        "UPDATE users SET status='ARCHIVED' WHERE id=? AND organization_id=?",
                        (int(target_id), int(org_id)),
                    )
                    conn.commit()
                msg = "User deactivated."
                _log_audit(org_id, int(user.get("id")), "user.deactivated", "user", target_id, {})
            elif action == "role":
                if role != "OWNER":
                    raise ValueError("Only owners can change roles.")
                new_role = (request.form.get("role") or "").strip().upper()
                if new_role not in ("OWNER", "SUPERVISOR", "ANALYST"):
                    raise ValueError("Invalid role.")
                with get_conn() as conn:
                    conn.execute("UPDATE users SET role=? WHERE id=? AND organization_id=?", (new_role, target_id, int(org_id)))
                    conn.commit()
                msg = "Role updated."
                _log_audit(org_id, int(user.get("id")), "user.role.updated", "user", target_id, {"role": new_role})
            elif action == "invite":
                invite_email = (request.form.get("invite_email") or "").strip().lower()
                invite_role = (request.form.get("invite_role") or "SUPERVISOR").strip().upper()
                invite_note = (request.form.get("invite_note") or "").strip()
                if not invite_email:
                    raise ValueError("Invite email required.")
                if invite_role not in ("SUPERVISOR", "ANALYST"):
                    raise ValueError("Invalid invite role.")
                token = _create_invite(int(org_id), invite_email, invite_role, created_by=int(user.get("id")))
                invite_url = url_for("accept_invite", token=token, _external=True)
                org_name = ""
                try:
                    org = prj.get_organization(int(org_id))
                    if org:
                        org_name = org.get("name") or ""
                except Exception:
                    org_name = ""
                inviter_name = (user.get("full_name") or "").strip()
                subject, text_body, html_body = _email_template(
                    "invite",
                    link=invite_url,
                    org_name=org_name,
                    inviter=inviter_name,
                    note=invite_note,
                )
                sent = _send_email(invite_email, subject, text_body, html_body)
                if sent:
                    msg = f"Invite sent to {invite_email}."
                else:
                    reason = "SMTP not configured" if not smtp_ready else "Email delivery failed"
                    msg = f"{reason}. Share this invite link: {invite_url}"
                    invite_link = invite_url
                _log_audit(org_id, int(user.get("id")), "user.invite.created", "invite", None, {"email": invite_email, "role": invite_role})
            elif action == "invite_resend":
                invite_id = request.form.get("invite_id") or ""
                invite_id = int(invite_id) if str(invite_id).isdigit() else None
                if not invite_id:
                    raise ValueError("Missing invite.")
                with get_conn() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT * FROM user_invites WHERE id=? AND organization_id=? LIMIT 1",
                        (invite_id, int(org_id)),
                    )
                    inv = cur.fetchone()
                if not inv:
                    raise ValueError("Invite not found.")
                inv = dict(inv)
                if (inv.get("status") or "").upper() != "PENDING":
                    raise ValueError("Invite already used or revoked.")
                invite_url = url_for("accept_invite", token=inv.get("token"), _external=True)
                org_name = ""
                try:
                    org = prj.get_organization(int(org_id))
                    if org:
                        org_name = org.get("name") or ""
                except Exception:
                    org_name = ""
                inviter_name = (user.get("full_name") or "").strip()
                subject, text_body, html_body = _email_template(
                    "invite",
                    link=invite_url,
                    org_name=org_name,
                    inviter=inviter_name,
                )
                sent = _send_email(inv.get("email") or "", subject, text_body, html_body)
                if sent:
                    msg = "Invite resent."
                else:
                    msg = f"Email delivery failed. Share this invite link: {invite_url}"
                    invite_link = invite_url
                _log_audit(org_id, int(user.get("id")), "user.invite.resent", "invite", invite_id, {"email": inv.get("email")})
            elif action == "invite_revoke":
                invite_id = request.form.get("invite_id") or ""
                invite_id = int(invite_id) if str(invite_id).isdigit() else None
                if not invite_id:
                    raise ValueError("Missing invite.")
                with get_conn() as conn:
                    conn.execute(
                        "UPDATE user_invites SET status='REVOKED' WHERE id=? AND organization_id=?",
                        (invite_id, int(org_id)),
                    )
                    conn.commit()
                msg = "Invite revoked."
                _log_audit(org_id, int(user.get("id")), "user.invite.revoked", "invite", invite_id, {})
            elif action == "create_supervisor":
                full_name = (request.form.get("full_name") or "").strip()
                email = (request.form.get("email") or "").strip()
                phone = (request.form.get("phone") or "").strip()
                access_key = (request.form.get("access_key") or "").strip()
                if not full_name:
                    raise ValueError("Full name required.")
                if not access_key:
                    access_key = "sup_" + secrets.token_urlsafe(6).replace("-", "").replace("_", "").lower()
                prj.create_supervisor(
                    full_name=full_name,
                    organization_id=int(org_id),
                    email=email,
                    phone=phone,
                    access_key=access_key,
                    status="ACTIVE",
                )
                msg = "Supervisor created."
                _log_audit(org_id, int(user.get("id")), "supervisor.created", "supervisor", None, {"email": email})
            elif action == "create_enumerator":
                enum_project_id = request.form.get("project_id") or ""
                enum_project_id = int(enum_project_id) if str(enum_project_id).isdigit() else None
                full_name = (request.form.get("enum_name") or "").strip()
                email = (request.form.get("enum_email") or "").strip()
                phone = (request.form.get("enum_phone") or "").strip()
                coverage_label = (request.form.get("coverage_label") or "").strip()
                target_count = (request.form.get("target_facilities_count") or "").strip()
                target_count = int(target_count) if target_count.isdigit() else None
                template_id = (request.form.get("template_id") or "").strip()
                template_id = int(template_id) if template_id.isdigit() else None

                if not enum_project_id and not template_id:
                    raise ValueError("Select a template for template-only enumerator assignment.")
                if not full_name:
                    raise ValueError("Enumerator full name required.")
                if not enum_project_id:
                    coverage_label = ""

                internal_project_id = int(enum_project_id) if enum_project_id is not None else None
                if internal_project_id is None:
                    internal_project_id = prj.get_or_create_template_only_project(int(org_id))
                if internal_project_id is None:
                    raise ValueError("Template-only workspace project not available. Please reload and try again.")
                if not prj.get_project(int(internal_project_id)):
                    internal_project_id = prj.get_or_create_template_only_project(int(org_id))
                enumerator_id = enum.create_enumerator(
                    project_id=int(internal_project_id),
                    name=full_name,
                    phone=phone,
                    email=email,
                    status="ACTIVE",
                )
                assignment_id = enum.assign_enumerator(
                    project_id=int(internal_project_id),
                    enumerator_id=int(enumerator_id),
                    template_id=int(template_id) if template_id else None,
                    target_facilities_count=target_count,
                )
                if coverage_label:
                    with get_conn() as conn:
                        cols = [r["name"] for r in conn.execute("PRAGMA table_info(enumerator_assignments)").fetchall()]
                        if "coverage_label" in cols:
                            conn.execute(
                                "UPDATE enumerator_assignments SET coverage_label=? WHERE id=?",
                                (coverage_label, int(assignment_id)),
                            )
                            conn.commit()
                if enum_project_id:
                    code_info = prj.ensure_assignment_code(int(enum_project_id), int(enumerator_id), int(assignment_id))
                else:
                    code_info = prj.ensure_assignment_code_template(int(template_id), int(enumerator_id), int(assignment_id))
                enum_msg = f"Enumerator created. Code: {code_info.get('code_full')}"
                _log_audit(
                    org_id,
                    int(user.get("id")),
                    "enumerator.created",
                    "enumerator",
                    int(enumerator_id),
                    {"project_id": enum_project_id, "assignment_id": assignment_id},
                )
            elif action == "toggle_supervisor":
                sup_id = request.form.get("supervisor_id") or ""
                sup_id = int(sup_id) if str(sup_id).isdigit() else None
                next_status = (request.form.get("next_status") or "ACTIVE").strip().upper()
                if not sup_id:
                    raise ValueError("Missing supervisor.")
                with get_conn() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT id FROM supervisors WHERE id=? AND organization_id=? LIMIT 1",
                        (int(sup_id), int(org_id)),
                    )
                    if not cur.fetchone():
                        raise ValueError("Supervisor not found for this organization.")
                prj.update_supervisor(sup_id, status=next_status)
                msg = "Supervisor status updated."
                _log_audit(org_id, int(user.get("id")), "supervisor.status.updated", "supervisor", sup_id, {"status": next_status})
        except Exception as e:
            if (request.form.get("action") or "").strip() == "create_enumerator":
                enum_err = str(e)
            else:
                err = str(e)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE organization_id=? ORDER BY id DESC", (int(org_id),))
        users = [dict(r) for r in cur.fetchall()]

    supervisors = prj.list_supervisors(organization_id=int(org_id), limit=200)
    supervisor_rows = []
    for s in supervisors:
        status = (s.get("status") or "ACTIVE").upper()
        next_status = "ARCHIVED" if status == "ACTIVE" else "ACTIVE"
        sup_code = html.escape(s.get("access_key") or "")
        assign_btn = ""
        if project_selected and project_id:
            assign_btn = f"<a class='btn btn-sm' href='{url_for('ui_project_assignments', project_id=project_id)}?supervisor_id={s.get('id')}{'&key=' + ADMIN_KEY if ADMIN_KEY else ''}'>Assign</a>"
        supervisor_rows.append(
            f"""
            <tr>
              <td><span class="template-id">#{s.get('id')}</span></td>
              <td>
                <div class="template-name">{html.escape(s.get('full_name') or '')}</div>
                <div class="template-desc">{html.escape(s.get('email') or '—')}</div>
              </td>
              <td>
                <div class="row" style="gap:8px; align-items:center;">
                  <span class="muted" style="font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;">{sup_code or '—'}</span>
                  {f"<button class='btn btn-sm' type='button' data-copy='{sup_code}' title='Copy access key'>📋</button>" if sup_code else ""}
                </div>
              </td>
              <td>
                <span class="status-badge {'status-active' if status == 'ACTIVE' else 'status-archived'}">
                  {('🟢 Active' if status == 'ACTIVE' else '🔴 Archived')}
                </span>
              </td>
              <td class="muted">{s.get('created_at') or ''}</td>
              <td>
                {assign_btn}
                <form method="POST" style="display:inline">
                  <input type="hidden" name="action" value="toggle_supervisor" />
                  <input type="hidden" name="supervisor_id" value="{s.get('id')}" />
                  <input type="hidden" name="next_status" value="{next_status}" />
                  <button class="btn btn-sm" type="submit">{'Archive' if status == 'ACTIVE' else 'Activate'}</button>
                </form>
              </td>
            </tr>
            """
        )
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM user_invites WHERE organization_id=? ORDER BY id DESC", (int(org_id),))
        invites = [dict(r) for r in cur.fetchall()]

    enum_rows = []
    with get_conn() as conn:
        cur = conn.cursor()
        if project_selected:
            cur.execute(
                """
                SELECT
                  ea.id AS assignment_id,
                  ea.project_id,
                  ea.code_full,
                  ea.target_facilities_count,
                  ea.created_at,
                  ea.is_active,
                  e.id AS enumerator_id,
                  COALESCE(e.name, e.full_name, '') AS enumerator_name,
                  e.email AS enumerator_email,
                  e.phone AS enumerator_phone,
                  t.name AS template_name
                FROM enumerator_assignments ea
                JOIN enumerators e ON e.id = ea.enumerator_id
                LEFT JOIN survey_templates t ON t.id = ea.template_id
                WHERE ea.project_id=?
                ORDER BY ea.id DESC
                """,
                (int(project_id),),
            )
        else:
            cur.execute(
                """
                SELECT
                  ea.id AS assignment_id,
                  ea.project_id,
                  ea.code_full,
                  ea.target_facilities_count,
                  ea.created_at,
                  ea.is_active,
                  e.id AS enumerator_id,
                  COALESCE(e.name, e.full_name, '') AS enumerator_name,
                  e.email AS enumerator_email,
                  e.phone AS enumerator_phone,
                  t.name AS template_name
                FROM enumerator_assignments ea
                JOIN enumerators e ON e.id = ea.enumerator_id
                LEFT JOIN survey_templates t ON t.id = ea.template_id
                WHERE ea.project_id=? 
                ORDER BY ea.id DESC
                """
            ,
                (int(template_only_project_id) if template_only_project_id else 0,),
            )
        rows = cur.fetchall()
    for r in rows:
        code_val = html.escape(r["code_full"] or "")
        assign_btn = ""
        row_project_id = (r["project_id"] if "project_id" in r.keys() else None) or project_id
        if row_project_id:
            assign_btn = f"<a class='btn btn-sm' href='{url_for('ui_project_assignments', project_id=int(row_project_id))}?enumerator_id={r['enumerator_id']}{'&key=' + ADMIN_KEY if ADMIN_KEY else ''}'>Assign</a>"
        enum_rows.append(
            f"""
            <tr>
              <td><span class="template-id">#{r['enumerator_id']}</span></td>
              <td>
                <div class="template-name">{html.escape(r['enumerator_name'] or '')}</div>
                <div class="template-desc">{html.escape((r['enumerator_email'] or '') + (' · ' + r['enumerator_phone'] if r['enumerator_phone'] else ''))}</div>
              </td>
              <td class="muted">{html.escape(r['template_name'] or 'Any form')}</td>
              <td>
                <div class="row" style="gap:8px; align-items:center;">
                  <span class="muted" style="font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;">{code_val or '—'}</span>
                  {f"<button class='btn btn-sm' type='button' data-copy='{code_val}' title='Copy code'>📋</button>" if code_val else ""}
                </div>
              </td>
              <td class="muted">{r['target_facilities_count'] or '—'}</td>
              <td class="muted">{r['created_at'] or ''}</td>
              <td>{assign_btn or "<span class='muted'>—</span>"}</td>
            </tr>
            """
        )

    rows = []
    for u in users:
        u_status = (u.get("status") or "ACTIVE").upper()
        u_role = (u.get("role") or "SUPERVISOR").upper()
        verify_badge = "Verified" if int(u.get("email_verified") or 0) == 1 else "Unverified"
        actions = ""
        if can_manage_team:
            if u_status == "PENDING":
                actions += f"<form method='POST' style='display:inline'><input type='hidden' name='action' value='approve'/><input type='hidden' name='user_id' value='{u.get('id')}'/><button class='btn btn-sm' type='submit'>Approve</button></form>"
            if u_status == "ACTIVE":
                actions += f"<form method='POST' style='display:inline'><input type='hidden' name='action' value='deactivate'/><input type='hidden' name='user_id' value='{u.get('id')}'/><button class='btn btn-sm' type='submit'>Remove</button></form>"
            if role == "OWNER":
                actions += f"<form method='POST' style='display:inline'><input type='hidden' name='action' value='role'/><input type='hidden' name='user_id' value='{u.get('id')}'/><select name='role' onchange='this.form.submit()'><option value='OWNER' {'selected' if u_role=='OWNER' else ''}>Owner</option><option value='SUPERVISOR' {'selected' if u_role=='SUPERVISOR' else ''}>Supervisor</option><option value='ANALYST' {'selected' if u_role=='ANALYST' else ''}>Analyst</option></select></form>"
            if project_selected and project_id and u_role == "SUPERVISOR":
                actions += f"<a class='btn btn-sm' href='{url_for('ui_project_assignments', project_id=project_id)}?supervisor_id={u.get('id')}{'&key=' + ADMIN_KEY if ADMIN_KEY else ''}'>Assign</a>"

        rows.append(
            f"""
            <tr>
              <td><span class="template-id">#{u.get('id')}</span></td>
              <td>
                <div class="template-name">{html.escape(u.get('full_name') or '')}</div>
                <div class="template-desc">{html.escape(u.get('email') or '')}</div>
              </td>
              <td class="muted">{u_role}</td>
              <td class="muted">{verify_badge}</td>
              <td>
                <span class="status-badge {'status-active' if u_status == 'ACTIVE' else ('status-draft' if u_status == 'PENDING' else 'status-archived')}">
                  {('🟢 Active' if u_status == 'ACTIVE' else ('🟡 Pending' if u_status == 'PENDING' else '🔴 Archived'))}
                </span>
              </td>
              <td class="muted">{u.get('created_at') or ''}</td>
              <td>{actions or "<span class='muted'>—</span>"}</td>
            </tr>
            """
        )

    invite_rows = []
    for inv in invites:
        inv_status = (inv.get("status") or "PENDING").upper()
        invite_rows.append(
            f"""
            <tr>
              <td><span class="template-id">#{inv.get('id')}</span></td>
              <td>{html.escape(inv.get('email') or '')}</td>
              <td class="muted">{inv.get('role')}</td>
              <td class="muted">{inv_status}</td>
              <td class="muted">{inv.get('created_at') or ''}</td>
              <td>
                {f"<form method='POST' style='display:inline'><input type='hidden' name='action' value='invite_resend'/><input type='hidden' name='invite_id' value='{inv.get('id')}'/><button class='btn btn-sm' type='submit'>Resend</button></form>" if can_manage_team and inv_status == 'PENDING' else ""}
                {f"<form method='POST' style='display:inline'><input type='hidden' name='action' value='invite_revoke'/><input type='hidden' name='invite_id' value='{inv.get('id')}'/><button class='btn btn-sm' type='submit'>Revoke</button></form>" if role == 'OWNER' and inv_status == 'PENDING' else ""}
              </td>
            </tr>
            """
        )

    project_selector_options = [f"<option value='' {'selected' if template_only_selected and not project_selected else ''}>Template-only (no project)</option>"]
    for p in projects:
        pid = p.get("id")
        sel = "selected" if project_selected and int(project_id) == int(pid) else ""
        project_selector_options.append(f"<option value='{pid}' {sel}>{html.escape(p.get('name') or 'Project')}</option>")

    invite_block = f"""
    <div class="card" style="margin-top:16px">
      <h3 style="margin-top:0">Invite teammate</h3>
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div class="muted">Send an invite link to a supervisor or analyst.</div>
        <span class="env-badge {'env-live' if smtp_ready else 'env-dev'}">{'Email: Connected' if smtp_ready else 'Email: Not configured'}</span>
      </div>
      {("<div class='card' style='margin-top:10px;border-color: rgba(245, 158, 11, .35);'><b>Email not configured:</b> invites will generate a link you can share manually.</div>" if not smtp_ready else "")}
      {(
        f"<div class='card' style='margin-top:10px; border-color: rgba(59, 130, 246, .35); display:flex; align-items:center; justify-content:space-between; gap:10px;'><div><b>Invite link ready:</b><div class='muted' style='font-size:12px; margin-top:4px; word-break:break-all;'>{html.escape(invite_link)}</div></div><button class='btn btn-sm' type='button' data-copy='{html.escape(invite_link)}'>Copy link</button></div>"
        if invite_link else ""
      )}
      <form method="POST" class="row" style="margin-top:12px; flex-wrap:wrap;">
        <input type="hidden" name="action" value="invite" />
        <div style="flex:2; min-width:220px">
          <input name="invite_email" placeholder="email@organization.org" />
        </div>
        <div style="flex:1; min-width:160px">
          <select name="invite_role">
            <option value="SUPERVISOR">Supervisor</option>
            <option value="ANALYST">Analyst</option>
          </select>
        </div>
        <div>
          <button class="btn btn-primary" type="submit">Send invite</button>
        </div>
        <div style="flex-basis:100%; margin-top:10px;">
          <textarea name="invite_note" placeholder="Add a short note (optional)" style="min-height:72px"></textarea>
        </div>
      </form>
    </div>
    """ if project_selected else ""

    supervisors_block = f"""
    <div class="card" style="margin-top:16px">
      <h3 style="margin-top:0">Supervisors</h3>
      <div class="muted">Create supervisor access keys and manage status.</div>
      <form method="POST" class="stack" style="margin-top:12px">
        <input type="hidden" name="action" value="create_supervisor" />
        <div class="row" style="gap:12px; flex-wrap:wrap;">
          <div style="flex:2; min-width:200px">
            <label style="font-weight:800">Full name</label>
            <input name="full_name" placeholder="Supervisor name" />
          </div>
          <div style="flex:2; min-width:200px">
            <label style="font-weight:800">Email</label>
            <input name="email" placeholder="email@example.com" />
          </div>
          <div style="flex:1; min-width:160px">
            <label style="font-weight:800">Phone</label>
            <input name="phone" placeholder="Phone" />
          </div>
        </div>
        <div class="row" style="gap:12px; flex-wrap:wrap;">
          <div style="flex:1; min-width:220px">
            <label style="font-weight:800">Access key (auto-generated)</label>
            <input name="access_key" placeholder="Auto-generated" disabled />
          </div>
        </div>
        <button class="btn btn-primary" type="submit">Create supervisor</button>
      </form>
      <div style="margin-top:12px">
        <table class="table">
          <thead>
            <tr>
              <th style="width:90px">ID</th>
              <th>Supervisor</th>
              <th style="width:200px">Access key</th>
              <th style="width:140px">Status</th>
              <th style="width:160px">Created</th>
              <th style="width:140px">Actions</th>
            </tr>
          </thead>
          <tbody>
            {("".join(supervisor_rows) if supervisor_rows else "<tr><td colspan='6' class='muted' style='padding:18px'>No supervisors yet.</td></tr>")}
          </tbody>
        </table>
      </div>
    </div>
    """ if project_selected else """
    <div class="card" style="margin-top:16px">
      <h3 style="margin-top:0">Supervisors & Analysts</h3>
      <div class="muted">Select a project to manage supervisors and analysts for that project.</div>
    </div>
    """

    members_block = f"""
    <div class="card" style="margin-top:16px">
      <table class="table">
        <thead>
          <tr>
            <th style="width:90px">ID</th>
            <th>User</th>
            <th style="width:120px">Role</th>
            <th style="width:120px">Email</th>
            <th style="width:140px">Status</th>
            <th style="width:160px">Created</th>
            <th style="width:260px">Actions</th>
          </tr>
        </thead>
        <tbody>
          {("".join(rows) if rows else "<tr><td colspan='7' class='muted' style='padding:18px'>No users yet.</td></tr>")}
        </tbody>
      </table>
      <div class="muted" style="margin-top:10px">Analysts are read‑only. Owners can approve pending users.</div>
    </div>

    <div class="card" style="margin-top:16px">
      <h3 style="margin-top:0">Pending invites</h3>
      <table class="table">
        <thead>
          <tr>
            <th style="width:90px">ID</th>
            <th>Email</th>
            <th style="width:140px">Role</th>
            <th style="width:140px">Status</th>
            <th style="width:160px">Created</th>
            <th style="width:180px">Actions</th>
          </tr>
        </thead>
        <tbody>
          {("".join(invite_rows) if invite_rows else "<tr><td colspan='6' class='muted' style='padding:18px'>No invites yet.</td></tr>")}
        </tbody>
      </table>
    </div>
    """ if project_selected else ""

    html_page = f"""
    <div class="card">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div>
          <h1 class="h1">Team members</h1>
          <div class="muted">Manage roles and approve access requests.</div>
        </div>
        <a class="btn" href="/ui">Back to dashboard</a>
      </div>
    </div>

    {"<div class='card' style='border-color: rgba(46, 204, 113, .35)'><b>Success:</b> " + msg + "</div>" if msg else ""}
    {"<div class='card' style='border-color: rgba(231, 76, 60, .35)'><b>Error:</b> " + err + "</div>" if err else ""}

    {invite_block}

    {supervisors_block}

    <div class="card" style="margin-top:16px">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div>
          <h3 style="margin-top:0">Enumerators</h3>
          <div class="muted">Add enumerators and assign them to a form link.</div>
        </div>
        <form method="GET">
          <select name="project_id" onchange="this.form.submit()">
            {''.join(project_selector_options)}
          </select>
        </form>
      </div>
      {("<div class='card' style='margin-top:10px; border-color: rgba(46, 204, 113, .35)'><b>Success:</b> " + enum_msg + "</div>" if enum_msg else "")}
      {("<div class='card' style='margin-top:10px; border-color: rgba(231, 76, 60, .35)'><b>Error:</b> " + enum_err + "</div>" if enum_err else "")}
      <form method="POST" class="stack" style="margin-top:12px" id="enumForm">
        <input type="hidden" name="action" value="create_enumerator" />
        <div class="row" style="gap:12px; flex-wrap:wrap;">
          <div style="flex:2; min-width:220px">
            <label style="font-weight:800">Full name</label>
            <input name="enum_name" placeholder="Enumerator full name" />
          </div>
          <div style="flex:2; min-width:220px">
            <label style="font-weight:800">Email</label>
            <input name="enum_email" placeholder="email@example.com" />
          </div>
          <div style="flex:1; min-width:160px">
            <label style="font-weight:800">Phone</label>
            <input name="enum_phone" placeholder="Phone" />
          </div>
        </div>
        <div class="row" style="gap:12px; flex-wrap:wrap;">
          <div style="flex:2; min-width:220px">
            <label style="font-weight:800">Project (optional)</label>
            <select name="project_id" id="enumProjectSelect">
              {''.join(project_selector_options)}
            </select>
          </div>
          <div style="flex:2; min-width:220px">
            <label style="font-weight:800">Assign to template (optional)</label>
            <select name="template_id">
              <option value="">Any form</option>
              {''.join(template_options) if template_options else ""}
            </select>
          </div>
          <div style="flex:1; min-width:160px">
            <label style="font-weight:800">Target facilities (optional)</label>
            <div id="enumTargetWrap">
              <input name="target_facilities_count" placeholder="e.g., 8" />
            </div>
          </div>
        </div>
        <div class="row" style="gap:12px; flex-wrap:wrap;">
          <div style="flex:2; min-width:220px">
            <label style="font-weight:800">Coverage label (optional)</label>
            <div id="enumCoverageWrap">
              <input name="coverage_label" placeholder="e.g., Mushin LGA" {"disabled" if not project_selected else ""} />
              {("<div class='muted' style='margin-top:6px'>Coverage is only available for project assignments.</div>" if not project_selected else "")}
            </div>
          </div>
        </div>
        <button class="btn btn-primary" type="submit">Create enumerator</button>
      </form>
      <div style="margin-top:12px">
        {(
          "<table class='table'><thead><tr><th style=\"width:90px\">ID</th><th>Enumerator</th><th style=\"width:200px\">Template</th><th style=\"width:200px\">Assignment code</th><th style=\"width:140px\">Target</th><th style=\"width:160px\">Created</th><th style=\"width:140px\">Actions</th></tr></thead><tbody>" + "".join(enum_rows) + "</tbody></table>"
          if enum_rows else ""
        )}
      </div>
    </div>

    {members_block}
    <script>
      (function(){{
        const toast = document.getElementById("toast");
        if(toast && toast.textContent.trim()) {{
          toast.style.display = "block";
          setTimeout(()=>toast.style.opacity="0", 2400);
          setTimeout(()=>toast.style.display="none", 3200);
        }}
        const sel = document.getElementById("enumProjectSelect");
        const cov = document.getElementById("enumCoverageWrap");
        const tgt = document.getElementById("enumTargetWrap");
        function toggle() {{
          if(!sel) return;
          const hasProject = !!(sel.value && String(sel.value).trim());
          if(cov) cov.style.display = hasProject ? "" : "none";
          if(tgt) tgt.style.display = hasProject ? "" : "none";
        }}
        if(sel){{ sel.addEventListener("change", toggle); }}
        toggle();
        const buttons = Array.from(document.querySelectorAll("[data-copy]"));
        buttons.forEach(btn => {{
          btn.addEventListener("click", async () => {{
            const text = btn.getAttribute("data-copy") || "";
            if(!text) return;
            try{{
              await navigator.clipboard.writeText(text);
              const prev = btn.innerText;
              btn.innerText = "Copied";
              setTimeout(()=>btn.innerText=prev || "📋", 1200);
            }}catch(e){{
              const prev = btn.innerText;
              btn.innerText = "Copy failed";
              setTimeout(()=>btn.innerText=prev || "📋", 1200);
            }}
          }});
        }});
      }})();
    </script>
    """
    return ui_shell("Team", html_page, show_project_switcher=False)


@app.route("/ui/docs/<path:doc_path>")
def ui_docs(doc_path):
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    full_path = _safe_docs_path(doc_path)
    if not full_path:
        return ui_shell("Document not found", "<div class='card'><h2>Document not found</h2></div>"), 404

    if doc_path.lower().endswith((".csv", ".json")):
        return send_file(full_path, as_attachment=True, download_name=os.path.basename(full_path))

    with open(full_path, "r", encoding="utf-8") as f:
        content = f.read()

    title = os.path.basename(doc_path).replace("-", " ").replace(".md", "").title()
    rendered = _markdown_to_html(content)
    html_view = f"""
    <style>
      .doc-view h1{{font-size:30px;margin:0 0 12px 0}}
      .doc-view h2{{font-size:22px;margin:20px 0 10px 0}}
      .doc-view h3{{font-size:18px;margin:18px 0 8px 0}}
      .doc-view p{{margin:8px 0;color:var(--text)}}
      .doc-view ul{{padding-left:20px;margin:8px 0}}
      .doc-view li{{margin:6px 0}}
      .doc-view pre{{background:var(--surface-2);padding:12px;border-radius:12px;overflow:auto}}
    </style>
    <div class="card">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div>
          <h1 class="h1">{title}</h1>
          <div class="muted">Operator-facing documentation</div>
        </div>
        <a class="btn" href="/ui/adoption{key_q}">Back to Adoption</a>
      </div>
    </div>
    <div class="card doc-view" style="margin-top:16px">
      {rendered}
    </div>
    """
    return ui_shell(title, html_view, show_project_switcher=False)


@app.route("/ui/adoption", methods=["GET", "POST"])
def ui_adoption():
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    is_admin = bool(ADMIN_KEY and request.args.get("key") == ADMIN_KEY)
    org_id = current_org_id() if (REQUIRE_SUPERVISOR_KEY and not is_admin) else None
    if org_id is None:
        try:
            sess_org = session.get("org_id")
            org_id = int(sess_org) if sess_org is not None else None
        except Exception:
            org_id = None
    msg = ""
    err = ""
    is_admin = bool(ADMIN_KEY and request.args.get("key") == ADMIN_KEY)
    org_id = current_org_id() if (REQUIRE_SUPERVISOR_KEY and not is_admin) else None

    if request.method == "POST":
        try:
            action = (request.form.get("action") or "").strip()
            if action == "create_playbook":
                playbook_key = (request.form.get("playbook_key") or "").strip()
                _create_playbook(playbook_key)
                msg = f"{PLAYBOOKS[playbook_key]['label']} playbook is ready."
            elif action == "mark_live":
                project_id = request.form.get("project_id") or ""
                project_id = int(project_id) if str(project_id).isdigit() else None
                if not project_id:
                    raise ValueError("Select a project to mark as live.")
                project = prj.get_project(project_id)
                if not project:
                    raise ValueError("Project not found.")
                if project_is_locked(project):
                    raise ValueError("Archived projects cannot be marked as live.")
                prj.update_project(project_id, is_live_project=1, is_test_project=0, status="ACTIVE")
                msg = f"{project.get('name')} marked as live pilot."
        except Exception as e:
            err = str(e)

    playbook_cards = []
    for key, pb in PLAYBOOKS.items():
        project = _find_project_by_name(pb["project"]["name"])
        project_id = int(project.get("id")) if project else None
        templates_cfg = pb.get("templates") or []
        template_ids = []
        for tmpl in templates_cfg:
            template_ids.append(
                _find_template_by_name(tmpl["name"], project_id) if project_id else None
            )

        scheme_name = f"{pb['label']} Coverage"
        scheme = next((s for s in cov.list_schemes(500) if (s.get("name") or "").strip().lower() == scheme_name.lower()), None)
        coverage_status = "Ready" if scheme else "Not created"
        project_status = "Ready" if project else "Not created"
        ready_templates = [t for t in template_ids if t]
        if ready_templates and len(ready_templates) == len(template_ids):
            template_status = "Ready"
        elif ready_templates:
            template_status = "Partial"
        else:
            template_status = "Not created"
        coverage_chain = " → ".join(pb.get("coverage_levels", []))
        export_buttons = "".join(
            [
                f"<a class='btn' href='/ui/docs/{e['path']}{key_q}'>Download {e['label']}</a>"
                for e in pb.get("export_paths", [])
            ]
        )
        template_buttons = "".join(
            [
                f"<a class='btn' href='/ui/templates/{tid}/manage{key_q}'>Open {templates_cfg[idx]['name']}</a>"
                for idx, tid in enumerate(template_ids)
                if tid
            ]
        )

        playbook_cards.append(
            f"""
            <div class="card">
              <div class="row" style="justify-content:space-between; align-items:flex-start; gap:16px;">
                <div style="flex:1">
                  <h3 style="margin:0 0 8px 0">{pb['label']}</h3>
                  <div class="muted">{pb['project']['description']}</div>
                  <div class="row" style="margin-top:12px; gap:8px; flex-wrap:wrap">
                    <span class="status-pill">{project_status} project</span>
                    <span class="status-pill">{template_status} templates</span>
                    <span class="status-pill">{coverage_status} coverage</span>
                  </div>
                  <div class="muted" style="margin-top:8px">Coverage structure: {coverage_chain}</div>
                </div>
                <div class="stack" style="min-width:220px">
                  <form method="POST">
                    <input type="hidden" name="action" value="create_playbook" />
                    <input type="hidden" name="playbook_key" value="{key}" />
                    {"<input type='hidden' name='key' value='" + ADMIN_KEY + "' />" if ADMIN_KEY else ""}
                    <button class="btn btn-primary" type="submit">Create playbook</button>
                  </form>
                  <a class="btn" href="/ui/docs/{pb['docs_path']}{key_q}">View playbook</a>
                  {export_buttons}
                  {f"<a class='btn' href='/ui/projects/{project_id}{key_q}'>Open project</a>" if project_id else ""}
                  {template_buttons}
                </div>
              </div>
            </div>
            """
        )

    operator_links = "".join(
        [
            f"<li><a href='/ui/docs/{d['path']}{key_q}'>{d['title']}</a></li>"
            for d in OPERATOR_DOCS
        ]
    )
    pilot_links = "".join(
        [
            f"<li><a href='/ui/docs/{d['path']}{key_q}'>{d['title']}</a></li>"
            for d in PILOT_DOCS
        ]
    )
    positioning_links = "".join(
        [
            f"<li><a href='/ui/docs/{d['path']}{key_q}'>{d['title']}</a></li>"
            for d in POSITIONING_DOCS
        ]
    )

    is_admin = bool(ADMIN_KEY and request.args.get("key") == ADMIN_KEY)
    org_id = current_org_id() if (REQUIRE_SUPERVISOR_KEY and not is_admin) else None
    projects = prj.list_projects(200, organization_id=org_id)
    live_projects = [p for p in projects if int(p.get("is_live_project") or 0) == 1]
    project_options = "".join(
        [
            f"<option value='{p.get('id')}'>{p.get('name')}</option>"
            for p in projects
            if (p.get("status") or "").upper() != "ARCHIVED"
        ]
    )

    html_page = f"""
    <style>
      .doc-links a{{text-decoration:underline; color:var(--primary)}}
    </style>
    <div class="card">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div>
          <div class="h1">External Readiness & Adoption</div>
          <div class="muted">Move from “built” to “used” with playbooks, operator docs, and pilot readiness.</div>
        </div>
        <a class="btn" href="/ui{key_q}">Back to dashboard</a>
      </div>
    </div>

    {"<div class='card' style='border-color: rgba(46, 204, 113, .35)'><b>Success:</b> " + msg + "</div>" if msg else ""}
    {"<div class='card' style='border-color: rgba(231, 76, 60, .35)'><b>Error:</b> " + err + "</div>" if err else ""}

    <div class="card" style="margin-top:16px">
      <div class="h2">Stage 5 — Pilot Playbooks</div>
      <div class="muted">Create ready-to-run pilots with prebuilt projects, templates, coverage, and export examples.</div>
    </div>
    {"".join(playbook_cards)}

    <div class="card" style="margin-top:16px">
      <div class="h2">Stage 6 — Operator Docs</div>
      <div class="muted">Self-serve guides for enumerators, supervisors, and first-time operators.</div>
      <ul class="doc-links" style="margin-top:12px">{operator_links}</ul>
    </div>

    <div class="card" style="margin-top:16px">
      <div class="h2">Stage 7 — Deployment & First Live Pilot</div>
      <div class="muted">Choose one real partner, run a single project, capture feedback, and fix blockers only.</div>
      <ul class="doc-links" style="margin-top:12px">{pilot_links}</ul>
      <form method="POST" class="row" style="margin-top:14px; gap:12px; align-items:flex-end;">
        <input type="hidden" name="action" value="mark_live" />
        {"<input type='hidden' name='key' value='" + ADMIN_KEY + "' />" if ADMIN_KEY else ""}
        <div style="flex:1">
          <label style="font-weight:800">Mark a project as live pilot</label>
          <select name="project_id">
            <option value="">Select a project</option>
            {project_options}
          </select>
        </div>
        <button class="btn btn-primary" type="submit">Mark live</button>
      </form>
      <div class="muted" style="margin-top:10px">
        Live pilots: {", ".join([p.get("name") for p in live_projects]) if live_projects else "None yet"}
      </div>
    </div>

    <div class="card" style="margin-top:16px">
      <div class="h2">Stage 8 — Positioning & Leverage</div>
      <div class="muted">Use after a live pilot to unlock grants, partnerships, and a case study.</div>
      <ul class="doc-links" style="margin-top:12px">{positioning_links}</ul>
    </div>
    """
    return ui_shell("Adoption", html, show_project_switcher=False)


@app.route("/ui/projects", methods=["GET"])
def ui_projects():
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    def proj_suffix(pid): return (
        f"{key_q}&project_id={pid}" if key_q else f"?project_id={pid}")
    is_admin = bool(ADMIN_KEY and request.args.get("key") == ADMIN_KEY)
    org_id = current_org_id() if (REQUIRE_SUPERVISOR_KEY and not is_admin) else None
    if org_id is None:
        try:
            sess_org = session.get("org_id")
            org_id = int(sess_org) if sess_org is not None else None
        except Exception:
            org_id = None
    sup_id = current_supervisor_id()
    projects = prj.list_projects(200, organization_id=org_id)
    orgs = prj.list_organizations(200)
    org_map = {int(o.get("id")): o.get("name") for o in orgs}
    org_filter = request.args.get("org_id") or ""
    org_filter = int(org_filter) if str(org_filter).isdigit() else None
    if org_id is not None:
        org_filter = org_id
    project_options = ""
    allowed_project_ids = None
    if sup_id is not None:
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT DISTINCT project_id FROM enumerator_assignments WHERE supervisor_id=? AND project_id IS NOT NULL",
                    (int(sup_id),),
                )
                allowed_project_ids = {int(r["project_id"]) for r in cur.fetchall() if r["project_id"] is not None}
        except Exception:
            allowed_project_ids = None

    archived_projects = [p for p in projects if (p.get("status") or "ACTIVE").upper() == "ARCHIVED"]
    active_projects = [p for p in projects if (p.get("status") or "ACTIVE").upper() != "ARCHIVED"]

    visible_projects = []
    for p in active_projects:
        if allowed_project_ids is not None and int(p.get("id") or 0) not in allowed_project_ids:
            continue
        if org_filter and int(p.get("organization_id") or 0) != int(org_filter):
            continue
        visible_projects.append(p)
        pid = int(p.get("id"))
        status = (p.get("status") or "ACTIVE").upper()
        status_text = " [Archived]" if status == "ARCHIVED" else (" [Draft]" if status == "DRAFT" else "")
        project_options += f"<option value='{pid}'>{p.get('name')}{status_text}</option>"

    rows = []
    for row_index, p in enumerate(visible_projects, start=1):
        status = (p.get("status") or "ACTIVE").upper()
        badges = []
        if int(p.get("is_test_project") or 0) == 1:
            badges.append("<span class='pill-status pill-draft'>Test</span>")
        if int(p.get("is_live_project") or 0) == 1:
            badges.append("<span class='pill-status pill-active'>Live</span>")
        badges_html = "".join(badges)
        locked = (p.get("status") or "").upper() == "ARCHIVED"
        org_name = org_map.get(int(p.get("organization_id") or 0)) or "—"
        status_class = "pill-active" if status == "ACTIVE" else ("pill-draft" if status == "DRAFT" else "pill-archived")
        status_text = "Active" if status == "ACTIVE" else ("Draft" if status == "DRAFT" else "Archived")
        rows.append(
            f"""
            <tr>
              <td style="color:var(--muted)">#{row_index}</td>
              <td>
                <div class="proj-name">{html.escape(p.get('name') or 'Project')}</div>
                <div class="proj-desc">{html.escape(p.get('description') or 'No description provided.')}</div>
                <div style="margin-top:6px; display:flex; flex-wrap:wrap; gap:6px">{badges_html}</div>
              </td>
              <td style="color:var(--muted)">{html.escape(org_name)}</td>
              <td style="color:var(--muted)">{html.escape(p.get('owner_name') or '—')}</td>
              <td style="color:var(--muted)">{html.escape(p.get('created_at') or '')}</td>
              <td>
                <span class="pill-status {status_class}">
                  {status_text}
                </span>
              </td>
              <td>
                <div class="proj-actions-row {'opacity-60 pointer-events-none' if locked else ''}">
                  <a class="proj-btn-primary" style="padding:7px 10px; font-size:11px; border-radius:10px;" href="{url_for('ui_project_detail', project_id=p.get('id'))}{key_q}">Open</a>
                  <a class="proj-btn-ghost" style="padding:7px 10px; font-size:11px; border-radius:10px;" href="{url_for('ui_templates')}{proj_suffix(p.get('id'))}">Templates</a>
                  <a class="proj-btn-ghost" style="padding:7px 10px; font-size:11px; border-radius:10px; color:#be123c; border-color:#fecdd3;" href="{url_for('ui_project_delete', project_id=p.get('id'))}{key_q}" title="Archive project" aria-label="Archive project">Archive</a>
                </div>
              </td>
            </tr>
            """
        )

    active_count = sum(1 for p in visible_projects if (p.get("status") or "ACTIVE").upper() == "ACTIVE")
    draft_count = sum(1 for p in visible_projects if (p.get("status") or "ACTIVE").upper() == "DRAFT")

    org_filter_options = "<option value=''>All organizations</option>" + "".join(
        [
            f"<option value='{o.get('id')}' {'selected' if org_filter and int(o.get('id')) == int(org_filter) else ''}>{o.get('name')}</option>"
            for o in orgs
            if org_id is None or int(o.get("id")) == int(org_id)
        ]
    )
    org_filter_html = (
        f"""
          <div class="proj-filter-pill">
            <label for="orgFilter">Organization</label>
            <select id="orgFilter">
              {org_filter_options}
            </select>
          </div>
        """
        if org_id is None
        else ""
    )

    html_page = f"""
<style>
  .proj-page {{
    min-height: 100vh;
    padding: 28px 0 40px;
    background:
      radial-gradient(980px 460px at -5% -12%, rgba(124, 58, 237, .13), transparent 62%),
      radial-gradient(920px 430px at 110% 0%, rgba(99, 102, 241, .10), transparent 58%),
      linear-gradient(180deg, #f7f5ff 0%, #f3f4f9 100%);
  }}
  html[data-theme="dark"] .proj-page {{
    background:
      radial-gradient(980px 460px at -5% -12%, rgba(124, 58, 237, .24), transparent 62%),
      radial-gradient(920px 430px at 110% 0%, rgba(99, 102, 241, .18), transparent 58%),
      linear-gradient(180deg, #0f1221 0%, #121529 100%);
  }}
  .proj-shell {{
    max-width: 1180px;
    margin: 0 auto;
    padding: 0 18px;
    display: grid;
    gap: 16px;
  }}
  .proj-hero {{
    border: 1px solid rgba(124, 58, 237, .3);
    border-radius: 22px;
    background: linear-gradient(136deg, rgba(124,58,237,.15) 0%, rgba(255,255,255,.96) 46%, rgba(221,214,254,.58) 100%);
    box-shadow: 0 18px 40px rgba(15, 18, 34, .1);
    padding: 20px;
    position: relative;
    overflow: hidden;
  }}
  .proj-hero::after {{
    content: "";
    position: absolute;
    right: -110px;
    top: -130px;
    width: 320px;
    height: 320px;
    border-radius: 999px;
    background: radial-gradient(circle, rgba(167,139,250,.4), transparent 64%);
  }}
  html[data-theme="dark"] .proj-hero {{
    background: linear-gradient(136deg, rgba(124,58,237,.35) 0%, rgba(18,21,44,.95) 46%, rgba(44,49,80,.9) 100%);
    border-color: rgba(167, 139, 250, .33);
  }}
  .proj-hero-top {{
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 14px;
    position: relative;
    z-index: 2;
  }}
  .proj-kicker {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 5px 10px;
    border-radius: 999px;
    background: rgba(124,58,237,.14);
    color: var(--primary);
    font-size: 11px;
    font-weight: 800;
    letter-spacing: .07em;
    text-transform: uppercase;
  }}
  .proj-title {{
    margin: 10px 0 6px;
    font-size: 30px;
    font-weight: 900;
    letter-spacing: -.02em;
    color: #111827;
  }}
  html[data-theme="dark"] .proj-title {{ color: #f8fafc; }}
  .proj-sub {{
    max-width: 660px;
    color: #475569;
    font-size: 14px;
  }}
  html[data-theme="dark"] .proj-sub {{ color: #cbd5e1; }}
  .proj-actions {{
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    justify-content: flex-end;
  }}
  .proj-btn-primary {{
    border: 1px solid transparent;
    background: linear-gradient(90deg, var(--primary), #8b5cf6);
    color: #fff;
    font-weight: 700;
    font-size: 13px;
    border-radius: 12px;
    padding: 10px 14px;
    box-shadow: 0 10px 20px rgba(124,58,237,.25);
  }}
  .proj-btn-primary:hover {{ filter: brightness(1.02); }}
  .proj-btn-ghost {{
    border: 1px solid var(--border);
    background: var(--surface);
    color: var(--text);
    font-weight: 700;
    font-size: 13px;
    border-radius: 12px;
    padding: 10px 14px;
  }}
  .proj-metrics {{
    margin-top: 14px;
    position: relative;
    z-index: 2;
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 10px;
  }}
  .proj-metric {{
    border: 1px solid rgba(124,58,237,.22);
    border-radius: 12px;
    background: rgba(255,255,255,.72);
    padding: 10px 12px;
  }}
  html[data-theme="dark"] .proj-metric {{
    border-color: rgba(167,139,250,.32);
    background: rgba(17, 22, 43, .7);
  }}
  .proj-metric .label {{
    display: block;
    font-size: 11px;
    font-weight: 800;
    letter-spacing: .06em;
    text-transform: uppercase;
    color: #64748b;
  }}
  .proj-metric .value {{
    display: block;
    margin-top: 3px;
    color: #111827;
    font-size: 20px;
    font-weight: 900;
  }}
  html[data-theme="dark"] .proj-metric .value {{ color: #f8fafc; }}
  .proj-filters {{
    margin-top: 12px;
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
  }}
  .proj-filter-pill {{
    display: flex;
    align-items: center;
    gap: 10px;
    border: 1px solid var(--border);
    border-radius: 12px;
    background: var(--surface);
    padding: 9px 12px;
    min-width: 220px;
  }}
  .proj-filter-pill label {{
    margin: 0;
    font-size: 11px;
    font-weight: 800;
    letter-spacing: .05em;
    text-transform: uppercase;
    color: var(--muted);
    white-space: nowrap;
  }}
  .proj-filter-pill select {{
    border: none;
    background: transparent;
    width: 100%;
    color: var(--text);
    font-size: 13px;
    padding: 0;
  }}
  .proj-filter-pill select:focus {{
    outline: none;
  }}
  .proj-table-card {{
    border: 1px solid var(--border);
    border-radius: 22px;
    background: var(--surface);
    box-shadow: 0 14px 34px rgba(15, 18, 34, .09);
    overflow: hidden;
  }}
  .proj-table-head {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 10px;
    padding: 14px 16px;
    border-bottom: 1px solid var(--border);
    background: linear-gradient(180deg, rgba(124,58,237,.06), rgba(124,58,237,.02));
  }}
  .proj-table-title {{
    font-size: 15px;
    font-weight: 800;
    color: var(--text);
  }}
  .proj-table-sub {{
    font-size: 12px;
    color: var(--muted);
  }}
  .proj-table-scroll {{
    overflow-x: auto;
  }}
  .proj-table {{
    width: 100%;
    min-width: 820px;
    border-collapse: collapse;
    font-size: 13px;
  }}
  .proj-table th {{
    text-align: left;
    padding: 11px 12px;
    background: var(--surface-2);
    color: var(--muted);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: .06em;
    font-weight: 800;
  }}
  .proj-table td {{
    padding: 12px;
    border-top: 1px solid var(--border);
    vertical-align: top;
  }}
  .proj-table tr:hover {{
    background: rgba(124,58,237,.03);
  }}
  .proj-name {{
    font-weight: 700;
    color: var(--text);
  }}
  .proj-desc {{
    color: var(--muted);
    font-size: 12px;
    margin-top: 2px;
  }}
  .proj-actions-row {{
    display: flex;
    flex-wrap: wrap;
    justify-content: flex-end;
    gap: 6px;
  }}
  .pill-status {{
    display: inline-flex;
    align-items: center;
    border-radius: 999px;
    padding: 4px 9px;
    font-size: 11px;
    font-weight: 800;
  }}
  .pill-active {{
    background: rgba(16,185,129,.14);
    color: #047857;
  }}
  .pill-draft {{
    background: rgba(245,158,11,.14);
    color: #b45309;
  }}
  .pill-archived {{
    background: rgba(244,63,94,.14);
    color: #be123c;
  }}
  @media (max-width: 1024px) {{
    .proj-metrics {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
  }}
  @media (max-width: 760px) {{
    .proj-page {{ padding-top: 18px; }}
    .proj-shell {{ padding: 0 12px; }}
    .proj-hero-top {{ flex-direction: column; }}
    .proj-actions {{ justify-content: flex-start; }}
    .proj-metrics {{ grid-template-columns: 1fr; }}
    .proj-title {{ font-size: 26px; }}
  }}
</style>
<div class="proj-page">
  <div class="proj-shell">
    <section class="proj-hero">
      <div class="proj-hero-top">
        <div>
          <div class="proj-kicker">Project operations</div>
          <h1 class="proj-title">Projects</h1>
          <p class="proj-sub">Organize templates, submissions, QA, and exports by project with clean operational control.</p>
          <div class="proj-filters">
            <div class="proj-filter-pill">
              <label for="projectSwitcherInline">Jump to</label>
              <select id="projectSwitcherInline">
                <option value="">Select project…</option>
                {project_options}
              </select>
            </div>
            {org_filter_html}
          </div>
        </div>
        <div class="proj-actions">
          <a class="proj-btn-primary" href="{url_for('ui_projects_new')}{key_q}">+ New Project</a>
          <a class="proj-btn-ghost" href="{url_for('ui_projects_archived')}{key_q}">Archived ({len(archived_projects)})</a>
        </div>
      </div>
      <div class="proj-metrics">
        <div class="proj-metric"><span class="label">Visible projects</span><span class="value">{len(visible_projects)}</span></div>
        <div class="proj-metric"><span class="label">Active</span><span class="value">{active_count}</span></div>
        <div class="proj-metric"><span class="label">Draft</span><span class="value">{draft_count}</span></div>
        <div class="proj-metric"><span class="label">Archived</span><span class="value">{len(archived_projects)}</span></div>
      </div>
    </section>

    <section class="proj-table-card">
      <div class="proj-table-head">
        <div>
          <div class="proj-table-title">Project Directory</div>
          <div class="proj-table-sub">Open any project to manage templates, assignments, and submission workflows.</div>
        </div>
      </div>
      <div class="proj-table-scroll">
        <table class="proj-table">
          <thead>
            <tr>
              <th>ID</th>
              <th>Project</th>
              <th>Organization</th>
              <th>Owner</th>
              <th>Created</th>
              <th>Status</th>
              <th style="text-align:right">Actions</th>
            </tr>
          </thead>
          <tbody>
            {("".join(rows) if rows else "<tr><td colspan='7' style='text-align:center; padding:26px; color:var(--muted);'>No projects yet.</td></tr>")}
          </tbody>
        </table>
      </div>
    </section>
  </div>
</div>
<script>
  (function(){{
    const switcher = document.getElementById("projectSwitcherInline");
    if(switcher){{
      switcher.addEventListener("change", (e)=>{{
        const val = e.target.value;
        if(!val) return;
        window.location.href = "/ui/projects/" + val + "{key_q}";
      }});
    }}
    const orgFilter = document.getElementById("orgFilter");
    if(orgFilter){{
      orgFilter.addEventListener("change", (e)=>{{
        const val = e.target.value;
        const qs = val ? "?org_id=" + encodeURIComponent(val) : "";
        window.location.href = "/ui/projects" + qs + "{'&key=' + ADMIN_KEY if ADMIN_KEY else ''}";
      }});
    }}
  }})();
</script>
"""
    return ui_shell("Projects", html_page)


@app.route("/ui/projects/new", methods=["GET", "POST"])
def ui_projects_new():
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    is_admin = bool(ADMIN_KEY and request.args.get("key") == ADMIN_KEY)
    org_id = current_org_id() if (REQUIRE_SUPERVISOR_KEY and not is_admin) else None
    if org_id is None:
        try:
            sess_org = session.get("org_id")
            org_id = int(sess_org) if sess_org is not None else None
        except Exception:
            org_id = None
    msg = ""
    err = ""

    if request.method == "POST":
        try:
            name = (request.form.get("name") or "").strip()
            description = (request.form.get("description") or "").strip()
            owner_name = (request.form.get("owner_name") or "").strip()
            assignment_mode = (request.form.get("assignment_mode") or "OPTIONAL").strip().upper()
            status = (request.form.get("status") or "DRAFT").strip().upper()
            is_test_project = 1 if request.form.get("is_test_project") == "on" else 0
            is_live_project = 1 if request.form.get("is_live_project") == "on" else 0
            expected_submissions = request.form.get("expected_submissions") or ""
            expected_submissions = int(expected_submissions) if str(expected_submissions).isdigit() else None
            expected_coverage = request.form.get("expected_coverage") or ""
            expected_coverage = int(expected_coverage) if str(expected_coverage).isdigit() else None
            organization_id = request.form.get("organization_id") or ""
            organization_id = int(organization_id) if str(organization_id).isdigit() else None
            if org_id is not None:
                organization_id = int(org_id)
            pid = prj.create_project(
                name,
                description=description,
                owner_name=owner_name,
                assignment_mode=assignment_mode,
                is_test_project=is_test_project,
                is_live_project=is_live_project,
                status=status,
                expected_submissions=expected_submissions,
                expected_coverage=expected_coverage,
                organization_id=organization_id,
            )
            return redirect(url_for("ui_project_detail", project_id=pid) + key_q)
        except Exception as e:
            err = str(e)

    orgs = prj.list_organizations(200)
    org_options = "".join(
        [f"<option value='{o.get('id')}'>{o.get('name')}</option>" for o in orgs]
    )
    org_label = None
    if org_id is not None:
        org_label = next((o.get("name") for o in orgs if int(o.get("id") or 0) == int(org_id)), "Organization")

    html_page = f"""
    <div class="card">
      <h1 class="h1">Create Project</h1>
      <div class="muted">Set up a new project context for templates and surveys.</div>
    </div>

    {"<div class='card' style='border-color: rgba(46, 204, 113, .35)'><b>Success:</b> " + msg + "</div>" if msg else ""}
    {"<div class='card' style='border-color: rgba(231, 76, 60, .35)'><b>Error:</b> " + err + "</div>" if err else ""}

    <div class="card">
      <form method="POST" class="stack">
        <div>
          <label style="font-weight:800">Organization</label>
          {(
            f"<div class='muted' style='margin-top:6px'>Assigned to: <b>{html.escape(org_label or 'Organization')}</b></div><input type='hidden' name='organization_id' value='{org_id}' />"
            if org_id is not None
            else f"<select name='organization_id'><option value=''>Select organization</option>{org_options}</select><div class='muted' style='margin-top:6px'>Create one in Onboarding if missing.</div>"
          )}
        </div>
        <div>
          <label style="font-weight:800">Project name</label>
          <input name="name" placeholder="e.g., Kaduna Baseline" />
        </div>
        <div>
          <label style="font-weight:800">Description</label>
          <textarea name="description" placeholder="Short project context"></textarea>
        </div>
        <div>
          <label style="font-weight:800">Owner name</label>
          <input name="owner_name" placeholder="Team lead or supervisor name" />
        </div>
        <div>
          <label style="font-weight:800">Expected submissions</label>
          <input name="expected_submissions" type="number" min="0" placeholder="e.g., 120" />
        </div>
        <div>
          <label style="font-weight:800">Expected coverage</label>
          <input name="expected_coverage" type="number" min="0" placeholder="e.g., 80 facilities" />
        </div>
        <div>
          <label style="font-weight:800">Assignment strictness</label>
          <select name="assignment_mode">
            <option value="OPTIONAL">A — Assignment optional</option>
            <option value="REQUIRED_PROJECT">B — Assignment required per project</option>
            <option value="REQUIRED_TEMPLATE">C — Assignment required per template</option>
          </select>
        </div>
        <div>
          <label style="font-weight:800">Status</label>
          <select name="status">
            <option value="DRAFT" selected>Draft</option>
            <option value="ACTIVE">Active</option>
            <option value="ARCHIVED">Archived</option>
          </select>
        </div>
        <div>
          <label style="font-weight:800">Environment flags</label>
          <div class="row" style="margin-top:6px">
            <label class="row" style="gap:8px">
              <input type="checkbox" name="is_test_project" style="width:auto" />
              <span>Test project</span>
            </label>
            <label class="row" style="gap:8px">
              <input type="checkbox" name="is_live_project" style="width:auto" />
              <span>Live data collection</span>
            </label>
          </div>
        </div>
        <div class="row">
          <button class="btn btn-primary" type="submit">Create project</button>
          <a class="btn" href="{url_for('ui_projects')}{key_q}">Cancel</a>
        </div>
      </form>
    </div>
    """
    return ui_shell("Create Project", html_page)


@app.route("/ui/project/new", methods=["GET", "POST"])
def ui_project_new():
    return ui_projects_new()


@app.route("/ui/projects/<int:project_id>", methods=["GET"])
def ui_project_detail(project_id):
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    proj_suffix = f"{key_q}&project_id={project_id}" if key_q else f"?project_id={project_id}"
    project = prj.get_project(int(project_id))
    if not project:
        return ui_shell("Project not found", "<div class='card'><h2>Project not found</h2></div>"), 404
    org_name = None
    if project.get("organization_id"):
        org = prj.get_organization(int(project.get("organization_id")))
        org_name = org.get("name") if org else None
    project_options = ""
    try:
        is_admin = bool(ADMIN_KEY and request.args.get("key") == ADMIN_KEY)
        org_id = current_org_id() if (REQUIRE_SUPERVISOR_KEY and not is_admin) else None
        projects = prj.list_projects(200, organization_id=org_id)
        for p in projects:
            pid = int(p.get("id"))
            status = (p.get("status") or "ACTIVE").upper()
            status_text = " [Archived]" if status == "ARCHIVED" else (" [Draft]" if status == "DRAFT" else "")
            selected = "selected" if int(project_id) == pid else ""
            project_options += f"<option value='{pid}' {selected}>{p.get('name')}{status_text}</option>"
    except Exception:
        project_options = ""

    assignment_mode = (project.get("assignment_mode") or "OPTIONAL").strip().upper()
    assignment_label = {
        "OPTIONAL": "A — Assignment optional",
        "REQUIRED_PROJECT": "B — Assignment required per project",
        "REQUIRED_TEMPLATE": "C — Assignment required per template",
    }.get(assignment_mode, "A — Assignment optional")
    flag_bits = []
    if project_is_locked(project):
        flag_bits.append("<span class='flag-pill archived'>Archived</span>")
    if int(project.get("is_test_project") or 0) == 1:
        flag_bits.append("<span class='flag-pill test'>Test</span>")
    if int(project.get("is_live_project") or 0) == 1:
        flag_bits.append("<span class='flag-pill live'>Live</span>")
    flag_html = "".join(flag_bits)

    metrics = prj.project_metrics(int(project_id))
    overview = prj.project_overview(int(project_id))
    enum_perf = prj.enumerator_performance(int(project_id), days=7)
    timeline = prj.submissions_timeline(int(project_id), days=14)

    def _sparkline(values, width=260, height=60, stroke="#111827", stroke2="#8E5CFF"):
        if not values:
            return ""
        min_v = min(values)
        max_v = max(values)
        span = max_v - min_v if max_v != min_v else 1
        step = (width - 8) / max(1, len(values) - 1)
        points = []
        for i, v in enumerate(values):
            x = 4 + i * step
            y = 4 + (height - 8) * (1 - ((v - min_v) / span))
            points.append(f"{x:.2f},{y:.2f}")
        return f"""<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}">
          <polyline fill="none" stroke="{stroke}" stroke-width="2" points="{' '.join(points)}" />
        </svg>"""

    tl_sorted = list(reversed(timeline))
    total_vals = [int(t.get("total") or 0) for t in tl_sorted]
    completed_vals = [int(t.get("completed") or 0) for t in tl_sorted]
    spark_total = _sparkline(total_vals, stroke="#111827") if total_vals else ""
    spark_completed = _sparkline(completed_vals, stroke="#8E5CFF") if completed_vals else ""

    enum_rows = []
    max_enum_total = max([int(e.get("total_submissions") or 0) for e in enum_perf], default=0)
    max_enum_recent = max([int(e.get("completed_recent") or 0) for e in enum_perf], default=0)
    max_enum_today = max([int(e.get("completed_today") or 0) for e in enum_perf], default=0)
    for e in enum_perf:
        total_val = int(e.get("total_submissions") or 0)
        recent_val = int(e.get("completed_recent") or 0)
        today_val = int(e.get("completed_today") or 0)
        bar_pct = int((total_val / max_enum_total) * 100) if max_enum_total else 0
        recent_pct = int((recent_val / max_enum_recent) * 100) if max_enum_recent else 0
        today_pct = int((today_val / max_enum_today) * 100) if max_enum_today else 0
        enum_rows.append(
            f"""
            <tr>
              <td>{e.get('enumerator_name') or '—'}</td>
              <td>
                <div style="display:flex; align-items:center; gap:8px;">
                  <span>{total_val}</span>
                  <div style="flex:1; min-width:80px; height:6px; background:#eef2f6; border-radius:999px; overflow:hidden;">
                    <div style="height:6px; width:{bar_pct}%; background:#111827;"></div>
                  </div>
                </div>
              </td>
              <td>
                <div style="display:flex; align-items:center; gap:8px;">
                  <span>{today_val}</span>
                  <div style="flex:1; min-width:70px; height:6px; background:#eef2f6; border-radius:999px; overflow:hidden;">
                    <div style="height:6px; width:{today_pct}%; background:#10b981;"></div>
                  </div>
                </div>
              </td>
              <td>
                <div style="display:flex; align-items:center; gap:8px;">
                  <span>{recent_val}</span>
                  <div style="flex:1; min-width:70px; height:6px; background:#eef2f6; border-radius:999px; overflow:hidden;">
                    <div style="height:6px; width:{recent_pct}%; background:#8E5CFF;"></div>
                  </div>
                </div>
              </td>
              <td>{int(e.get('drafts_total') or 0)}</td>
              <td>{int(e.get('qa_flags') or 0)}</td>
            </tr>
            """
        )

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

    html_page = f"""
    <style>
      .proj-page {{
        min-height: 100vh;
        background:
          radial-gradient(1200px 500px at -10% -20%, rgba(124,58,237,.14), transparent 65%),
          radial-gradient(900px 420px at 110% -10%, rgba(139,92,246,.12), transparent 58%),
          linear-gradient(180deg, #f8f6ff 0%, #f3f4f8 100%);
        padding: 20px 0 34px;
      }}
      html[data-theme="dark"] .proj-page {{
        background:
          radial-gradient(1200px 500px at -10% -20%, rgba(124,58,237,.26), transparent 65%),
          radial-gradient(900px 420px at 110% -10%, rgba(139,92,246,.2), transparent 58%),
          linear-gradient(180deg, #0f1221 0%, #11162a 100%);
      }}
      .proj-shell {{
        max-width: 1200px;
        margin: 0 auto;
        padding: 0 16px;
        display: grid;
        gap: 14px;
      }}
      .proj-hero {{
        border: 1px solid rgba(124,58,237,.28);
        border-radius: 22px;
        padding: 18px;
        background: linear-gradient(125deg, rgba(124,58,237,.18) 0%, rgba(255,255,255,.97) 46%, rgba(224,231,255,.62) 100%);
        box-shadow: 0 16px 36px rgba(15,18,34,.1);
      }}
      html[data-theme="dark"] .proj-hero {{
        background: linear-gradient(125deg, rgba(124,58,237,.38) 0%, rgba(21,24,44,.95) 46%, rgba(35,40,71,.9) 100%);
        border-color: rgba(167,139,250,.35);
      }}
      .proj-hero-head {{
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 14px;
      }}
      .proj-kicker {{
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 5px 10px;
        border-radius: 999px;
        background: rgba(124,58,237,.14);
        color: var(--primary);
        font-size: 11px;
        font-weight: 800;
        letter-spacing: .06em;
        text-transform: uppercase;
      }}
      .proj-title {{
        margin: 10px 0 4px;
        font-size: 30px;
        font-weight: 900;
        letter-spacing: -.02em;
        color: #111827;
      }}
      html[data-theme="dark"] .proj-title {{
        color: #f8fafc;
      }}
      .proj-desc {{
        font-size: 14px;
        color: #475569;
        max-width: 760px;
      }}
      html[data-theme="dark"] .proj-desc {{
        color: #cbd5e1;
      }}
      .proj-hero-actions {{
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
      }}
      .proj-btn-primary {{
        border: 1px solid transparent;
        background: linear-gradient(90deg, var(--primary), #8b5cf6);
        color: #fff;
        box-shadow: 0 10px 22px rgba(124,58,237,.26);
      }}
      .proj-inline-switcher {{
        display: inline-flex;
        align-items: center;
        gap: 8px;
        margin-top: 12px;
        padding: 6px 10px;
        border-radius: 12px;
        border: 1px solid rgba(124,58,237,.2);
        background: rgba(255,255,255,.7);
      }}
      html[data-theme="dark"] .proj-inline-switcher {{
        background: rgba(30,41,59,.55);
      }}
      .proj-inline-switcher label {{
        font-size: 11px;
        color: var(--muted);
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: .05em;
      }}
      .proj-inline-switcher select {{
        border: none;
        padding: 6px 8px;
        font-size: 12px;
        background: transparent;
        color: var(--text);
      }}
      .proj-inline-switcher select:focus {{
        outline: none;
      }}
      .proj-kpi-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
        gap: 12px;
      }}
      .proj-kpi-card {{
        border: 1px solid var(--border);
        background: var(--surface);
        border-radius: 16px;
        padding: 14px;
        box-shadow: 0 10px 24px rgba(15,18,34,.06);
      }}
      .proj-kpi-card .label {{
        font-size: 12px;
        color: var(--muted);
      }}
      .proj-kpi-card .value {{
        font-size: 24px;
        font-weight: 900;
        margin-top: 2px;
      }}
      .proj-section-card {{
        border: 1px solid var(--border);
        background: var(--surface);
        border-radius: 18px;
        padding: 16px;
        box-shadow: 0 12px 28px rgba(15,18,34,.06);
      }}
      .proj-section-head {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 10px;
        margin-bottom: 10px;
      }}
      .proj-section-title {{
        margin: 0;
        font-size: 18px;
        font-weight: 800;
      }}
      .proj-legend {{
        display: flex;
        gap: 12px;
        flex-wrap: wrap;
        margin-bottom: 10px;
      }}
      .proj-legend-item {{
        font-size: 12px;
        color: var(--muted);
        display: inline-flex;
        align-items: center;
        gap: 6px;
      }}
      .proj-legend-dot {{
        width: 10px;
        height: 10px;
        border-radius: 999px;
        display: inline-block;
      }}
      .proj-links-card {{
        border: 1px solid var(--border);
        background: var(--surface);
        border-radius: 18px;
        padding: 16px;
        box-shadow: 0 12px 28px rgba(15,18,34,.06);
      }}
      .proj-link-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
        gap: 10px;
      }}
      .proj-link-btn {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 10px 12px;
        border-radius: 12px;
        border: 1px solid var(--border);
        background: var(--surface-2);
        text-decoration: none;
        color: var(--text);
        font-size: 13px;
        font-weight: 700;
      }}
      .proj-link-btn:hover {{
        border-color: rgba(124,58,237,.34);
        box-shadow: 0 8px 16px rgba(124,58,237,.14);
      }}
      .flag-pill {{
        display: inline-flex;
        align-items: center;
        padding: 4px 8px;
        border-radius: 999px;
        font-size: 11px;
        font-weight: 700;
        border: 1px solid var(--border);
        background: var(--surface-2);
        margin-right: 6px;
      }}
      .flag-pill.archived {{
        color: #991b1b;
        border-color: rgba(239,68,68,.35);
        background: rgba(254,226,226,.7);
      }}
      .flag-pill.test {{
        color: #b45309;
        border-color: rgba(245,158,11,.35);
        background: rgba(245,158,11,.12);
      }}
      .flag-pill.live {{
        color: #047857;
        border-color: rgba(16,185,129,.35);
        background: rgba(16,185,129,.12);
      }}
      .proj-alert-archived {{
        border: 1px solid rgba(239,68,68,.35);
        background: rgba(254,242,242,.88);
        color: #991b1b;
        border-radius: 14px;
        padding: 12px 14px;
        font-size: 13px;
      }}
      @media (max-width: 860px) {{
        .proj-hero-head {{
          flex-direction: column;
        }}
        .proj-title {{
          font-size: 26px;
        }}
      }}
    </style>

    <div class="proj-page">
      <div class="proj-shell">
        <section class="proj-hero">
          <div class="proj-hero-head">
            <div>
              <div class="proj-kicker">Project workspace</div>
              <h1 class="proj-title">{project.get('name')}</h1>
              <div class="proj-desc">{project.get('description') or 'No description provided.'}</div>
              <div class="muted" style="margin-top:6px">Organization: {org_name or '—'}</div>
              <div style="margin-top:8px">{flag_html}</div>
              <div class="proj-inline-switcher">
                <label>Project</label>
                <select id="projectSwitcherInline">
                  <option value="">All</option>
                  {project_options}
                </select>
              </div>
            </div>
            <div class="proj-hero-actions">
              <a class="btn btn-sm" href="{url_for('ui_projects')}{key_q}">Back to projects</a>
              <a class="btn btn-sm proj-btn-primary" href="{url_for('ui_project_settings', project_id=project_id)}{key_q}">Settings</a>
            </div>
          </div>
        </section>

        {"<div class='proj-alert-archived'><b>Archived:</b> This project is read-only. Actions are disabled.</div>" if project_is_locked(project) else ""}

        <section class="proj-kpi-grid">
          <div class="proj-kpi-card">
            <div class="label">Total submissions</div>
            <div class="value">{overview.get('total_submissions')}</div>
            <div class="muted" style="margin-top:6px">{overview.get('completed_submissions')} completed</div>
          </div>
          <div class="proj-kpi-card">
            <div class="label">Drafts pending</div>
            <div class="value">{overview.get('draft_submissions')}</div>
          </div>
          <div class="proj-kpi-card">
            <div class="label">Active enumerators</div>
            <div class="value">{overview.get('active_enumerators')}</div>
          </div>
          <div class="proj-kpi-card">
            <div class="label">Coverage target</div>
            <div class="value">{project.get('expected_coverage') if project.get('expected_coverage') is not None else '—'}</div>
          </div>
          <div class="proj-kpi-card">
            <div class="label">QA alerts</div>
            <div class="value">{metrics.get('qa_alerts_count')}</div>
          </div>
        </section>

        <section class="proj-section-card">
          <div class="proj-section-head">
            <div>
              <h3 class="proj-section-title">Assignment policy</h3>
              <div class="muted">Controls whether enumerators must use assignment links.</div>
            </div>
            <div style="font-weight:800">{assignment_label}</div>
          </div>
        </section>

        <section class="proj-section-card">
          <div class="proj-section-head">
            <h3 class="proj-section-title">Enumerator performance</h3>
          </div>
          <div class="muted" style="margin-bottom:10px">Completed today, last 7 days, and QA flags.</div>
          <div class="proj-legend">
            <div class="proj-legend-item"><span class="proj-legend-dot" style="background:#111827"></span> Total submissions</div>
            <div class="proj-legend-item"><span class="proj-legend-dot" style="background:#10b981"></span> Completed today</div>
            <div class="proj-legend-item"><span class="proj-legend-dot" style="background:#8E5CFF"></span> Completed (7d)</div>
          </div>
          <table class="table">
            <thead>
              <tr>
                <th>Enumerator</th>
                <th style="width:120px">Total</th>
                <th style="width:140px">Completed today</th>
                <th style="width:140px">Completed (7d)</th>
                <th style="width:120px">Drafts</th>
                <th style="width:120px">QA flags</th>
              </tr>
            </thead>
            <tbody>
              {("".join(enum_rows) if enum_rows else "<tr><td colspan='6' class='muted' style='padding:18px'>No enumerator activity yet.</td></tr>")}
            </tbody>
          </table>
        </section>

        <section class="proj-section-card">
          <div class="proj-section-head">
            <h3 class="proj-section-title">Submission timeline (last 14 days)</h3>
          </div>
          <div class="row" style="gap:18px; align-items:center; flex-wrap:wrap; margin-bottom:10px;">
            <div>
              <div class="muted" style="font-size:12px">Total submissions</div>
              {spark_total or "<div class='muted'>No data yet</div>"}
            </div>
            <div>
              <div class="muted" style="font-size:12px">Completed submissions</div>
              {spark_completed or "<div class='muted'>No data yet</div>"}
            </div>
          </div>
          <table class="table">
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
        </section>

        <section class="proj-links-card">
          <div class="proj-section-head">
            <h3 class="proj-section-title">Project tools</h3>
          </div>
          <div class="proj-link-grid">
            <a class="proj-link-btn" href="{url_for('ui_templates')}{proj_suffix}">Templates <span>→</span></a>
            <a class="proj-link-btn" href="{url_for('ui_surveys')}{proj_suffix}">Submissions <span>→</span></a>
            <a class="proj-link-btn" href="{url_for('ui_qa')}{proj_suffix}">QA Alerts <span>→</span></a>
            <a class="proj-link-btn" href="{url_for('ui_exports')}{proj_suffix}">Exports <span>→</span></a>
            <a class="proj-link-btn" href="{url_for('ui_project_coverage', project_id=project_id)}{key_q}">Coverage <span>→</span></a>
            <a class="proj-link-btn" href="{url_for('ui_project_enumerators', project_id=project_id)}{key_q}">Enumerators <span>→</span></a>
            <a class="proj-link-btn" href="{url_for('ui_project_assignments', project_id=project_id)}{key_q}">Assignments <span>→</span></a>
            <a class="proj-link-btn" href="{url_for('ui_project_interviews', project_id=project_id)}{key_q}">Interviews <span>→</span></a>
            <a class="proj-link-btn" href="{url_for('ui_project_analytics', project_id=project_id)}{key_q}">Analytics <span>→</span></a>
          </div>
        </section>
      </div>
    </div>
    <script>
      (function(){{
        const switcher = document.getElementById("projectSwitcherInline");
        if(!switcher) return;
        switcher.addEventListener("change", (e)=>{{
          const val = e.target.value;
          if(!val) {{
            window.location.href = "/ui{key_q}";
          }} else {{
            window.location.href = "/ui/projects/" + val + "{key_q}";
          }}
        }});
      }})();
    </script>
    """
    return ui_shell("Project", html_page, show_project_switcher=False)


@app.route("/ui/projects/<int:project_id>/delete", methods=["GET", "POST"])
def ui_project_delete(project_id):
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    project = prj.get_project(int(project_id))
    if not project:
        return ui_shell("Project not found", "<div class='card'><h2>Project not found</h2></div>"), 404

    err = ""
    if request.method == "POST":
        confirm = (request.form.get("confirm") or "").strip().upper()
        if confirm != "DELETE":
            err = "Type DELETE to confirm."
        else:
            prj.soft_delete_project(int(project_id))
            return redirect(url_for("ui_projects_archived") + key_q)

    html_page = f"""
    <div class="card">
      <h1 class="h1">Archive Project</h1>
      <div class="muted">This moves the project to the Recycle Bin. You can restore it later.</div>
    </div>
    {"<div class='card' style='border-color: rgba(231, 76, 60, .35)'><b>Error:</b> " + err + "</div>" if err else ""}
    <div class="card">
      <form method="POST" class="stack">
        <div>
          <label style="font-weight:800">Confirm delete</label>
          <input name="confirm" placeholder="Type DELETE to confirm" />
        </div>
        <div class="row">
          <button class="btn btn-primary" type="submit">Move to archive</button>
          <a class="btn" href="{url_for('ui_projects')}{key_q}">Cancel</a>
        </div>
      </form>
    </div>
    """
    return ui_shell("Delete Project", html, show_project_switcher=False)


@app.route("/ui/projects/archived", methods=["GET"])
def ui_projects_archived():
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    is_admin = bool(ADMIN_KEY and request.args.get("key") == ADMIN_KEY)
    org_id = current_org_id() if (REQUIRE_SUPERVISOR_KEY and not is_admin) else None
    projects = prj.list_projects(200, organization_id=org_id)
    archived = [p for p in projects if (p.get("status") or "ACTIVE").upper() == "ARCHIVED"]

    rows = []
    for p in archived:
        rows.append(
            f"""
            <tr>
              <td><span class="template-id">#{p.get('id')}</span></td>
              <td>
                <div class="template-name">{p.get('name')}</div>
                <div class="template-desc">{p.get('description') or 'No description provided.'}</div>
              </td>
              <td class="muted">{p.get('created_at') or ''}</td>
              <td>
                <div class="proj-actions">
                  <a class="btn btn-sm" href="{url_for('ui_project_restore', project_id=p.get('id'))}{key_q}">Restore</a>
                  <a class="btn btn-sm danger" href="{url_for('ui_project_purge', project_id=p.get('id'))}{key_q}">Delete forever</a>
                </div>
              </td>
            </tr>
            """
        )

    html_page = f"""
    <div class="card">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div>
          <div class="h1">Archived Projects</div>
          <div class="muted">Projects in the recycle bin. Restore or delete forever.</div>
        </div>
        <a class="btn" href="{url_for('ui_projects')}{key_q}">Back to projects</a>
      </div>
    </div>
    <div class="card" style="margin-top:16px">
      <table class="table">
        <thead>
          <tr>
            <th>ID</th>
            <th>Project</th>
            <th>Archived at</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {("".join(rows) if rows else "<tr><td colspan='4' class='muted' style='padding:18px'>No archived projects.</td></tr>")}
        </tbody>
      </table>
    </div>
    """
    return ui_shell("Archived Projects", html, show_project_switcher=False)


@app.route("/ui/projects/<int:project_id>/restore")
def ui_project_restore(project_id):
    gate = admin_gate()
    if gate:
        return gate
    prj.restore_project(int(project_id))
    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    return redirect(url_for("ui_projects_archived") + key_q)


@app.route("/ui/projects/<int:project_id>/purge", methods=["GET", "POST"])
def ui_project_purge(project_id):
    gate = admin_gate()
    if gate:
        return gate
    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    project = prj.get_project(int(project_id))
    if not project:
        return ui_shell("Project not found", "<div class='card'><h2>Project not found</h2></div>"), 404
    err = ""
    if request.method == "POST":
        confirm = (request.form.get("confirm") or "").strip().upper()
        if confirm != "DELETE":
            err = "Type DELETE to confirm."
        else:
            prj.hard_delete_project(int(project_id))
            return redirect(url_for("ui_projects_archived") + key_q)
    html_page = f"""
    <div class="card">
      <h1 class="h1">Delete Project Forever</h1>
      <div class="muted">This permanently deletes the project and all related data. This cannot be undone.</div>
    </div>
    {"<div class='card' style='border-color: rgba(231, 76, 60, .35)'><b>Error:</b> " + err + "</div>" if err else ""}
    <div class="card">
      <form method="POST" class="stack">
        <div>
          <label style="font-weight:800">Confirm delete</label>
          <input name="confirm" placeholder="Type DELETE to confirm" />
        </div>
        <div class="row">
          <button class="btn btn-primary" type="submit">Delete forever</button>
          <a class="btn" href="{url_for('ui_projects_archived')}{key_q}">Cancel</a>
        </div>
      </form>
    </div>
    """
    return ui_shell("Delete Project", html, show_project_switcher=False)


@app.route("/ui/projects/<int:project_id>/settings", methods=["GET", "POST"])
def ui_project_settings(project_id):
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    project = prj.get_project(int(project_id))
    if not project:
        return ui_shell("Project not found", "<div class='card'><h2>Project not found</h2></div>"), 404
    is_admin = bool(ADMIN_KEY and request.args.get("key") == ADMIN_KEY)
    org_id = current_org_id() if (REQUIRE_SUPERVISOR_KEY and not is_admin) else None

    err = ""
    if request.method == "POST":
        try:
            if project_is_locked(project):
                raise ValueError("Archived projects are read-only.")
            assignment_mode = (request.form.get("assignment_mode") or "OPTIONAL").strip().upper()
            status = (request.form.get("status") or (project.get("status") or "DRAFT")).strip().upper()
            is_test_project = 1 if request.form.get("is_test_project") == "on" else 0
            is_live_project = 1 if request.form.get("is_live_project") == "on" else 0
            expected_submissions = request.form.get("expected_submissions") or ""
            expected_submissions = int(expected_submissions) if str(expected_submissions).isdigit() else None
            expected_coverage = request.form.get("expected_coverage") or ""
            expected_coverage = int(expected_coverage) if str(expected_coverage).isdigit() else None
            organization_id = request.form.get("organization_id") or ""
            organization_id = int(organization_id) if str(organization_id).isdigit() else None
            if org_id is not None:
                organization_id = int(org_id)
            allow_unlisted_facilities = 1 if request.form.get("allow_unlisted_facilities") == "on" else 0
            prj.update_project(
                int(project_id),
                assignment_mode=assignment_mode,
                is_test_project=is_test_project,
                is_live_project=is_live_project,
                status=status,
                expected_submissions=expected_submissions,
                expected_coverage=expected_coverage,
                organization_id=organization_id,
                allow_unlisted_facilities=allow_unlisted_facilities,
            )
            return redirect(url_for("ui_project_detail", project_id=project_id) + key_q)
        except Exception as e:
            err = str(e)

    current_mode = (project.get("assignment_mode") or "OPTIONAL").strip().upper()
    current_status = (project.get("status") or "DRAFT").strip().upper()
    is_test = int(project.get("is_test_project") or 0) == 1
    is_live = int(project.get("is_live_project") or 0) == 1
    allow_unlisted = int(project.get("allow_unlisted_facilities") or 0) == 1
    current_expected = project.get("expected_submissions") if project.get("expected_submissions") is not None else ""
    current_expected_cov = project.get("expected_coverage") if project.get("expected_coverage") is not None else ""
    orgs = prj.list_organizations(200)
    org_options = "".join(
        [
            f"<option value='{o.get('id')}' {'selected' if str(o.get('id')) == str(project.get('organization_id') or '') else ''}>{o.get('name')}</option>"
            for o in orgs
            if org_id is None or int(o.get('id') or 0) == int(org_id)
        ]
    )
    org_label = None
    if org_id is not None:
        org_label = next((o.get("name") for o in orgs if int(o.get("id") or 0) == int(org_id)), "Organization")
    html_view = render_template_string(
        """
        <div class="card">
          <h1 class="h1">Project Settings</h1>
          <div class="muted">Set how strictly assignments are required for submissions.</div>
        </div>
        {% if err %}<div class="card" style="border-color: rgba(231, 76, 60, .35)"><b>Error:</b> {{err}}</div>{% endif %}
        <div class="card">
          <form method="POST" class="stack">
            <div>
              <label style="font-weight:800">Organization</label>
              {% if org_id %}
                <div class="muted" style="margin-top:6px">Assigned to: <b>{{org_label}}</b></div>
                <input type="hidden" name="organization_id" value="{{org_id}}" />
              {% else %}
                <select name="organization_id">
                  <option value="">Select organization</option>
                  {{org_options|safe}}
                </select>
              {% endif %}
            </div>
            <div>
              <label style="font-weight:800">Assignment strictness</label>
              <select name="assignment_mode">
                <option value="OPTIONAL" {% if mode == 'OPTIONAL' %}selected{% endif %}>A — Assignment optional</option>
                <option value="REQUIRED_PROJECT" {% if mode == 'REQUIRED_PROJECT' %}selected{% endif %}>B — Assignment required per project</option>
                <option value="REQUIRED_TEMPLATE" {% if mode == 'REQUIRED_TEMPLATE' %}selected{% endif %}>C — Assignment required per template</option>
              </select>
            </div>
            <div>
              <label style="font-weight:800">Allow unlisted facilities</label>
              <label class="row" style="gap:8px; margin-top:6px">
                <input type="checkbox" name="allow_unlisted_facilities" style="width:auto" {% if allow_unlisted %}checked{% endif %}/>
                <span>Allow enumerators to enter a facility name not on their list.</span>
              </label>
            </div>
            <div>
              <label style="font-weight:800">Expected submissions</label>
              <input name="expected_submissions" type="number" min="0" value="{{expected}}" />
            </div>
            <div>
              <label style="font-weight:800">Expected coverage</label>
              <input name="expected_coverage" type="number" min="0" value="{{expected_coverage}}" />
            </div>
            <div>
              <label style="font-weight:800">Status</label>
              <select name="status">
                <option value="DRAFT" {% if status == 'DRAFT' %}selected{% endif %}>Draft</option>
                <option value="ACTIVE" {% if status == 'ACTIVE' %}selected{% endif %}>Active</option>
                <option value="ARCHIVED" {% if status == 'ARCHIVED' %}selected{% endif %}>Archived</option>
              </select>
            </div>
            <div>
              <label style="font-weight:800">Environment flags</label>
              <div class="row" style="margin-top:6px">
                <label class="row" style="gap:8px">
                  <input type="checkbox" name="is_test_project" style="width:auto" {% if is_test %}checked{% endif %}/>
                  <span>Test project</span>
                </label>
                <label class="row" style="gap:8px">
                  <input type="checkbox" name="is_live_project" style="width:auto" {% if is_live %}checked{% endif %}/>
                  <span>Live data collection</span>
                </label>
              </div>
            </div>
            <div class="row">
              <button class="btn btn-primary" type="submit">Save settings</button>
              <a class="btn" href="{{ url_for('ui_project_detail', project_id=project_id) }}{{kq}}">Cancel</a>
            </div>
          </form>
        </div>
        """,
        project_id=project_id,
        mode=current_mode,
        status=current_status,
        is_test=is_test,
        is_live=is_live,
        err=err,
        kq=key_q,
        expected=current_expected,
        expected_coverage=current_expected_cov,
        org_options=org_options,
        org_id=org_id,
        org_label=org_label or "Organization",
        allow_unlisted=allow_unlisted,
    )
    return ui_shell("Project Settings", html, show_project_switcher=False)


@app.route("/ui/projects/<int:project_id>/analytics", methods=["GET"])
def ui_project_analytics(project_id):
    gate = admin_gate()
    if gate:
        return gate

    project = prj.get_project(int(project_id))
    if not project:
        return ui_shell("Project not found", "<div class='card'><h2>Project not found</h2></div>"), 404

    html = ana.render_project_analytics(project, ADMIN_KEY, request.args)
    return ui_shell("Analytics", html, show_project_switcher=False)


def _resolve_project_for_analytics() -> Optional[int]:
    pid_arg = request.args.get("project_id")
    if pid_arg and str(pid_arg).isdigit():
        return int(pid_arg)
    try:
        org_id = current_org_id()
        if org_id is None:
            sess_org = session.get("org_id")
            org_id = int(sess_org) if sess_org is not None and str(sess_org).isdigit() else None
        projects = prj.list_projects(1, organization_id=org_id)
        if projects:
            return int(projects[0].get("id"))
        if org_id is not None:
            return int(prj.get_default_project_id(int(org_id)))
    except Exception:
        return None
    return None


@app.route("/ui/analytics")
def ui_analytics_root():
    gate = admin_gate()
    if gate:
        return gate
    pid = _resolve_project_for_analytics()
    if not pid:
        return ui_shell("Analytics", "<div class='card'><h2>No projects yet</h2><div class='muted'>Create a project to view analytics.</div></div>"), 200
    return redirect(url_for("ui_project_analytics", project_id=pid) + (f"?key={ADMIN_KEY}" if ADMIN_KEY else ""))


@app.route("/ui/analytics/enumerators")
def ui_analytics_enumerators():
    gate = admin_gate()
    if gate:
        return gate
    pid = _resolve_project_for_analytics()
    if not pid:
        return ui_shell("Analytics", "<div class='card'><h2>No projects yet</h2><div class='muted'>Create a project to view analytics.</div></div>"), 200
    key_q = f"&key={ADMIN_KEY}" if ADMIN_KEY else ""
    return redirect(url_for("ui_project_analytics", project_id=pid) + f"?tab=enumerators{key_q}")


@app.route("/ui/analytics/qa")
def ui_analytics_qa():
    gate = admin_gate()
    if gate:
        return gate
    pid = _resolve_project_for_analytics()
    if not pid:
        return ui_shell("Analytics", "<div class='card'><h2>No projects yet</h2><div class='muted'>Create a project to view analytics.</div></div>"), 200
    key_q = f"&key={ADMIN_KEY}" if ADMIN_KEY else ""
    return redirect(url_for("ui_project_analytics", project_id=pid) + f"?tab=quality{key_q}")


@app.route("/ui/analytics/coverage")
def ui_analytics_coverage():
    gate = admin_gate()
    if gate:
        return gate
    pid = _resolve_project_for_analytics()
    if not pid:
        return ui_shell("Analytics", "<div class='card'><h2>No projects yet</h2><div class='muted'>Create a project to view analytics.</div></div>"), 200
    key_q = f"&key={ADMIN_KEY}" if ADMIN_KEY else ""
    return redirect(url_for("ui_project_analytics", project_id=pid) + f"?tab=coverage{key_q}")


# ---------------------------------------------------------
# Researchers (Enumerators) UI (Supervisor)
# ---------------------------------------------------------
@app.route("/ui/researchers")
def ui_researchers():
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    if PROJECT_REQUIRED:
        projects = prj.list_projects(200, organization_id=current_org_id())
        options = "".join(
            [f"<option value='{p.get('id')}'>{html.escape(p.get('name') or 'Project')}</option>" for p in projects]
        )
        html_view = f"""
        <div class="card">
          <div class="row" style="justify-content:space-between; align-items:center;">
            <div>
              <div class="h1">Researchers</div>
              <div class="muted">Select a project to view researcher profiles.</div>
            </div>
            <a class="btn" href="/ui{key_q}">Back to dashboard</a>
          </div>
        </div>
        <div class="card" style="margin-top:16px">
          <form method="GET" onsubmit="event.preventDefault(); const pid=this.querySelector('select').value; if(pid) window.location='/ui/projects/'+pid+'/researchers{key_q}';">
            <select name="project_id" required>
              <option value="">Choose project</option>
              {options}
            </select>
            <button class="btn btn-primary" type="submit" style="margin-left:8px">Open</button>
          </form>
        </div>
        """
        return ui_shell("Researchers", html_view, show_project_switcher=False)
    try:
        names = sup.list_researchers(limit=300)
    except Exception as e:
        return ui_shell("Researchers", f"<div class='card'><h2>Researchers</h2><p class='muted'>Error: {e}</p></div>")

    rows = []
    for n in names:
        safe_name = n.replace("/", " ").strip()
        rows.append(
            f"""
            <tr>
              <td><b>{safe_name}</b></td>
              <td><a class="btn btn-sm" href="/ui/researchers/{safe_name}{key_q}">View profile</a></td>
            </tr>
            """
        )

    html_page = f"""
    <div class="card">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div>
          <div class="h1">Researchers (Enumerators)</div>
          <div class="muted">Enumerators with at least one submission.</div>
        </div>
        <a class="btn" href="/ui{key_q}">Back to dashboard</a>
      </div>
    </div>
    <div class="card" style="margin-top:16px">
      <table class="table">
        <thead>
          <tr>
            <th>Name</th>
            <th style="width:160px">Action</th>
          </tr>
        </thead>
        <tbody>
          {("".join(rows) if rows else "<tr><td colspan='2' class='muted' style='padding:18px'>No enumerators yet.</td></tr>")}
        </tbody>
      </table>
    </div>
    """
    return ui_shell("Researchers", html_page)


@app.route("/ui/researchers/<path:name>")
def ui_researcher_profile(name):
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    enum_name = (name or "").strip()
    if not enum_name:
        return redirect(url_for("ui_researchers") + key_q)

    try:
        p = sup.get_researcher_profile(enum_name, alerts_limit=10)
    except Exception as e:
        return ui_shell("Researcher Profile", f"<div class='card'><h2>Researcher Profile</h2><p class='muted'>Error: {e}</p></div>")

    alert_rows = []
    for a in p.recent_alerts:
        flags = ", ".join(a.get("flags") or [])
        alert_rows.append(
            f"""
            <tr>
              <td>{a.get('survey_id')}</td>
              <td>{a.get('facility_name')}</td>
              <td>{flags}</td>
              <td>{a.get('severity',0):.2f}</td>
              <td><a class="btn btn-sm" href="/ui/surveys/{a.get('survey_id')}{key_q}">View</a></td>
            </tr>
            """
        )

    avg_txt = f"{int(p.avg_completion_seconds)}s" if p.avg_completion_seconds is not None else "—"
    gps_txt = f"{p.gps_capture_pct:.0f}%" if p.gps_capture_pct is not None else "—"
    last_txt = p.last_activity or "—"

    html_page = f"""
    <div class="card">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div>
          <div class="h1">Researcher Profile</div>
          <div class="muted">Derived operational profile (no login).</div>
        </div>
        <div class="row" style="gap:10px">
          <a class="btn btn-sm" href="/ui/researchers{key_q}">Back to researchers</a>
          <a class="btn btn-sm" href="/ui/analytics/enumerators{key_q}">Back to performance</a>
        </div>
      </div>
    </div>

    <div class="card" style="margin-top:16px">
      <h3 style="margin-top:0">{p.enumerator_name}</h3>
      <div class="row" style="gap:24px; flex-wrap:wrap;">
        <div><div class="muted">Total surveys</div><div style="font-weight:800">{p.total_surveys}</div></div>
        <div><div class="muted">Completed</div><div style="font-weight:800">{p.completed}</div></div>
        <div><div class="muted">Drafts</div><div style="font-weight:800">{p.drafts}</div></div>
        <div><div class="muted">Unique facilities</div><div style="font-weight:800">{p.unique_facilities}</div></div>
        <div><div class="muted">Templates used</div><div style="font-weight:800">{p.templates_used}</div></div>
        <div><div class="muted">Avg completion time</div><div style="font-weight:800">{avg_txt}</div></div>
        <div><div class="muted">GPS capture</div><div style="font-weight:800">{gps_txt}</div></div>
        <div><div class="muted">Last activity</div><div style="font-weight:800">{last_txt}</div></div>
      </div>
    </div>

    <div class="card" style="margin-top:16px">
      <h3 style="margin-top:0">QA alerts (recent)</h3>
      <table class="table">
        <thead>
          <tr>
            <th>Survey</th>
            <th>Facility</th>
            <th>Flags</th>
            <th>Severity</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          {("".join(alert_rows) if alert_rows else "<tr><td colspan='5' class='muted' style='padding:18px'>No QA alerts for this enumerator.</td></tr>")}
        </tbody>
      </table>
    </div>
    """
    return ui_shell("Researcher Profile", html_page)


@app.route("/ui/projects/<int:project_id>/dashboard", methods=["GET"])
def ui_project_dashboard(project_id):
    return ui_project_detail(project_id)


@app.route("/ui/projects/<int:project_id>/researchers", methods=["GET"])
def ui_project_researchers(project_id):
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    project = prj.get_project(int(project_id))
    if not project:
        return ui_shell("Project not found", "<div class='card'><h2>Project not found</h2></div>"), 404

    target_name = (request.args.get("name") or "").strip()
    range_days = request.args.get("days") or "30"
    range_days = int(range_days) if str(range_days).isdigit() else 30
    start_date = (request.args.get("start_date") or "").strip()
    end_date = (request.args.get("end_date") or "").strip()
    date_err = ""
    if (start_date and not end_date) or (end_date and not start_date):
        date_err = "Provide both start and end dates."
    if start_date and end_date and start_date > end_date:
        date_err = "Start date must be before end date."
    profiles = prj.researcher_profiles(int(project_id))
    detail = next((p for p in profiles if p.get("display_name") == target_name), None) if target_name else None
    derived_profile = None
    if target_name:
        try:
            derived_profile = sup.get_researcher_profile(target_name, alerts_limit=6)
        except Exception:
            derived_profile = None
    if detail and start_date and end_date and not date_err:
        series = prj.enumerator_activity_series_range(int(project_id), target_name, start_date, end_date)
    else:
        series = prj.enumerator_activity_series(int(project_id), target_name, days=range_days) if detail else []

    enum_id_for_detail = None
    if target_name:
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT id FROM enumerators WHERE project_id=? AND LOWER(name)=LOWER(?) LIMIT 1",
                    (int(project_id), target_name),
                )
                row = cur.fetchone()
                if row:
                    enum_id_for_detail = int(row["id"])
        except Exception:
            enum_id_for_detail = None

    def _sparkline(values, width=360, height=80, stroke="#8E5CFF"):
        if not values:
            return "<div class='muted'>No data yet.</div>"
        min_v = min(values)
        max_v = max(values)
        span = max_v - min_v if max_v != min_v else 1
        step = (width - 8) / max(1, len(values) - 1)
        points = []
        for i, v in enumerate(values):
            x = 4 + i * step
            y = 4 + (height - 8) * (1 - ((v - min_v) / span))
            points.append(f"{x:.2f},{y:.2f}")
        return f"""<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}">
          <polyline fill="none" stroke="{stroke}" stroke-width="2" points="{' '.join(points)}" />
        </svg>"""

    rows = []
    for p in profiles:
        rows.append(
            f"""
            <tr onclick="window.location.href='/ui/projects/{project_id}/researchers?name={p.get('display_name')}{key_q}'" style="cursor:pointer">
              <td>{p.get('researcher_id')}</td>
              <td>{p.get('display_name')}</td>
              <td>{p.get('reliability_score')}</td>
              <td>{p.get('completed_submissions')}</td>
              <td>{p.get('qa_flags')}</td>
              <td>{p.get('avg_completion_minutes') if p.get('avg_completion_minutes') is not None else '—'}</td>
            </tr>
            """
        )

    detail_html = ""
    if detail:
        domains = ", ".join(detail.get("domains") or []) or "—"
        regions = ", ".join(detail.get("regions") or []) or "—"
        time_vals = [float(r.get("avg_minutes") or 0) for r in series if r.get("avg_minutes") is not None]
        qa_vals = [int(r.get("qa_flags") or 0) for r in series]
        time_chart = _sparkline(time_vals, stroke="#111827") if time_vals else "<div class='muted'>No completion time data yet.</div>"
        qa_chart = _sparkline(qa_vals, stroke="#dc2626") if qa_vals else "<div class='muted'>No QA flags yet.</div>"
        series_rows = "".join(
            [
                f"<tr><td>{r.get('day') or ''}</td><td>{int(r.get('completed') or 0)}</td><td>{int(r.get('drafts') or 0)}</td><td>{int(r.get('qa_flags') or 0)}</td><td>{(round(float(r.get('avg_minutes')),1) if r.get('avg_minutes') is not None else '—')}</td></tr>"
                for r in series
            ]
        )
        alerts_html = ""
        if derived_profile and derived_profile.recent_alerts:
            alerts_html = "".join(
                [
                    f"<tr><td>#{a.get('survey_id')}</td><td>{a.get('facility_name') or '—'}</td><td>{', '.join(a.get('flags') or [])}</td><td>{a.get('severity'):.2f}</td></tr>"
                    for a in derived_profile.recent_alerts
                ]
            )
        assign_btn = ""
        if enum_id_for_detail:
            assign_btn = f"<a class='btn btn-sm' href='{url_for('ui_project_assignments', project_id=project_id)}?enumerator_id={enum_id_for_detail}{'&key=' + ADMIN_KEY if ADMIN_KEY else ''}'>Assign task</a>"
        detail_html = f"""
        <div class="card" style="margin-top:16px">
          <h3 style="margin-top:0">Researcher profile</h3>
          <div class="row" style="justify-content:flex-end; margin-bottom:12px;">{assign_btn}</div>
          <div class="row" style="gap:24px; flex-wrap:wrap;">
            <div>
              <div class="muted">Name</div>
              <div style="font-weight:800">{detail.get('display_name')}</div>
            </div>
            <div>
              <div class="muted">Reliability score</div>
              <div style="font-weight:800">{detail.get('reliability_score')}</div>
            </div>
            <div>
              <div class="muted">Quality / Consistency / Completion</div>
              <div style="font-weight:800">{detail.get('quality_score')} / {detail.get('consistency_score')} / {detail.get('completion_score')}</div>
            </div>
          </div>
          <div class="row" style="gap:24px; margin-top:12px; flex-wrap:wrap;">
            <div>
              <div class="muted">Domains</div>
              <div>{domains}</div>
            </div>
            <div>
              <div class="muted">Regions</div>
              <div>{regions}</div>
            </div>
          </div>
          <div class="row" style="gap:24px; margin-top:12px; flex-wrap:wrap;">
            <div>
              <div class="muted">First activity</div>
              <div>{detail.get('first_activity_at') or '—'}</div>
            </div>
            <div>
              <div class="muted">Last activity</div>
              <div>{detail.get('last_activity_at') or '—'}</div>
            </div>
          </div>
        </div>
        {f'''
        <div class="card" style="margin-top:16px">
          <h3 style="margin-top:0">Derived field profile</h3>
          <div class="row" style="gap:24px; flex-wrap:wrap;">
            <div><div class="muted">Total surveys</div><div style="font-weight:800">{derived_profile.total_surveys}</div></div>
            <div><div class="muted">Completed</div><div style="font-weight:800">{derived_profile.completed}</div></div>
            <div><div class="muted">Drafts</div><div style="font-weight:800">{derived_profile.drafts}</div></div>
            <div><div class="muted">Unique facilities</div><div style="font-weight:800">{derived_profile.unique_facilities}</div></div>
            <div><div class="muted">Templates used</div><div style="font-weight:800">{derived_profile.templates_used}</div></div>
            <div><div class="muted">Avg completion (sec)</div><div style="font-weight:800">{(int(derived_profile.avg_completion_seconds) if derived_profile.avg_completion_seconds else '—')}</div></div>
            <div><div class="muted">GPS capture</div><div style="font-weight:800">{(f"{derived_profile.gps_capture_pct:.0f}%" if derived_profile.gps_capture_pct is not None else '—')}</div></div>
            <div><div class="muted">QA alerts</div><div style="font-weight:800">{derived_profile.qa_alerts_count}</div></div>
          </div>
          <div class="muted" style="margin-top:10px">Last activity: {derived_profile.last_activity or '—'}</div>
        </div>
        <div class="card" style="margin-top:16px">
          <h3 style="margin-top:0">Recent QA alerts</h3>
          <table class="table">
            <thead><tr><th>Survey</th><th>Facility</th><th>Flags</th><th>Severity</th></tr></thead>
            <tbody>
              {alerts_html if alerts_html else "<tr><td colspan='4' class='muted' style='padding:18px'>No recent QA alerts.</td></tr>"}
            </tbody>
          </table>
        </div>
        ''' if derived_profile else ""}
        <div class="card" style="margin-top:16px">
          <h3 style="margin-top:0">Enumerator trends (last 30 days)</h3>
          <form method="GET" class="row" style="gap:12px; margin-bottom:12px; flex-wrap:wrap;">
            <input type="hidden" name="name" value="{detail.get('display_name')}" />
            {"<input type='hidden' name='key' value='" + ADMIN_KEY + "' />" if ADMIN_KEY else ""}
            <div>
              <label class="muted" style="font-size:12px">Quick range</label>
              <select name="days" onchange="this.form.submit()">
                <option value="7" {'selected' if range_days == 7 else ''}>Last 7 days</option>
                <option value="30" {'selected' if range_days == 30 else ''}>Last 30 days</option>
                <option value="90" {'selected' if range_days == 90 else ''}>Last 90 days</option>
              </select>
            </div>
            <div>
              <label class="muted" style="font-size:12px">Start date</label>
              <input type="date" name="start_date" value="{start_date}" />
            </div>
            <div>
              <label class="muted" style="font-size:12px">End date</label>
              <input type="date" name="end_date" value="{end_date}" />
            </div>
            <div style="align-self:flex-end">
              <button class="btn btn-sm" type="submit">Apply</button>
            </div>
          </form>
          {f"<div class='muted' style='margin-bottom:10px; color:#b91c1c'>{date_err}</div>" if date_err else ""}
          <div class="row" style="gap:18px; flex-wrap:wrap;">
            <div>
              <div class="muted" style="font-size:12px">Avg completion time (min)</div>
              <div class="muted" style="font-size:11px">Line chart • daily average</div>
              {time_chart}
            </div>
            <div>
              <div class="muted" style="font-size:12px">QA flags history</div>
              <div class="muted" style="font-size:11px">Line chart • daily flags</div>
              {qa_chart}
            </div>
          </div>
          <table class="table" style="margin-top:12px">
            <thead>
              <tr>
                <th>Date</th>
                <th style="width:120px">Completed</th>
                <th style="width:120px">Drafts</th>
                <th style="width:120px">QA flags</th>
                <th style="width:160px">Avg time (min)</th>
              </tr>
            </thead>
            <tbody>
              {series_rows if series_rows else "<tr><td colspan='5' class='muted' style='padding:18px'>No activity yet.</td></tr>"}
            </tbody>
          </table>
        </div>
        """

    html_page = f"""
    <div class="card">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div>
          <div class="h1">Researcher Profiles — {project.get('name')}</div>
          <div class="muted">Computed, evidence-based profiles. Private by default.</div>
        </div>
        <div class="row" style="gap:10px">
          <a class="btn btn-sm" href="/ui/projects/{project_id}/researchers/export.csv{key_q}">Export CSV</a>
          <a class="btn btn-sm" href="/ui/projects/{project_id}/researchers/export.json{key_q}">Export JSON</a>
          <a class="btn" href="{url_for('ui_project_analytics', project_id=project_id)}{key_q}?tab=enumerators">Back to analytics</a>
        </div>
      </div>
    </div>

    <div class="card" style="margin-top:16px">
      <table class="table">
        <thead>
          <tr>
            <th>ID</th>
            <th>Researcher</th>
            <th>Reliability</th>
            <th>Completed</th>
            <th>QA flags</th>
            <th>Avg time (min)</th>
          </tr>
        </thead>
        <tbody>
          {("".join(rows) if rows else "<tr><td colspan='6' class='muted' style='padding:18px'>No researcher activity yet.</td></tr>")}
        </tbody>
      </table>
    </div>
    {detail_html}
    """
    return ui_shell("Researcher Profiles", html_page, show_project_switcher=False)


@app.route("/ui/projects/<int:project_id>/researchers/export.csv")
def ui_project_researchers_export(project_id):
    gate = admin_gate()
    if gate:
        return gate

    project = prj.get_project(int(project_id))
    if not project:
        return ui_shell("Project not found", "<div class='card'><h2>Project not found</h2></div>"), 404

    rows = prj.researcher_profiles(int(project_id))
    start_date = (request.args.get("start_date") or "").strip()
    end_date = (request.args.get("end_date") or "").strip()
    days = request.args.get("days") or ""
    date_tag = ""
    if start_date and end_date:
        date_tag = f"{start_date.replace('-', '')}-{end_date.replace('-', '')}"
    elif str(days).isdigit():
        date_tag = f"last{days}d"
    filename = f"researcher_profiles_{date_tag + '_' if date_tag else ''}{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    path = os.path.join(EXPORT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "researcher_id",
            "display_name",
            "roles",
            "completed_submissions",
            "drafts",
            "qa_flags",
            "quality_score",
            "consistency_score",
            "completion_score",
            "experience_score",
            "reliability_score",
            "avg_completion_minutes",
            "domains",
            "regions",
            "first_activity_at",
            "last_activity_at",
        ])
        for r in rows:
            w.writerow([
                r.get("researcher_id"),
                r.get("display_name"),
                ",".join(r.get("roles") or []),
                r.get("completed_submissions"),
                r.get("drafts"),
                r.get("qa_flags"),
                r.get("quality_score"),
                r.get("consistency_score"),
                r.get("completion_score"),
                r.get("experience_score"),
                r.get("reliability_score"),
                r.get("avg_completion_minutes"),
                "; ".join(r.get("domains") or []),
                "; ".join(r.get("regions") or []),
                r.get("first_activity_at"),
                r.get("last_activity_at"),
            ])
    return send_file(
        path,
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


@app.route("/ui/projects/<int:project_id>/researchers/export.json")
def ui_project_researchers_export_json(project_id):
    gate = admin_gate()
    if gate:
        return gate

    project = prj.get_project(int(project_id))
    if not project:
        return ui_shell("Project not found", "<div class='card'><h2>Project not found</h2></div>"), 404

    rows = prj.researcher_profiles(int(project_id))
    start_date = (request.args.get("start_date") or "").strip()
    end_date = (request.args.get("end_date") or "").strip()
    days = request.args.get("days") or ""
    date_tag = ""
    if start_date and end_date:
        date_tag = f"{start_date.replace('-', '')}-{end_date.replace('-', '')}"
    elif str(days).isdigit():
        date_tag = f"last{days}d"
    filename = f"researcher_profiles_{date_tag + '_' if date_tag else ''}{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    path = os.path.join(EXPORT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    return send_file(
        path,
        mimetype="application/json",
        as_attachment=True,
        download_name=filename,
    )


@app.route("/ui/projects/<int:project_id>/templates", methods=["GET", "POST"])
def ui_project_templates(project_id):
    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    proj_q = f"{key_q}&project_id={project_id}" if key_q else f"?project_id={project_id}"
    return redirect(url_for("ui_templates") + proj_q)


@app.route("/ui/projects/<int:project_id>/coverage", methods=["GET", "POST"])
def ui_project_coverage(project_id):
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    project = prj.get_project(int(project_id))
    if not project:
        return ui_shell("Project not found", "<div class='card'><h2>Project not found</h2></div>"), 404

    msg = ""
    err = ""
    sup_ctx = current_supervisor()
    sup_id = int(sup_ctx.get("id")) if sup_ctx and sup_ctx.get("id") else None
    scheme_id = request.values.get("scheme_id") or project.get("coverage_scheme_id") or ""
    scheme_id = int(scheme_id) if str(scheme_id).isdigit() else None
    edit_node_id = request.args.get("edit_node_id") or ""
    edit_node_id = int(edit_node_id) if str(edit_node_id).isdigit() else None

    if request.method == "POST":
        try:
            if project_is_locked(project):
                raise ValueError("Archived projects are read-only.")
            action = (request.form.get("action") or "").strip()
            if action == "create_scheme":
                name = (request.form.get("scheme_name") or "").strip()
                if not name:
                    raise ValueError("Scheme name is required.")
                desc = (request.form.get("scheme_description") or "").strip()
                scheme_id = cov.create_scheme(name, description=desc)
                prj.update_project(project_id, coverage_scheme_id=int(scheme_id))
                msg = "Coverage scheme created and set active."
            elif action == "set_active_scheme":
                scheme_id = request.form.get("scheme_id") or ""
                scheme_id = int(scheme_id) if str(scheme_id).isdigit() else None
                if not scheme_id:
                    raise ValueError("Select a coverage scheme first.")
                prj.update_project(project_id, coverage_scheme_id=int(scheme_id))
                msg = "Active coverage scheme updated."
            elif action == "create_node":
                if not scheme_id:
                    raise ValueError("Select a coverage scheme first.")
                name = (request.form.get("node_name") or "").strip()
                parent_id = request.form.get("parent_id") or ""
                parent_id = int(parent_id) if str(parent_id).isdigit() else None
                gps_lat = request.form.get("gps_lat") or ""
                gps_lng = request.form.get("gps_lng") or ""
                gps_radius_m = request.form.get("gps_radius_m") or ""
                gps_lat_val = float(gps_lat) if str(gps_lat).strip() else None
                gps_lng_val = float(gps_lng) if str(gps_lng).strip() else None
                gps_radius_val = float(gps_radius_m) if str(gps_radius_m).strip() else None
                cov.create_node(
                    int(scheme_id),
                    name,
                    parent_id=parent_id,
                    gps_lat=gps_lat_val,
                    gps_lng=gps_lng_val,
                    gps_radius_m=gps_radius_val,
                )
                msg = "Coverage node added."
            elif action == "update_node":
                node_id = request.form.get("node_id") or ""
                node_id = int(node_id) if str(node_id).isdigit() else None
                if not node_id:
                    raise ValueError("Missing node to update.")
                name = (request.form.get("node_name") or "").strip()
                parent_id = request.form.get("parent_id") or ""
                parent_id = int(parent_id) if str(parent_id).isdigit() else None
                gps_lat = request.form.get("gps_lat") or ""
                gps_lng = request.form.get("gps_lng") or ""
                gps_radius_m = request.form.get("gps_radius_m") or ""
                gps_lat_val = float(gps_lat) if str(gps_lat).strip() else None
                gps_lng_val = float(gps_lng) if str(gps_lng).strip() else None
                gps_radius_val = float(gps_radius_m) if str(gps_radius_m).strip() else None
                cov.update_node(
                    node_id,
                    name=name,
                    parent_id=parent_id,
                    gps_lat=gps_lat_val,
                    gps_lng=gps_lng_val,
                    gps_radius_m=gps_radius_val,
                )
                msg = "Coverage node updated."
            elif action == "delete_node":
                node_id = request.form.get("node_id") or ""
                node_id = int(node_id) if str(node_id).isdigit() else None
                if not node_id:
                    raise ValueError("Missing node to delete.")
                cov.delete_node(node_id)
                msg = "Coverage node deleted."
        except Exception as e:
            err = str(e)

    schemes = cov.list_schemes(200)
    active_scheme_id = project.get("coverage_scheme_id")
    if scheme_id is None and active_scheme_id:
        scheme_id = int(active_scheme_id)
    if scheme_id is None and schemes:
        scheme_id = schemes[0]["id"]

    nodes = cov.list_nodes(int(scheme_id), limit=2000) if scheme_id else []
    node_map = {n["id"]: n for n in nodes}
    edit_node = cov.get_node(edit_node_id) if edit_node_id else None

    rows = []
    for n in nodes:
        parent_name = node_map.get(n.get("parent_id"), {}).get("name") if n.get("parent_id") else "—"
        level = int(n.get("level_index") or 0)
        left_pad = 8 + (level * 16)
        rows.append(
            f"""
            <tr>
              <td><span class="cov-id">#{n.get('id')}</span></td>
              <td>
                <div class="cov-node-cell" style="padding-left:{left_pad}px">
                  <span class="cov-node-dot"></span>
                  <span>{html.escape(n.get('name') or '')}</span>
                </div>
              </td>
              <td><span class="cov-level-badge">L{level}</span></td>
              <td class="muted">{html.escape(parent_name or '—')}</td>
              <td class="muted">{html.escape(n.get('created_at') or '')}</td>
              <td>
                <div class="action-buttons">
                  <a class="btn btn-sm" href="{url_for('ui_project_coverage', project_id=project_id)}?scheme_id={scheme_id}&edit_node_id={n.get('id')}{'&key=' + ADMIN_KEY if ADMIN_KEY else ''}">Edit</a>
                  <form method="POST" style="display:inline">
                    <input type="hidden" name="scheme_id" value="{scheme_id or ''}" />
                    <input type="hidden" name="action" value="delete_node" />
                    <input type="hidden" name="node_id" value="{n.get('id')}" />
                    {"<input type='hidden' name='key' value='" + ADMIN_KEY + "' />" if ADMIN_KEY else ""}
                    <button class="btn btn-sm" type="submit">Delete</button>
                  </form>
                </div>
              </td>
            </tr>
            """
        )

    scheme_options = "".join(
        [
            f"<option value='{s['id']}' {'selected' if scheme_id == s['id'] else ''}>{s['name']}</option>"
            for s in schemes
        ]
    )
    active_label = next(
        (s.get("name") for s in schemes if scheme_id and int(s.get("id")) == int(scheme_id)),
        "—",
    )
    parent_options = "<option value=''>No parent</option>" + "".join(
        [
            f"<option value='{n['id']}' {'selected' if edit_node and edit_node.get('parent_id') == n['id'] else ''}>{n['name']}</option>"
            for n in nodes
        ]
    )

    html_page = f"""
    <style>
      .cov-page {{
        min-height: 100vh;
        background:
          radial-gradient(980px 430px at -8% -20%, rgba(124,58,237,.13), transparent 62%),
          radial-gradient(820px 340px at 108% -12%, rgba(139,92,246,.11), transparent 58%),
          linear-gradient(180deg, #f8f6ff 0%, #f3f4f8 100%);
        padding: 20px 0 34px;
      }}
      html[data-theme="dark"] .cov-page {{
        background:
          radial-gradient(980px 430px at -8% -20%, rgba(124,58,237,.25), transparent 62%),
          radial-gradient(820px 340px at 108% -12%, rgba(139,92,246,.2), transparent 58%),
          linear-gradient(180deg, #0f1221 0%, #11162a 100%);
      }}
      .cov-shell {{
        max-width: 1200px;
        margin: 0 auto;
        padding: 0 16px;
        display: grid;
        gap: 14px;
      }}
      .cov-hero {{
        border: 1px solid rgba(124,58,237,.28);
        border-radius: 22px;
        padding: 18px;
        background: linear-gradient(125deg, rgba(124,58,237,.18) 0%, rgba(255,255,255,.97) 46%, rgba(224,231,255,.62) 100%);
        box-shadow: 0 16px 36px rgba(15,18,34,.1);
      }}
      html[data-theme="dark"] .cov-hero {{
        background: linear-gradient(125deg, rgba(124,58,237,.38) 0%, rgba(21,24,44,.95) 46%, rgba(35,40,71,.9) 100%);
        border-color: rgba(167,139,250,.35);
      }}
      .cov-hero-row {{
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 14px;
      }}
      .cov-kicker {{
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 5px 10px;
        border-radius: 999px;
        background: rgba(124,58,237,.14);
        color: var(--primary);
        font-size: 11px;
        font-weight: 800;
        letter-spacing: .06em;
        text-transform: uppercase;
      }}
      .cov-title {{
        margin: 10px 0 4px;
        font-size: 30px;
        font-weight: 900;
        letter-spacing: -.02em;
        color: #111827;
      }}
      html[data-theme="dark"] .cov-title {{ color: #f8fafc; }}
      .cov-desc {{
        font-size: 14px;
        color: #475569;
        max-width: 760px;
      }}
      html[data-theme="dark"] .cov-desc {{ color: #cbd5e1; }}
      .cov-card {{
        border: 1px solid var(--border);
        background: var(--surface);
        border-radius: 18px;
        padding: 16px;
        box-shadow: 0 12px 28px rgba(15,18,34,.06);
      }}
      .cov-grid-2 {{
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
      }}
      .cov-tip {{
        border: 1px solid rgba(124,58,237,.22);
        background: rgba(124,58,237,.08);
        color: var(--text);
        border-radius: 12px;
        padding: 10px 12px;
        font-size: 13px;
        margin-bottom: 12px;
      }}
      .cov-form-title {{
        margin: 0 0 10px;
        font-size: 15px;
        font-weight: 800;
      }}
      .cov-card label {{
        display: block;
        margin-bottom: 6px;
        font-size: 11px;
        font-weight: 800;
        letter-spacing: .06em;
        text-transform: uppercase;
        color: var(--muted);
      }}
      .cov-card input:not([type="hidden"]),
      .cov-card select,
      .cov-card textarea {{
        width: 100%;
        border: 1px solid rgba(124,58,237,.24);
        border-radius: 12px;
        background: linear-gradient(180deg, #ffffff 0%, #f7f7fb 100%);
        color: var(--text);
        padding: 11px 12px;
        font-size: 14px;
        transition: border-color .18s ease, box-shadow .18s ease, background .18s ease;
      }}
      html[data-theme="dark"] .cov-card input:not([type="hidden"]),
      html[data-theme="dark"] .cov-card select,
      html[data-theme="dark"] .cov-card textarea {{
        background: linear-gradient(180deg, rgba(30,41,59,.92) 0%, rgba(17,24,39,.92) 100%);
        border-color: rgba(167,139,250,.3);
        color: #e5e7eb;
      }}
      .cov-card input:not([type="hidden"])::placeholder,
      .cov-card textarea::placeholder {{
        color: #9aa4b2;
      }}
      .cov-card input:not([type="hidden"]):focus,
      .cov-card select:focus,
      .cov-card textarea:focus {{
        outline: none;
        border-color: rgba(124,58,237,.72);
        box-shadow: 0 0 0 4px rgba(124,58,237,.14);
      }}
      .cov-id {{
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        border: 1px solid var(--border);
        background: var(--surface-2);
        padding: 4px 9px;
        font-size: 11px;
        font-weight: 800;
      }}
      .cov-node-cell {{
        display: inline-flex;
        align-items: center;
        gap: 8px;
      }}
      .cov-node-dot {{
        width: 8px;
        height: 8px;
        border-radius: 999px;
        background: linear-gradient(90deg, var(--primary), #8b5cf6);
        box-shadow: 0 0 0 3px rgba(124,58,237,.15);
      }}
      .cov-level-badge {{
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        border: 1px solid rgba(124,58,237,.26);
        color: var(--primary);
        background: rgba(124,58,237,.08);
        padding: 3px 8px;
        font-size: 11px;
        font-weight: 800;
      }}
      .cov-alert-success {{
        border: 1px solid rgba(34,197,94,.35);
        background: rgba(240,253,244,.85);
        color: #166534;
        border-radius: 14px;
        padding: 12px 14px;
        font-size: 13px;
      }}
      .cov-alert-error {{
        border: 1px solid rgba(239,68,68,.35);
        background: rgba(254,242,242,.88);
        color: #991b1b;
        border-radius: 14px;
        padding: 12px 14px;
        font-size: 13px;
      }}
      @media (max-width: 900px) {{
        .cov-hero-row {{ flex-direction: column; }}
        .cov-title {{ font-size: 26px; }}
        .cov-grid-2 {{ grid-template-columns: 1fr; }}
      }}
    </style>

    <div class="cov-page">
      <div class="cov-shell">
        <section class="cov-hero">
          <div class="cov-hero-row">
            <div>
              <div class="cov-kicker">Coverage nodes</div>
              <h1 class="cov-title">{html.escape(project.get('name') or 'Project')}</h1>
              <div class="cov-desc">Build a clean location tree (Country → State → LGA → Facility) and assign enumerators to nodes. Only one scheme is active per project.</div>
            </div>
            <a class="btn" href="{url_for('ui_project_detail', project_id=project_id)}{key_q}">Back to project</a>
          </div>
        </section>

        {"<div class='cov-alert-success'><b>Success:</b> " + html.escape(msg) + "</div>" if msg else ""}
        {"<div class='cov-alert-error'><b>Error:</b> " + html.escape(err) + "</div>" if err else ""}

        <section class="cov-card">
          <div class="cov-tip"><b>Tip:</b> Start from the top level (Country/State) and then add children (LGA → Ward → Facility).</div>
          <div class="cov-grid-2">
            <form method="POST" class="stack">
              <input type="hidden" name="action" value="create_scheme" />
              <h3 class="cov-form-title">Create a coverage set</h3>
              <input name="scheme_name" placeholder="e.g., Nigeria → State → LGA → Ward" />
              <input name="scheme_description" placeholder="Optional description (who/what this scheme covers)" />
              <button class="btn btn-primary" type="submit">Create coverage set</button>
            </form>
            <form method="POST" class="stack">
              <input type="hidden" name="action" value="set_active_scheme" />
              <h3 class="cov-form-title">Select active coverage set</h3>
              <select name="scheme_id">
                {scheme_options}
              </select>
              <div class="muted">Only one scheme can be active per project.</div>
              <button class="btn" type="submit">Set active scheme</button>
              <div class="muted" style="margin-top:6px">Current: <b>{html.escape(active_label)}</b></div>
            </form>
          </div>
        </section>

        <section class="cov-card">
          <h3 class="cov-form-title">{'Edit location' if edit_node else 'Add a location'}</h3>
          <form method="POST" class="stack">
            <input type="hidden" name="action" value="{'update_node' if edit_node else 'create_node'}" />
            <input type="hidden" name="scheme_id" value="{scheme_id or ''}" />
            {"<input type='hidden' name='node_id' value='" + str(edit_node.get('id')) + "' />" if edit_node else ""}
            <div class="row" style="gap:16px">
              <div style="flex:2">
                <label style="font-weight:800">Location name</label>
                <input name="node_name" placeholder="e.g., Lagos Mainland LGA" value="{html.escape(edit_node.get('name') if edit_node else '')}" />
              </div>
              <div style="flex:1">
                <label style="font-weight:800">Parent location</label>
                <select name="parent_id">
                  {parent_options}
                </select>
              </div>
            </div>
            <div class="row" style="gap:16px">
              <div style="flex:1">
                <label style="font-weight:800">GPS lat (optional)</label>
                <input name="gps_lat" placeholder="e.g., 6.5244" value="{edit_node.get('gps_lat') if edit_node else ''}" />
              </div>
              <div style="flex:1">
                <label style="font-weight:800">GPS lng (optional)</label>
                <input name="gps_lng" placeholder="e.g., 3.3792" value="{edit_node.get('gps_lng') if edit_node else ''}" />
              </div>
              <div style="flex:1">
                <label style="font-weight:800">Radius (m)</label>
                <input name="gps_radius_m" placeholder="e.g., 1500" value="{edit_node.get('gps_radius_m') if edit_node else ''}" />
              </div>
            </div>
            <div class="row" style="gap:8px">
              <button class="btn btn-primary" type="submit">{'Update location' if edit_node else 'Add location'}</button>
              {f"<a class='btn' href='{url_for('ui_project_coverage', project_id=project_id)}?scheme_id={scheme_id}{'&key=' + ADMIN_KEY if ADMIN_KEY else ''}'>Cancel</a>" if edit_node else ""}
            </div>
          </form>
        </section>

        <section class="cov-card">
          <div class="cov-tip"><b>Tip:</b> If a location should not be selectable, remove it or move it under the correct parent.</div>
          <table class="table">
            <thead>
              <tr>
                <th style="width:90px">ID</th>
                <th>Location</th>
                <th style="width:90px">Level</th>
                <th style="width:220px">Parent</th>
                <th style="width:180px">Created</th>
                <th style="width:200px">Actions</th>
              </tr>
            </thead>
            <tbody>
              {("".join(rows) if rows else "<tr><td colspan='6' class='muted' style='padding:18px'>No nodes yet.</td></tr>")}
            </tbody>
          </table>
        </section>
      </div>
    </div>
    """
    return ui_shell("Coverage", html_page, show_project_switcher=False)


@app.route("/ui/projects/<int:project_id>/enumerators", methods=["GET", "POST"])
def ui_project_enumerators(project_id):
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    project = prj.get_project(int(project_id))
    if not project:
        return ui_shell("Project not found", "<div class='card'><h2>Project not found</h2></div>"), 404

    msg = ""
    err = ""

    edit_id = request.args.get("edit_id") or ""
    edit_id = int(edit_id) if str(edit_id).isdigit() else None
    edit_enum = enum.get_enumerator(edit_id) if edit_id else None

    if request.method == "POST":
        try:
            if project_is_locked(project):
                raise ValueError("Archived projects are read-only.")
            action = (request.form.get("action") or "").strip()
            if action == "delete":
                enumerator_id = request.form.get("enumerator_id") or ""
                enumerator_id = int(enumerator_id) if str(enumerator_id).isdigit() else None
                if not enumerator_id:
                    raise ValueError("Missing enumerator to delete.")
                enum.delete_enumerator(enumerator_id)
                msg = "Enumerator deleted."
            elif action == "toggle":
                enumerator_id = request.form.get("enumerator_id") or ""
                enumerator_id = int(enumerator_id) if str(enumerator_id).isdigit() else None
                next_status = (request.form.get("next_status") or "").strip().upper()
                if not enumerator_id or next_status not in ("ACTIVE", "ARCHIVED"):
                    raise ValueError("Invalid status toggle.")
                enum.update_enumerator(enumerator_id, status=next_status)
                msg = "Enumerator status updated."
            else:
                name = (request.form.get("name") or "").strip()
                code = (request.form.get("code") or "").strip()
                phone = (request.form.get("phone") or "").strip()
                email = (request.form.get("email") or "").strip()
                enumerator_id = request.form.get("enumerator_id") or ""
                enumerator_id = int(enumerator_id) if str(enumerator_id).isdigit() else None
                if enumerator_id:
                    enum.update_enumerator(
                        enumerator_id,
                        name=name,
                        code=code,
                        phone=phone,
                        email=email,
                    )
                    msg = "Enumerator updated."
                else:
                    enum.create_enumerator(project_id, name, code=code, phone=phone, email=email)
                    msg = "Enumerator created."
        except Exception as e:
            err = str(e)

    enumerators = enum.list_enumerators(project_id=project_id, limit=200)
    sup_id = current_supervisor_id()
    if sup_id:
        try:
            assignments = enum.list_assignments(project_id=project_id, supervisor_id=int(sup_id), limit=1000)
            allowed_enum_ids = {int(a.get("enumerator_id")) for a in assignments if a.get("enumerator_id")}
            enumerators = [e for e in enumerators if int(e.get("id")) in allowed_enum_ids]
        except Exception:
            pass
    assignment_codes = {}
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT enumerator_id, code_full
                FROM enumerator_assignments
                WHERE project_id=?
                """,
                (int(project_id),),
            )
            assignment_codes = {int(r["enumerator_id"]): r["code_full"] for r in cur.fetchall() if r["code_full"]}
    except Exception:
        assignment_codes = {}

    rows = []
    for e in enumerators:
        status = (e.get("status") or "ACTIVE").upper()
        next_status = "ARCHIVED" if status == "ACTIVE" else "ACTIVE"
        code_display = assignment_codes.get(int(e.get("id"))) or e.get("code") or "—"
        name_safe = html.escape(e.get("name") or "")
        email_safe = html.escape(e.get("email") or "—")
        phone_safe = html.escape(e.get("phone") or "—")
        code_safe = html.escape(code_display)
        created_safe = html.escape(e.get("created_at") or "")
        rows.append(
            f"""
            <tr>
              <td><span class="template-id">#{e.get('id')}</span></td>
              <td>
                <div class="template-name">{name_safe}</div>
                <div class="template-desc">{email_safe}</div>
              </td>
              <td class="muted">
                {code_safe}
                {f"<button class='btn btn-sm enum-copy-btn' type='button' data-copy='{code_safe}'>Copy</button>" if code_display != '—' else ""}
              </td>
              <td class="muted">{phone_safe}</td>
              <td>
                <span class="status-badge {'status-active' if status == 'ACTIVE' else 'status-archived'}">
                  {('🟢 Active' if status == 'ACTIVE' else '🔴 Archived')}
                </span>
              </td>
              <td class="muted">{created_safe}</td>
              <td>
                <div class="action-buttons">
                  <a class="btn btn-sm" href="{url_for('ui_project_enumerators', project_id=project_id)}?edit_id={e.get('id')}{'&key=' + ADMIN_KEY if ADMIN_KEY else ''}">Edit</a>
                  <a class="btn btn-sm" href="{url_for('ui_project_assignments', project_id=project_id)}?enumerator_id={e.get('id')}{'&key=' + ADMIN_KEY if ADMIN_KEY else ''}">Assign</a>
                  <form method="POST" style="display:inline">
                    <input type="hidden" name="action" value="toggle" />
                    <input type="hidden" name="enumerator_id" value="{e.get('id')}" />
                    <input type="hidden" name="next_status" value="{next_status}" />
                    {"<input type='hidden' name='key' value='" + ADMIN_KEY + "' />" if ADMIN_KEY else ""}
                    <button class="btn btn-sm" type="submit">{'Archive' if status == 'ACTIVE' else 'Activate'}</button>
                  </form>
                  <form method="POST" style="display:inline">
                    <input type="hidden" name="action" value="delete" />
                    <input type="hidden" name="enumerator_id" value="{e.get('id')}" />
                    {"<input type='hidden' name='key' value='" + ADMIN_KEY + "' />" if ADMIN_KEY else ""}
                    <button class="btn btn-sm" type="submit">Delete</button>
                  </form>
                </div>
              </td>
            </tr>
            """
        )

    total_enumerators = len(enumerators)
    active_enumerators = sum(1 for e in enumerators if (e.get("status") or "ACTIVE").upper() == "ACTIVE")
    archived_enumerators = max(0, total_enumerators - active_enumerators)
    with_code_count = sum(1 for e in enumerators if (assignment_codes.get(int(e.get("id"))) or e.get("code")))

    html_page = f"""
    <style>
      .enum-page {{
        min-height: 100vh;
        background:
          radial-gradient(980px 430px at -8% -20%, rgba(124,58,237,.13), transparent 62%),
          radial-gradient(820px 340px at 108% -12%, rgba(139,92,246,.11), transparent 58%),
          linear-gradient(180deg, #f8f6ff 0%, #f3f4f8 100%);
        padding: 20px 0 34px;
      }}
      html[data-theme="dark"] .enum-page {{
        background:
          radial-gradient(980px 430px at -8% -20%, rgba(124,58,237,.25), transparent 62%),
          radial-gradient(820px 340px at 108% -12%, rgba(139,92,246,.2), transparent 58%),
          linear-gradient(180deg, #0f1221 0%, #11162a 100%);
      }}
      .enum-shell {{
        max-width: 1200px;
        margin: 0 auto;
        padding: 0 16px;
        display: grid;
        gap: 14px;
      }}
      .enum-hero {{
        border: 1px solid rgba(124,58,237,.28);
        border-radius: 22px;
        padding: 18px;
        background: linear-gradient(125deg, rgba(124,58,237,.18) 0%, rgba(255,255,255,.97) 46%, rgba(224,231,255,.62) 100%);
        box-shadow: 0 16px 36px rgba(15,18,34,.1);
      }}
      html[data-theme="dark"] .enum-hero {{
        background: linear-gradient(125deg, rgba(124,58,237,.38) 0%, rgba(21,24,44,.95) 46%, rgba(35,40,71,.9) 100%);
        border-color: rgba(167,139,250,.35);
      }}
      .enum-hero-row {{
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 14px;
      }}
      .enum-kicker {{
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 5px 10px;
        border-radius: 999px;
        background: rgba(124,58,237,.14);
        color: var(--primary);
        font-size: 11px;
        font-weight: 800;
        letter-spacing: .06em;
        text-transform: uppercase;
      }}
      .enum-title {{
        margin: 10px 0 4px;
        font-size: 30px;
        font-weight: 900;
        letter-spacing: -.02em;
        color: #111827;
      }}
      html[data-theme="dark"] .enum-title {{ color: #f8fafc; }}
      .enum-sub {{
        font-size: 14px;
        color: #475569;
        max-width: 760px;
      }}
      html[data-theme="dark"] .enum-sub {{ color: #cbd5e1; }}
      .enum-kpi-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
        gap: 12px;
      }}
      .enum-kpi-card {{
        border: 1px solid var(--border);
        background: var(--surface);
        border-radius: 16px;
        padding: 14px;
        box-shadow: 0 10px 24px rgba(15,18,34,.06);
      }}
      .enum-kpi-label {{ font-size: 12px; color: var(--muted); }}
      .enum-kpi-value {{ font-size: 24px; font-weight: 900; margin-top: 2px; }}
      .enum-card {{
        border: 1px solid var(--border);
        background: var(--surface);
        border-radius: 18px;
        padding: 16px;
        box-shadow: 0 12px 28px rgba(15,18,34,.06);
      }}
      .enum-tip {{
        border: 1px solid rgba(124,58,237,.22);
        background: linear-gradient(180deg, rgba(124,58,237,.08) 0%, rgba(124,58,237,.04) 100%);
        color: var(--text);
        border-radius: 12px;
        padding: 10px 12px;
        font-size: 13px;
        margin-bottom: 12px;
      }}
      .enum-form-grid {{
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 16px;
      }}
      .enum-form-grid.main {{
        grid-template-columns: 2fr 1fr;
      }}
      .enum-field label {{
        display: block;
        margin-bottom: 6px;
        font-size: 11px;
        font-weight: 800;
        letter-spacing: .06em;
        text-transform: uppercase;
        color: var(--muted);
      }}
      .enum-field input {{
        width: 100%;
        border: 1px solid rgba(124,58,237,.22);
        border-radius: 12px;
        background: linear-gradient(180deg, #ffffff 0%, #f7f8fc 100%);
        color: var(--text);
        padding: 11px 12px;
        font-size: 14px;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.85);
        transition: border-color .18s ease, box-shadow .18s ease, background .18s ease;
      }}
      html[data-theme="dark"] .enum-field input {{
        background: linear-gradient(180deg, rgba(30,41,59,.9) 0%, rgba(17,24,39,.92) 100%);
        border-color: rgba(167,139,250,.3);
        color: #e5e7eb;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.02);
      }}
      .enum-field input::placeholder {{
        color: #9aa4b2;
      }}
      .enum-field input:focus {{
        outline: none;
        border-color: rgba(124,58,237,.72);
        box-shadow: 0 0 0 4px rgba(124,58,237,.14);
      }}
      .enum-form-actions {{
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
        align-items: center;
      }}
      .enum-form-title {{
        margin: 0 0 10px;
        font-size: 16px;
        font-weight: 800;
      }}
      .enum-code-cell {{
        display: flex;
        flex-direction: column;
        gap: 6px;
      }}
      .enum-copy-btn {{
        width: fit-content;
      }}
      .enum-alert-success {{
        border: 1px solid rgba(34,197,94,.35);
        background: rgba(240,253,244,.85);
        color: #166534;
        border-radius: 14px;
        padding: 12px 14px;
        font-size: 13px;
      }}
      .enum-alert-error {{
        border: 1px solid rgba(239,68,68,.35);
        background: rgba(254,242,242,.88);
        color: #991b1b;
        border-radius: 14px;
        padding: 12px 14px;
        font-size: 13px;
      }}
      @media (max-width: 980px) {{
        .enum-hero-row {{ flex-direction: column; }}
        .enum-title {{ font-size: 26px; }}
        .enum-form-grid,
        .enum-form-grid.main {{ grid-template-columns: 1fr; }}
      }}
    </style>

    <div class="enum-page">
      <div class="enum-shell">
        <section class="enum-hero">
          <div class="enum-hero-row">
            <div>
              <div class="enum-kicker">Field workforce</div>
              <h1 class="enum-title">Enumerators — {html.escape(project.get('name') or 'Project')}</h1>
              <div class="enum-sub">Add and manage the people who collect data in the field. Keep their assignment codes organized for smooth operations.</div>
            </div>
            <a class="btn" href="{url_for('ui_project_detail', project_id=project_id)}{key_q}">Back to project</a>
          </div>
        </section>

        {"<div class='enum-alert-success'><b>Success:</b> " + html.escape(msg) + "</div>" if msg else ""}
        {"<div class='enum-alert-error'><b>Error:</b> " + html.escape(err) + "</div>" if err else ""}

        <section class="enum-kpi-grid">
          <div class="enum-kpi-card">
            <div class="enum-kpi-label">Total enumerators</div>
            <div class="enum-kpi-value">{total_enumerators}</div>
          </div>
          <div class="enum-kpi-card">
            <div class="enum-kpi-label">Active</div>
            <div class="enum-kpi-value">{active_enumerators}</div>
          </div>
          <div class="enum-kpi-card">
            <div class="enum-kpi-label">Archived</div>
            <div class="enum-kpi-value">{archived_enumerators}</div>
          </div>
          <div class="enum-kpi-card">
            <div class="enum-kpi-label">With assignment code</div>
            <div class="enum-kpi-value">{with_code_count}</div>
          </div>
        </section>

        <section class="enum-card">
          <div class="enum-tip"><b>Tip:</b> Use short, memorable codes for fast verification in the field.</div>
          <h3 class="enum-form-title">{'Edit enumerator' if edit_enum else 'Add an enumerator'}</h3>
          <form method="POST" class="stack">
            <input type="hidden" name="enumerator_id" value="{edit_enum.get('id') if edit_enum else ''}" />
            <div class="enum-form-grid main">
              <div class="enum-field">
                <label>Full name</label>
                <input name="name" placeholder="Enumerator full name" value="{html.escape(edit_enum.get('name') if edit_enum else '')}" />
              </div>
              <div class="enum-field">
                <label>Code</label>
                <input name="code" placeholder="e.g., LG-IKJ-01" value="{html.escape(edit_enum.get('code') if edit_enum else '')}" />
              </div>
            </div>
            <div class="enum-form-grid">
              <div class="enum-field">
                <label>Phone</label>
                <input name="phone" placeholder="Phone number" value="{html.escape(edit_enum.get('phone') if edit_enum else '')}" />
              </div>
              <div class="enum-field">
                <label>Email</label>
                <input name="email" placeholder="Email address" value="{html.escape(edit_enum.get('email') if edit_enum else '')}" />
              </div>
            </div>
            <div class="enum-form-actions">
              <button class="btn btn-primary" type="submit">{'Update enumerator' if edit_enum else 'Create enumerator'}</button>
              {f"<a class='btn' href='{url_for('ui_project_enumerators', project_id=project_id)}{key_q}'>Cancel</a>" if edit_enum else ""}
            </div>
          </form>
        </section>

        <section class="enum-card">
          <div class="enum-tip"><b>Tip:</b> Archive an enumerator when they are no longer active. Use Assign to map them into coverage and facilities.</div>
          <table class="table">
            <thead>
              <tr>
                <th style="width:90px">ID</th>
                <th>Enumerator</th>
                <th style="width:180px">Code</th>
                <th style="width:160px">Phone</th>
                <th style="width:160px">Status</th>
                <th style="width:180px">Created</th>
                <th style="width:230px">Actions</th>
              </tr>
            </thead>
            <tbody>
              {("".join(rows) if rows else "<tr><td colspan='7' class='muted' style='padding:18px'>No enumerators yet.</td></tr>")}
            </tbody>
          </table>
        </section>
      </div>
    </div>
    <script>
      (function(){{
        const buttons = Array.from(document.querySelectorAll("[data-copy]"));
        buttons.forEach(btn => {{
          btn.addEventListener("click", async () => {{
            const text = btn.getAttribute("data-copy") || "";
            if(!text) return;
            try{{
              await navigator.clipboard.writeText(text);
              const prev = btn.innerText;
              btn.innerText = "Copied";
              setTimeout(()=>btn.innerText=prev || "Copy", 1200);
            }}catch(e){{
              const prev = btn.innerText;
              btn.innerText = "Copy failed";
              setTimeout(()=>btn.innerText=prev || "Copy", 1200);
            }}
          }});
        }});
      }})();
    </script>
    """
    return ui_shell("Enumerators", html_page + (f"<script>setTimeout(function(){{window.location.href='/ui/org/users?project_id={project_id}{'&key=' + ADMIN_KEY if ADMIN_KEY else ''}';}}, 2200);</script>" if msg and "Enumerator" in msg else ""), show_project_switcher=False)


@app.route("/ui/projects/<int:project_id>/assignments", methods=["GET", "POST"])
def ui_project_assignments(project_id):
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    project = prj.get_project(int(project_id))
    if not project:
        return ui_shell("Project not found", "<div class='card'><h2>Project not found</h2></div>"), 404

    msg = ""
    err = ""

    scheme_id = request.values.get("scheme_id") or ""
    scheme_id = int(scheme_id) if str(scheme_id).isdigit() else None
    sup_id = request.args.get("supervisor_id") or ""
    sup_id = int(sup_id) if str(sup_id).isdigit() else None
    sup_ctx = prj.get_supervisor(int(sup_id)) if sup_id else None

    if request.method == "POST":
        try:
            if project_is_locked(project):
                raise ValueError("Archived projects are read-only.")
            action = (request.form.get("action") or "").strip()
            if action == "delete":
                assignment_id = request.form.get("assignment_id") or ""
                assignment_id = int(assignment_id) if str(assignment_id).isdigit() else None
                if not assignment_id:
                    raise ValueError("Missing assignment to delete.")
                enum.delete_assignment(assignment_id)
                msg = "Assignment deleted."
            elif action == "assign_supervisor_coverage":
                supervisor_id = request.form.get("supervisor_id") or ""
                supervisor_id = int(supervisor_id) if str(supervisor_id).isdigit() else None
                cov_list = request.form.getlist("supervisor_coverage_node_ids") or []
                cov_ids = [int(cid) for cid in cov_list if str(cid).isdigit()]
                if not supervisor_id:
                    raise ValueError("Missing supervisor.")
                with get_conn() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "DELETE FROM supervisor_coverage_nodes WHERE supervisor_id=? AND project_id=?",
                        (int(supervisor_id), int(project_id)),
                    )
                    for cid in cov_ids:
                        cur.execute(
                            "INSERT OR IGNORE INTO supervisor_coverage_nodes (supervisor_id, project_id, coverage_node_id, created_at) VALUES (?, ?, ?, ?)",
                            (int(supervisor_id), int(project_id), int(cid), now_iso()),
                        )
                    conn.commit()
                msg = "Supervisor coverage updated."
            else:
                enumerator_id = request.form.get("enumerator_id") or ""
                coverage_node_id = request.form.get("coverage_node_id") or ""
                coverage_node_ids = request.form.getlist("coverage_node_ids") or []
                template_id = request.form.get("template_id") or ""
                supervisor_id = request.form.get("supervisor_id") or ""
                target_facilities_count = request.form.get("target_facilities_count") or ""
                target_facilities_count = int(target_facilities_count) if str(target_facilities_count).isdigit() else None
                facility_names = (request.form.get("facility_names") or "").strip()
                if not enumerator_id:
                    raise ValueError("Select an enumerator.")
                supervisor_id = int(supervisor_id) if str(supervisor_id).isdigit() else None
                if sup_id:
                    supervisor_id = sup_id
                assignment_id = enum.assign_enumerator(
                    project_id,
                    int(enumerator_id),
                    coverage_node_id=(int(coverage_node_id) if str(coverage_node_id).isdigit() else None),
                    template_id=(int(template_id) if str(template_id).isdigit() else None),
                    target_facilities_count=target_facilities_count,
                    scheme_id=(int(scheme_id) if str(scheme_id).isdigit() else None),
                    supervisor_id=supervisor_id,
                )
                # Apply multi-coverage nodes for enumerator assignment (optional)
                cov_ids = [int(cid) for cid in coverage_node_ids if str(cid).isdigit()]
                if cov_ids:
                    with get_conn() as conn:
                        cur = conn.cursor()
                        cur.execute("DELETE FROM assignment_coverage_nodes WHERE assignment_id=?", (int(assignment_id),))
                        for cid in cov_ids:
                            cur.execute(
                                "INSERT OR IGNORE INTO assignment_coverage_nodes (assignment_id, coverage_node_id, created_at) VALUES (?, ?, ?)",
                                (int(assignment_id), int(cid), now_iso()),
                            )
                        # Set primary coverage_node_id as first selected
                        cur.execute(
                            "UPDATE enumerator_assignments SET coverage_node_id=? WHERE id=?",
                            (int(cov_ids[0]), int(assignment_id)),
                        )
                        conn.commit()
                try:
                    prj.ensure_assignment_code(int(project_id), int(enumerator_id), int(assignment_id))
                except Exception:
                    pass
                if facility_names:
                    names = [n.strip() for n in facility_names.replace(",", "\n").splitlines() if n.strip()]
                    existing = enum.list_assignment_facilities(int(assignment_id))
                    existing_ids = {int(f.get("facility_id")) for f in existing if f.get("facility_id")}
                    for name in names:
                        fid = get_or_create_facility_by_name(name)
                        if int(fid) in existing_ids:
                            continue
                        enum.add_assignment_facility(int(assignment_id), int(fid))
                        existing_ids.add(int(fid))
                msg = "Assignment created."
        except Exception as e:
            err = str(e)

    enumerators = enum.list_enumerators(project_id=project_id, limit=500)
    pre_enum_id = request.args.get("enumerator_id") or ""
    pre_enum_id = int(pre_enum_id) if str(pre_enum_id).isdigit() else None
    templates = prj.list_project_templates(project_id)
    schemes = cov.list_schemes(200)
    if scheme_id is None:
        scheme_id = schemes[0]["id"] if schemes else None
    nodes = cov.list_nodes(int(scheme_id), limit=2000) if scheme_id else []
    node_map = {n["id"]: n for n in nodes}

    assignments = enum.list_assignments(project_id=project_id, limit=500, supervisor_id=sup_id)
    assignment_code_map = {}
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, code_full
                FROM enumerator_assignments
                WHERE project_id=?
                """,
                (int(project_id),),
            )
            assignment_code_map = {int(r["id"]): r["code_full"] for r in cur.fetchall() if r["code_full"]}
    except Exception:
        assignment_code_map = {}

    enum_opts = "".join(
        [
            f"<option value='{e['id']}' {'selected' if pre_enum_id and int(e['id']) == int(pre_enum_id) else ''}>{e['name']}</option>"
            for e in enumerators
        ]
    )
    tpl_opts = "<option value=''>Any template</option>" + "".join(
        [f"<option value='{t['id']}'>{t['name']}</option>" for t in templates]
    )
    node_opts = "<option value=''>Any coverage node</option>" + "".join(
        [f"<option value='{n['id']}'>{n['name']}</option>" for n in nodes]
    )
    scheme_opts = "".join(
        [
            f"<option value='{s['id']}' {'selected' if scheme_id == s['id'] else ''}>{s['name']}</option>"
            for s in schemes
        ]
    )
    scheme_key_q = f"&key={ADMIN_KEY}" if ADMIN_KEY else ""

    enum_map = {e["id"]: e for e in enumerators}
    tpl_map = {t["id"]: t for t in templates}
    org_id = project.get("organization_id") or current_org_id()
    supervisors = prj.list_supervisors(organization_id=int(org_id) if org_id else None, limit=200)
    supervisor_map = {int(s.get("id")): s for s in supervisors}
    supervisor_opts = "<option value=''>Unassigned</option>" + "".join(
        [f"<option value='{s['id']}' {'selected' if sup_id and int(s['id']) == int(sup_id) else ''}>{s['full_name']}</option>" for s in supervisors]
    )
    sup_cov_ids = set()
    if sup_id:
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT coverage_node_id FROM supervisor_coverage_nodes WHERE supervisor_id=? AND project_id=?",
                    (int(sup_id), int(project_id)),
                )
                sup_cov_ids = {int(r["coverage_node_id"]) for r in cur.fetchall() if r["coverage_node_id"]}
        except Exception:
            sup_cov_ids = set()

    rows = []
    total_done = 0
    total_target = 0
    templated_count = 0
    supervised_count = 0
    coded_count = 0
    coverage_count = 0
    for a in assignments:
        assignment_id = int(a.get("id") or 0)
        enum_id = int(a.get("enumerator_id") or 0) if a.get("enumerator_id") else None
        if assignment_id and enum_id and not assignment_code_map.get(assignment_id):
            try:
                gen = prj.ensure_assignment_code(int(project_id), int(enum_id), int(assignment_id))
                if gen and gen.get("code_full"):
                    assignment_code_map[int(assignment_id)] = gen.get("code_full")
            except Exception:
                pass
        e = enum_map.get(a.get("enumerator_id"), {})
        n = node_map.get(a.get("coverage_node_id"), {})
        t = tpl_map.get(a.get("template_id"), {})
        code_full = assignment_code_map.get(int(a.get("id") or 0)) or "—"
        sup_name = ""
        if a.get("supervisor_id"):
            sup_name = (supervisor_map.get(int(a.get("supervisor_id"))) or {}).get("full_name") or ""
            supervised_count += 1
        if a.get("template_id"):
            templated_count += 1
        if a.get("coverage_node_id"):
            coverage_count += 1
        if code_full != "—":
            coded_count += 1
        fac_list = []
        try:
            fac_list = enum.list_assignment_facilities(int(a.get("id")))
        except Exception:
            fac_list = []
        done_count = len([f for f in fac_list if (f.get("status") or "").upper() == "DONE"])
        total_count = len(fac_list)
        target_count = a.get("target_facilities_count") or (total_count if total_count else None)
        target_for_total = int(target_count) if str(target_count).isdigit() else total_count
        total_done += done_count
        total_target += target_for_total
        share_link = ""
        if a.get("template_id"):
            token = ensure_share_token(int(a.get("template_id")))
            if project_id:
                share_path = url_for("fill_form_project", project_id=int(project_id), token=token)
            else:
                share_path = url_for("fill_form", token=token)
            share_link = f"{share_path}?assign_id={a.get('id')}"

        e_name = html.escape(e.get("name") or "—")
        n_name = html.escape(n.get("name") or "—")
        t_name = html.escape(t.get("name") or "—")
        sup_name_safe = html.escape(sup_name or "—")
        code_safe = html.escape(code_full)
        created_safe = html.escape(a.get("created_at") or "")
        progress_display = f"{done_count}/{target_count if target_count is not None else total_count}"

        rows.append(
            f"""
            <tr>
              <td><span class="template-id">#{a.get('id')}</span></td>
              <td>{e_name}</td>
              <td class="muted">{code_safe}</td>
              <td class="muted">{sup_name_safe}</td>
              <td>{n_name}</td>
              <td>{t_name}</td>
              <td class="muted">{progress_display}</td>
              <td class="muted">{created_safe}</td>
              <td>
                <div class="assign-actions">
                  {f"<a class='btn btn-sm' href='{share_link}'>Share</a>" if share_link else "<span class='muted'>—</span>"}
                  {f"<button class='btn btn-sm' type='button' data-copy='{code_safe}'>Copy</button>" if code_full != '—' else ""}
                  {f"<a class='btn btn-sm' href='{url_for('ui_assignment_qr', assignment_id=a.get('id'))}{key_q}'>QR</a>" if code_full != '—' else ""}
                  <a class='btn btn-sm' href='{url_for('ui_assignment_facilities', project_id=project_id, assignment_id=a.get('id'))}{key_q}'>Facilities</a>
                  <form method="POST" style="display:inline">
                    <input type="hidden" name="action" value="delete" />
                    <input type="hidden" name="assignment_id" value="{a.get('id')}" />
                    <input type="hidden" name="scheme_id" value="{scheme_id or ''}" />
                    {"<input type='hidden' name='key' value='" + ADMIN_KEY + "' />" if ADMIN_KEY else ""}
                    <button class="btn btn-sm" type="submit">Delete</button>
                  </form>
                </div>
              </td>
            </tr>
            """
        )

    sup_cov_options = "".join(
        [
            f"<option value='{n['id']}' {'selected' if int(n['id']) in sup_cov_ids else ''}>{html.escape(n['name'])}</option>"
            for n in nodes
        ]
    )
    overall_progress = f"{total_done}/{total_target}" if total_target > 0 else "0/0"

    html_page = f"""
    <style>
      .assign-page {{
        min-height: 100vh;
        background:
          radial-gradient(980px 430px at -8% -20%, rgba(124,58,237,.13), transparent 62%),
          radial-gradient(820px 340px at 108% -12%, rgba(139,92,246,.11), transparent 58%),
          linear-gradient(180deg, #f8f6ff 0%, #f3f4f8 100%);
        padding: 20px 0 34px;
      }}
      html[data-theme="dark"] .assign-page {{
        background:
          radial-gradient(980px 430px at -8% -20%, rgba(124,58,237,.25), transparent 62%),
          radial-gradient(820px 340px at 108% -12%, rgba(139,92,246,.2), transparent 58%),
          linear-gradient(180deg, #0f1221 0%, #11162a 100%);
      }}
      .assign-shell {{
        max-width: 1200px;
        margin: 0 auto;
        padding: 0 16px;
        display: grid;
        gap: 14px;
      }}
      .assign-hero {{
        border: 1px solid rgba(124,58,237,.28);
        border-radius: 22px;
        padding: 18px;
        background: linear-gradient(125deg, rgba(124,58,237,.18) 0%, rgba(255,255,255,.97) 46%, rgba(224,231,255,.62) 100%);
        box-shadow: 0 16px 36px rgba(15,18,34,.1);
      }}
      html[data-theme="dark"] .assign-hero {{
        background: linear-gradient(125deg, rgba(124,58,237,.38) 0%, rgba(21,24,44,.95) 46%, rgba(35,40,71,.9) 100%);
        border-color: rgba(167,139,250,.35);
      }}
      .assign-hero-row {{
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 14px;
      }}
      .assign-kicker {{
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 5px 10px;
        border-radius: 999px;
        background: rgba(124,58,237,.14);
        color: var(--primary);
        font-size: 11px;
        font-weight: 800;
        letter-spacing: .06em;
        text-transform: uppercase;
      }}
      .assign-title {{
        margin: 10px 0 4px;
        font-size: 30px;
        font-weight: 900;
        letter-spacing: -.02em;
        color: #111827;
      }}
      html[data-theme="dark"] .assign-title {{ color: #f8fafc; }}
      .assign-desc {{
        font-size: 14px;
        color: #475569;
        max-width: 760px;
      }}
      html[data-theme="dark"] .assign-desc {{ color: #cbd5e1; }}
      .assign-card {{
        border: 1px solid var(--border);
        background: var(--surface);
        border-radius: 18px;
        padding: 16px;
        box-shadow: 0 12px 28px rgba(15,18,34,.06);
      }}
      .assign-kpi-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
        gap: 12px;
      }}
      .assign-kpi-card {{
        border: 1px solid var(--border);
        background: var(--surface);
        border-radius: 16px;
        padding: 14px;
        box-shadow: 0 10px 24px rgba(15,18,34,.06);
      }}
      .assign-kpi-label {{ font-size: 12px; color: var(--muted); }}
      .assign-kpi-value {{ font-size: 24px; font-weight: 900; margin-top: 2px; }}
      .assign-tip {{
        border: 1px solid rgba(124,58,237,.22);
        background: linear-gradient(180deg, rgba(124,58,237,.08) 0%, rgba(124,58,237,.04) 100%);
        color: var(--text);
        border-radius: 12px;
        padding: 10px 12px;
        font-size: 13px;
        margin-bottom: 12px;
      }}
      .assign-grid-3 {{
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 12px;
      }}
      .assign-grid-2 {{
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
      }}
      .assign-field label {{
        display: block;
        margin-bottom: 6px;
        font-size: 11px;
        font-weight: 800;
        letter-spacing: .06em;
        text-transform: uppercase;
        color: var(--muted);
      }}
      .assign-field select,
      .assign-field input:not([type="hidden"]),
      .assign-field textarea {{
        width: 100%;
        border: 1px solid rgba(124,58,237,.24);
        border-radius: 12px;
        background: linear-gradient(180deg, #ffffff 0%, #f7f7fb 100%);
        color: var(--text);
        padding: 11px 12px;
        font-size: 14px;
        transition: border-color .18s ease, box-shadow .18s ease, background .18s ease;
      }}
      html[data-theme="dark"] .assign-field select,
      html[data-theme="dark"] .assign-field input:not([type="hidden"]),
      html[data-theme="dark"] .assign-field textarea {{
        background: linear-gradient(180deg, rgba(30,41,59,.92) 0%, rgba(17,24,39,.92) 100%);
        border-color: rgba(167,139,250,.3);
        color: #e5e7eb;
      }}
      .assign-field input:not([type="hidden"])::placeholder,
      .assign-field textarea::placeholder {{
        color: #9aa4b2;
      }}
      .assign-field select:focus,
      .assign-field input:not([type="hidden"]):focus,
      .assign-field textarea:focus {{
        outline: none;
        border-color: rgba(124,58,237,.72);
        box-shadow: 0 0 0 4px rgba(124,58,237,.14);
      }}
      .assign-field input:not([type="hidden"]):disabled {{
        opacity: .88;
        cursor: not-allowed;
      }}
      .assign-alert-success {{
        border: 1px solid rgba(34,197,94,.35);
        background: rgba(240,253,244,.85);
        color: #166534;
        border-radius: 14px;
        padding: 12px 14px;
        font-size: 13px;
      }}
      .assign-alert-error {{
        border: 1px solid rgba(239,68,68,.35);
        background: rgba(254,242,242,.88);
        color: #991b1b;
        border-radius: 14px;
        padding: 12px 14px;
        font-size: 13px;
      }}
      .assign-actions {{
        display: flex;
        flex-wrap: wrap;
        gap: 6px;
        align-items: center;
      }}
      @media (max-width: 980px) {{
        .assign-hero-row {{ flex-direction: column; }}
        .assign-title {{ font-size: 26px; }}
        .assign-grid-3,
        .assign-grid-2 {{ grid-template-columns: 1fr; }}
      }}
    </style>

    <div class="assign-page">
      <div class="assign-shell">
        <section class="assign-hero">
          <div class="assign-hero-row">
            <div>
              <div class="assign-kicker">Assignment ops</div>
              <h1 class="assign-title">Assignments — {html.escape(project.get('name') or 'Project')}</h1>
              <div class="assign-desc">Map enumerators to templates, coverage nodes, and supervisors. Share links instantly and track delivery in one place.</div>
            </div>
            <div class="row" style="gap:10px; align-items:center;">
              <a class="btn" href="{url_for('ui_project_detail', project_id=project_id)}{key_q}">Back to project</a>
              <a class="btn btn-primary" href="{url_for('ui_project_enumerators', project_id=project_id)}{key_q}">Manage team</a>
            </div>
          </div>
        </section>

        {"<div class='assign-alert-success'><b>Success:</b> " + html.escape(msg) + "</div>" if msg else ""}
        {"<div class='assign-alert-error'><b>Error:</b> " + html.escape(err) + "</div>" if err else ""}

        <section class="assign-kpi-grid">
          <div class="assign-kpi-card">
            <div class="assign-kpi-label">Total assignments</div>
            <div class="assign-kpi-value">{len(assignments)}</div>
          </div>
          <div class="assign-kpi-card">
            <div class="assign-kpi-label">With supervisor</div>
            <div class="assign-kpi-value">{supervised_count}</div>
          </div>
          <div class="assign-kpi-card">
            <div class="assign-kpi-label">With template</div>
            <div class="assign-kpi-value">{templated_count}</div>
          </div>
          <div class="assign-kpi-card">
            <div class="assign-kpi-label">With assignment code</div>
            <div class="assign-kpi-value">{coded_count}</div>
          </div>
          <div class="assign-kpi-card">
            <div class="assign-kpi-label">Coverage linked</div>
            <div class="assign-kpi-value">{coverage_count}</div>
          </div>
          <div class="assign-kpi-card">
            <div class="assign-kpi-label">Facility progress</div>
            <div class="assign-kpi-value">{overall_progress}</div>
          </div>
        </section>

        <section class="assign-card">
          <div class="assign-tip"><b>Tip:</b> Leave Template empty to allow the enumerator to use any form in this project.</div>
          <h3 style="margin:0 0 12px">Create assignment</h3>
          <form method="POST" class="stack">
            <div class="assign-grid-3">
              <div class="assign-field">
                <label>Enumerator</label>
                <select name="enumerator_id">
                  {enum_opts}
                </select>
              </div>
              <div class="assign-field">
                <label>Supervisor</label>
                {(
                  f"<input value='{html.escape((sup_ctx.get('full_name') or '').strip())}' disabled /><input type='hidden' name='supervisor_id' value='{sup_id}' />"
                  if (sup_id and sup_ctx)
                  else f"<select name='supervisor_id'>{supervisor_opts}</select>"
                )}
              </div>
              <div class="assign-field">
                <label>Template</label>
                <select name="template_id">
                  {tpl_opts}
                </select>
              </div>
            </div>
            <div class="assign-grid-2">
              <div class="assign-field">
                <label>Coverage scheme</label>
                <select name="scheme_id_picker" onchange="window.location='?scheme_id=' + this.value + '{scheme_key_q}'">
                  {scheme_opts}
                </select>
              </div>
              <div class="assign-field">
                <label>Primary coverage node</label>
                <select name="coverage_node_id">
                  {node_opts}
                </select>
              </div>
            </div>
            <div class="assign-grid-2">
              <div class="assign-field">
                <label>Coverage nodes (multi-select)</label>
                <select name="coverage_node_ids" multiple size="5">
                  {node_opts}
                </select>
                <div class="muted" style="margin-top:6px">Optional: choose multiple LGAs/areas. The first selected becomes primary.</div>
              </div>
              <div class="assign-field">
                <label>Target facilities</label>
                <input name="target_facilities_count" type="number" min="0" placeholder="e.g., 8" />
                <div class="muted" style="margin-top:6px">Optional. Used for progress and delivery tracking.</div>
              </div>
            </div>
            <div class="assign-field">
              <label>Facility list (optional)</label>
              <textarea name="facility_names" rows="4" placeholder="Add facility names (one per line or comma-separated)"></textarea>
              <div class="muted" style="margin-top:6px">You can also assign facilities later from the Facilities button in the table.</div>
            </div>
            <input type="hidden" name="scheme_id" value="{scheme_id or ''}" />
            <button class="btn btn-primary" type="submit">Assign</button>
          </form>
        </section>

        {(
          f"""
          <section class="assign-card">
            <h3 style="margin:0 0 8px">Supervisor coverage</h3>
            <div class="assign-tip" style="margin-bottom:10px">Assign this supervisor to one or more coverage nodes so their scope is explicit.</div>
            <form method="POST" class="stack">
              <input type="hidden" name="action" value="assign_supervisor_coverage" />
              <input type="hidden" name="supervisor_id" value="{sup_id}" />
              <div class="assign-field">
                <label>Coverage nodes</label>
                <select name="supervisor_coverage_node_ids" multiple size="8">
                  {sup_cov_options}
                </select>
                <div class="muted" style="margin-top:6px">Hold Cmd/Ctrl to select multiple LGAs.</div>
              </div>
              <button class="btn btn-primary" type="submit">Save supervisor coverage</button>
            </form>
          </section>
          """
          if sup_id else ""
        )}

        <section class="assign-card">
          <div class="assign-tip"><b>Tip:</b> Use <b>Share</b> to send the exact assignment link, then copy code or QR for field onboarding.</div>
          <table class="table">
            <thead>
              <tr>
                <th style="width:90px">ID</th>
                <th>Enumerator</th>
                <th style="width:200px">Assignment code</th>
                <th style="width:160px">Supervisor</th>
                <th>Coverage node</th>
                <th>Template</th>
                <th style="width:140px">Progress</th>
                <th style="width:180px">Created</th>
                <th style="width:240px">Actions</th>
              </tr>
            </thead>
            <tbody>
              {("".join(rows) if rows else "<tr><td colspan='9' class='muted' style='padding:18px'>No assignments yet.</td></tr>")}
            </tbody>
          </table>
        </section>
      </div>
    </div>
    <script>
      (function(){{
        const buttons = Array.from(document.querySelectorAll("[data-copy]"));
        buttons.forEach(btn => {{
          btn.addEventListener("click", async () => {{
            const text = btn.getAttribute("data-copy") || "";
            if(!text) return;
            try{{
              await navigator.clipboard.writeText(text);
              const prev = btn.innerText;
              btn.innerText = "Copied";
              setTimeout(()=>btn.innerText=prev || "Copy", 1200);
            }}catch(e){{
              const prev = btn.innerText;
              btn.innerText = "Copy failed";
              setTimeout(()=>btn.innerText=prev || "Copy", 1200);
            }}
          }});
        }});
      }})();
    </script>
    """
    return ui_shell("Assignments", html_page, show_project_switcher=False)


@app.route("/ui/projects/<int:project_id>/assignments/<int:assignment_id>/facilities", methods=["GET", "POST"])
def ui_assignment_facilities(project_id, assignment_id):
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    project = prj.get_project(int(project_id))
    if not project:
        return ui_shell("Project not found", "<div class='card'><h2>Project not found</h2></div>"), 404

    assignment = enum.get_assignment(int(assignment_id))
    if not assignment or (assignment.get("project_id") and int(assignment.get("project_id")) != int(project_id)):
        return ui_shell("Assignment not found", "<div class='card'><h2>Assignment not found</h2></div>"), 404

    msg = ""
    err = ""
    enumerator = enum.get_enumerator(int(assignment.get("enumerator_id"))) if assignment.get("enumerator_id") else None
    template = tpl.get_template_config(int(assignment.get("template_id"))) if assignment.get("template_id") else None

    if request.method == "POST":
        try:
            if project_is_locked(project):
                raise ValueError("Archived projects are read-only.")
            action = (request.form.get("action") or "").strip()
            if action == "delete":
                af_id = request.form.get("assignment_facility_id") or ""
                af_id = int(af_id) if str(af_id).isdigit() else None
                if not af_id:
                    raise ValueError("Missing facility to remove.")
                enum.delete_assignment_facility(af_id)
                msg = "Facility removed."
            elif action == "target":
                target = request.form.get("target_facilities_count") or ""
                target_val = int(target) if str(target).isdigit() else None
                enum.update_assignment_target(int(assignment_id), target_val)
                msg = "Target updated."
            else:
                raw = (request.form.get("facility_names") or "").strip()
                if not raw:
                    raise ValueError("Add at least one facility name.")
                names = [n.strip() for n in raw.replace(",", "\n").splitlines() if n.strip()]
                existing = enum.list_assignment_facilities(int(assignment_id))
                existing_ids = {int(f.get("facility_id")) for f in existing if f.get("facility_id")}
                added = 0
                for name in names:
                    fid = get_or_create_facility_by_name(name)
                    if int(fid) in existing_ids:
                        continue
                    enum.add_assignment_facility(int(assignment_id), int(fid))
                    existing_ids.add(int(fid))
                    added += 1
                msg = f"Added {added} facilities." if added else "No new facilities added."
        except Exception as e:
            err = str(e)

    facilities = enum.list_assignment_facilities(int(assignment_id))
    done_count = len([f for f in facilities if (f.get("status") or "").upper() == "DONE"])
    total_count = len(facilities)
    target = assignment.get("target_facilities_count") or ""

    rows = []
    for f in facilities:
        status = (f.get("status") or "PENDING").upper()
        rows.append(
            f"""
            <tr>
              <td><span class="template-id">#{f.get('id')}</span></td>
              <td>{f.get('facility_name') or '—'}</td>
              <td>
                <span class="status-badge {'status-active' if status == 'DONE' else 'status-draft'}">
                  {('✅ Done' if status == 'DONE' else '⏳ Pending')}
                </span>
              </td>
              <td class="muted">{f.get('done_survey_id') or '—'}</td>
              <td class="muted">{f.get('created_at') or ''}</td>
              <td>
                <form method="POST" style="display:inline">
                  <input type="hidden" name="action" value="delete" />
                  <input type="hidden" name="assignment_facility_id" value="{f.get('id')}" />
                  {"<input type='hidden' name='key' value='" + ADMIN_KEY + "' />" if ADMIN_KEY else ""}
                  <button class="btn btn-sm" type="submit">Remove</button>
                </form>
              </td>
            </tr>
            """
        )

    html = f"""
    <div class="card">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div>
          <h1 class="h1">Assignment Facilities</h1>
          <div class="muted">{enumerator.get('name') if enumerator else 'Enumerator'} · {template.get('name') if template else 'Any template'}</div>
        </div>
        <a class="btn" href="{url_for('ui_project_assignments', project_id=project_id)}{key_q}">Back to assignments</a>
      </div>
    </div>

    {"<div class='card' style='border-color: rgba(46, 204, 113, .35)'><b>Success:</b> " + msg + "</div>" if msg else ""}
    {"<div class='card' style='border-color: rgba(231, 76, 60, .35)'><b>Error:</b> " + err + "</div>" if err else ""}

    <div class="card" style="margin-top:16px">
      <div class="row" style="gap:16px; align-items:flex-end;">
        <form method="POST" class="stack" style="flex:2">
          <input type="hidden" name="action" value="add" />
          <label style="font-weight:800">Add facilities</label>
          <textarea name="facility_names" rows="4" placeholder="Add facility names (one per line or comma-separated)"></textarea>
          <button class="btn btn-primary" type="submit">Add to assignment</button>
        </form>
        <form method="POST" class="stack" style="flex:1">
          <input type="hidden" name="action" value="target" />
          <label style="font-weight:800">Target count</label>
          <input name="target_facilities_count" type="number" min="0" value="{target}" placeholder="e.g., 8" />
          <button class="btn" type="submit">Update target</button>
        </form>
      </div>
      <div class="muted" style="margin-top:8px">Progress: {done_count}/{target if target != '' else total_count}</div>
    </div>

    <div class="card" style="margin-top:16px">
      <table class="table">
        <thead>
          <tr>
            <th style="width:90px">ID</th>
            <th>Facility</th>
            <th style="width:160px">Status</th>
            <th style="width:160px">Survey ID</th>
            <th style="width:180px">Added</th>
            <th style="width:140px">Actions</th>
          </tr>
        </thead>
        <tbody>
          {("".join(rows) if rows else "<tr><td colspan='6' class='muted' style='padding:18px'>No facilities assigned yet.</td></tr>")}
        </tbody>
      </table>
    </div>
    """
    return ui_shell("Assignment Facilities", html, show_project_switcher=False)


@app.route("/ui/projects/<int:project_id>/interviews", methods=["GET", "POST"])
def ui_project_interviews(project_id):
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    project = prj.get_project(int(project_id))
    if not project:
        return ui_shell("Project not found", "<div class='card'><h2>Project not found</h2></div>"), 404

    msg = ""
    err = ""

    transcribe_ok, transcribe_reason = transcription_config_status()

    if request.method == "POST":
        try:
            action = (request.form.get("action") or "").strip()
            interview_id = request.form.get("interview_id") or ""
            interview_id = int(interview_id) if str(interview_id).isdigit() else None
            if action == "transcribe":
                if not transcribe_ok:
                    raise ValueError(transcribe_reason)
                if not interview_id:
                    raise ValueError("Missing interview.")
                with get_conn() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT id, project_id, audio_file_url FROM qualitative_interviews WHERE id=? LIMIT 1",
                        (int(interview_id),),
                    )
                    iv = cur.fetchone()
                    if not iv:
                        raise ValueError("Interview not found.")
                    if int(iv["project_id"] or 0) != int(project_id):
                        raise ValueError("Interview does not belong to this project.")
                    audio_url = (iv["audio_file_url"] or "").strip()
                    if not audio_url:
                        raise ValueError("No audio file found for this interview.")
                    conn.execute(
                        "UPDATE qualitative_interviews SET transcript_status='PENDING', updated_at=? WHERE id=?",
                        (now_iso(), int(interview_id)),
                    )
                    conn.commit()
                transcript_text = run_audio_transcription(audio_url)
                with get_conn() as conn:
                    conn.execute(
                        """
                        UPDATE qualitative_interviews
                        SET transcript_text=?, transcript_status='COMPLETED', updated_at=?
                        WHERE id=?
                        """,
                        (transcript_text, now_iso(), int(interview_id)),
                    )
                    conn.commit()
                msg = "Transcription completed."
        except Exception as e:
            if interview_id:
                try:
                    with get_conn() as conn:
                        conn.execute(
                            "UPDATE qualitative_interviews SET transcript_status='NONE', updated_at=? WHERE id=?",
                            (now_iso(), int(interview_id)),
                        )
                        conn.commit()
                except Exception:
                    pass
            err = str(e)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT qi.*, e.name AS enumerator_name, e.full_name AS enumerator_full_name
            FROM qualitative_interviews qi
            LEFT JOIN enumerators e ON e.id = qi.enumerator_id
            WHERE qi.project_id=?
            ORDER BY qi.id DESC
            """,
            (int(project_id),),
        )
        interviews = [dict(r) for r in cur.fetchall()]

    rows = []
    for r in interviews:
        enum_name = r.get("enumerator_name") or r.get("enumerator_full_name") or "—"
        has_audio = bool((r.get("audio_file_url") or "").strip())
        t_status = (r.get("transcript_status") or "NONE").strip().upper()
        can_transcribe = has_audio and t_status != "PENDING"
        transcribe_title = ""
        if not has_audio:
            transcribe_title = "Attach audio first"
        elif t_status == "PENDING":
            transcribe_title = "Transcription in progress"
        rows.append(
            f"""
            <tr>
              <td><span class="template-id">#{r.get('id')}</span></td>
              <td>{html.escape(enum_name)}</td>
              <td class="muted">{html.escape(r.get('interview_mode') or 'TEXT')}</td>
              <td>{'Yes' if int(r.get('consent_obtained') or 0) == 1 else 'No'}</td>
              <td class="muted">{html.escape(t_status)}</td>
              <td class="muted">{html.escape(r.get('created_at') or '')}</td>
              <td>
                <a class="btn btn-sm" href="{url_for('ui_interview_view', interview_id=r.get('id'))}{key_q}">View</a>
                <form method="POST" style="display:inline">
                  <input type="hidden" name="action" value="transcribe" />
                  <input type="hidden" name="interview_id" value="{r.get('id')}" />
                  <button class="btn btn-sm" type="submit" {'disabled' if not can_transcribe else ''} {f'title=\"{html.escape(transcribe_title)}\"' if transcribe_title else ''} {'style=\"opacity:.6;cursor:not-allowed\"' if not can_transcribe else ''}>{'Transcribing…' if t_status == 'PENDING' else 'Transcribe'}</button>
                </form>
              </td>
            </tr>
            """
        )

    html_page = f"""
    <div class="card">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div>
          <h1 class="h1">Interviews — {html.escape(project.get('name') or 'Project')}</h1>
          <div class="muted">Qualitative interviews with consent-first controls.</div>
        </div>
        <div class="row" style="gap:8px;">
          <a class="btn" href="{url_for('ui_project_detail', project_id=project_id)}{key_q}">Back to project</a>
          <a class="btn btn-primary" href="/ui/interviews/new?project_id={project_id}{'&key=' + ADMIN_KEY if ADMIN_KEY else ''}">New interview</a>
        </div>
      </div>
    </div>

    {"<div class='card' style='border-color: rgba(46, 204, 113, .35)'><b>Success:</b> " + msg + "</div>" if msg else ""}
    {"<div class='card' style='border-color: rgba(231, 76, 60, .35)'><b>Error:</b> " + err + "</div>" if err else ""}

    <div class="card" style="margin-top:16px">
      {f"<div class='muted' style='margin-bottom:10px'><b>Transcription:</b> {html.escape(transcribe_reason)}</div>" if not transcribe_ok else ""}
      <table class="table">
        <thead>
          <tr>
            <th style="width:90px">ID</th>
            <th>Enumerator</th>
            <th style="width:140px">Mode</th>
            <th style="width:120px">Consent</th>
            <th style="width:140px">Transcript</th>
            <th style="width:180px">Created</th>
            <th style="width:220px">Actions</th>
          </tr>
        </thead>
        <tbody>
          {("".join(rows) if rows else "<tr><td colspan='7' class='muted' style='padding:18px'>No interviews yet.</td></tr>")}
        </tbody>
      </table>
    </div>
    """
    return ui_shell("Interviews", html_page, show_project_switcher=False)


@app.route("/ui/interviews/new", methods=["GET", "POST"])
def ui_interview_new():
    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    msg = ""
    err = ""
    project_id = request.args.get("project_id") or request.form.get("project_id") or ""
    project_id = int(project_id) if str(project_id).isdigit() else None

    if request.method == "POST":
        try:
            mode = (request.form.get("interview_mode") or "TEXT").strip().upper()
            consent_val = (request.form.get("consent_obtained") or "").strip().upper()
            audio_allowed = (request.form.get("audio_recording_allowed") or "").strip().upper()
            interview_text = (request.form.get("interview_text") or "").strip()
            audio_data_url = (request.form.get("audio_data_url") or "").strip()
            audio_file_upload = request.files.get("audio_file_import")
            enumerator_code = (request.form.get("enumerator_code") or "").strip()

            if consent_val not in ("YES", "NO"):
                raise ValueError("Consent is required before submission.")
            consent_obtained = 1 if consent_val == "YES" else 0

            if audio_allowed not in ("YES", "NO"):
                audio_allowed = "NO"
            audio_allowed_val = 1 if audio_allowed == "YES" else 0
            if mode not in ("TEXT", "AUDIO"):
                mode = "TEXT"
            if mode == "TEXT":
                audio_allowed_val = 0

            enumerator_id = None
            assignment_id = None

            if enumerator_code:
                ctx = prj.validate_enumerator_code(enumerator_code)
                if not ctx.get("ok"):
                    raise ValueError(ctx.get("error") or "Invalid enumerator code.")
                project_id = int(ctx.get("project_id"))
                enumerator_id = int(ctx.get("enumerator_id"))
                assignment_id = int(ctx.get("assignment_id"))

            if not project_id:
                raise ValueError("Project is required.")

            audio_file_url = None
            audio_confirmed = 0
            if audio_file_upload and (audio_file_upload.filename or "").strip():
                raw_name = (audio_file_upload.filename or "").strip()
                safe_name = secure_filename(raw_name) or f"audio_{uuid.uuid4().hex[:8]}"
                root, ext = os.path.splitext(safe_name)
                ext = ext.lower()
                if not ext:
                    ext = _guess_audio_ext_from_mime(audio_file_upload.mimetype)
                if not ext:
                    ext = ".webm"
                ts = datetime.now().strftime("%Y%m%d%H%M%S")
                save_name = f"interview_new_{project_id}_{ts}_{uuid.uuid4().hex[:8]}{ext}"
                save_path = os.path.join(UPLOAD_DIR, save_name)
                audio_file_upload.save(save_path)
                try:
                    if os.path.getsize(save_path) <= 0:
                        raise ValueError("Uploaded audio file is empty.")
                except Exception:
                    try:
                        os.remove(save_path)
                    except Exception:
                        pass
                    raise ValueError("Uploaded audio file is invalid.")
                audio_file_url = f"/uploads/{save_name}"
                audio_confirmed = 1
                audio_allowed_val = 1

            if (not audio_file_url) and audio_data_url:
                if not audio_data_url.startswith("data:audio/") or ";base64," not in audio_data_url:
                    raise ValueError("Invalid recorded audio payload.")
                header, raw_b64 = audio_data_url.split(",", 1)
                if ";base64" not in header:
                    raise ValueError("Invalid recorded audio payload.")
                mime = (header[5:].split(";", 1)[0] or "").lower()
                raw_b64 = re.sub(r"\s+", "", raw_b64 or "")
                try:
                    audio_bytes = base64.b64decode(raw_b64, validate=True)
                except Exception:
                    raise ValueError("Could not decode recorded audio.")
                if not audio_bytes:
                    raise ValueError("Recorded audio is empty.")
                ext_map = {
                    "audio/webm": "webm",
                    "audio/wav": "wav",
                    "audio/x-wav": "wav",
                    "audio/mpeg": "mp3",
                    "audio/mp4": "m4a",
                    "audio/ogg": "ogg",
                }
                ext = ext_map.get(mime, "webm")
                ts = datetime.now().strftime("%Y%m%d%H%M%S")
                save_name = f"interview_new_{project_id}_{ts}_{uuid.uuid4().hex[:8]}.{ext}"
                save_path = os.path.join(UPLOAD_DIR, save_name)
                with open(save_path, "wb") as f:
                    f.write(audio_bytes)
                audio_file_url = f"/uploads/{save_name}"
                audio_confirmed = 1
                audio_allowed_val = 1

            if mode == "AUDIO" and audio_allowed_val == 1 and not audio_file_url:
                raise ValueError("Record audio before submitting, or switch to note-taking mode.")
            if mode == "TEXT" and not interview_text:
                raise ValueError("Interview notes are required in note-taking mode.")

            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO qualitative_interviews
                      (project_id, enumerator_id, assignment_id, interview_mode,
                       interview_text, consent_obtained, consent_timestamp, audio_recording_allowed,
                       audio_confirmed, audio_file_url, transcript_status, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'NONE', ?, ?)
                    """,
                    (
                        int(project_id),
                        int(enumerator_id) if enumerator_id else None,
                        int(assignment_id) if assignment_id else None,
                        mode,
                        interview_text or None,
                        int(consent_obtained),
                        now_iso(),
                        int(audio_allowed_val),
                        int(audio_confirmed),
                        audio_file_url,
                        now_iso(),
                        now_iso(),
                    ),
                )
                conn.commit()
            msg = "Interview submitted."
        except Exception as e:
            err = str(e)

    back_link = (
        f"<a class='intv-btn intv-btn-ghost' href='{url_for('ui_project_interviews', project_id=project_id)}{key_q}'>Back to interviews</a>"
        if project_id else
        f"<a class='intv-btn intv-btn-ghost' href='/ui/projects{key_q}'>Back to projects</a>"
    )
    success_block = (
        "<div class='intv-alert intv-alert-success'><strong>Success:</strong> " + html.escape(msg) + "</div>"
        if msg else ""
    )
    error_block = (
        "<div class='intv-alert intv-alert-error'><strong>Error:</strong> " + html.escape(err) + "</div>"
        if err else ""
    )
    pref_mode = ((request.form.get("interview_mode") or "TEXT").strip().upper() if request.method == "POST" else "TEXT")
    if pref_mode not in ("TEXT", "AUDIO"):
        pref_mode = "TEXT"
    pref_consent = ((request.form.get("consent_obtained") or "").strip().upper() if request.method == "POST" else "")
    if pref_consent not in ("YES", "NO"):
        pref_consent = ""
    pref_audio_allowed = ((request.form.get("audio_recording_allowed") or "").strip().upper() if request.method == "POST" else "NO")
    if pref_audio_allowed not in ("YES", "NO"):
        pref_audio_allowed = "NO"
    pref_enum = (request.form.get("enumerator_code") or "").strip() if request.method == "POST" else ""
    pref_notes = (request.form.get("interview_text") or "").strip() if request.method == "POST" else ""
    pref_audio_data = (request.form.get("audio_data_url") or "").strip() if request.method == "POST" else ""

    html_page = f"""
    <style>
      .intv-page {{
        min-height: 100vh;
        background:
          radial-gradient(900px 430px at -5% -10%, rgba(124,58,237,.12), transparent 62%),
          radial-gradient(760px 360px at 110% 0%, rgba(99,102,241,.10), transparent 58%),
          linear-gradient(180deg, #f7f5ff 0%, #f3f4f9 100%);
        padding: 26px 0 36px;
      }}
      html[data-theme="dark"] .intv-page {{
        background:
          radial-gradient(900px 430px at -5% -10%, rgba(124,58,237,.24), transparent 62%),
          radial-gradient(760px 360px at 110% 0%, rgba(99,102,241,.18), transparent 58%),
          linear-gradient(180deg, #0f1221 0%, #121529 100%);
      }}
      .intv-shell {{
        max-width: 1080px;
        margin: 0 auto;
        padding: 0 16px;
        display: grid;
        gap: 14px;
      }}
      .intv-hero {{
        border: 1px solid rgba(124,58,237,.3);
        border-radius: 22px;
        background: linear-gradient(136deg, rgba(124,58,237,.15) 0%, rgba(255,255,255,.96) 46%, rgba(221,214,254,.58) 100%);
        box-shadow: 0 18px 40px rgba(15,18,34,.1);
        padding: 20px;
        position: relative;
        overflow: hidden;
      }}
      .intv-hero::after {{
        content: "";
        position: absolute;
        right: -120px;
        top: -130px;
        width: 320px;
        height: 320px;
        border-radius: 50%;
        background: radial-gradient(circle, rgba(167,139,250,.38), transparent 64%);
      }}
      html[data-theme="dark"] .intv-hero {{
        background: linear-gradient(136deg, rgba(124,58,237,.35) 0%, rgba(18,21,44,.95) 46%, rgba(44,49,80,.9) 100%);
        border-color: rgba(167,139,250,.33);
      }}
      .intv-hero-row {{
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 12px;
        position: relative;
        z-index: 2;
      }}
      .intv-kicker {{
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 5px 10px;
        border-radius: 999px;
        background: rgba(124,58,237,.14);
        color: var(--primary);
        font-size: 11px;
        font-weight: 800;
        letter-spacing: .07em;
        text-transform: uppercase;
      }}
      .intv-title {{
        margin: 10px 0 6px;
        font-size: 31px;
        font-weight: 900;
        letter-spacing: -.02em;
        color: #111827;
      }}
      html[data-theme="dark"] .intv-title {{
        color: #f8fafc;
      }}
      .intv-sub {{
        color: #475569;
        font-size: 14px;
        max-width: 620px;
      }}
      html[data-theme="dark"] .intv-sub {{
        color: #cbd5e1;
      }}
      .intv-badges {{
        margin-top: 12px;
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
      }}
      .intv-pill {{
        display: inline-flex;
        align-items: center;
        gap: 6px;
        border-radius: 999px;
        padding: 6px 10px;
        font-size: 11px;
        font-weight: 800;
        letter-spacing: .04em;
        text-transform: uppercase;
        color: #4c1d95;
        background: rgba(124,58,237,.15);
        border: 1px solid rgba(124,58,237,.2);
      }}
      html[data-theme="dark"] .intv-pill {{
        color: #e9d5ff;
        border-color: rgba(167,139,250,.35);
      }}
      .intv-btn {{
        border-radius: 12px;
        padding: 10px 14px;
        font-size: 13px;
        font-weight: 700;
        text-decoration: none;
        display: inline-flex;
        align-items: center;
        gap: 8px;
      }}
      .intv-btn-ghost {{
        border: 1px solid var(--border);
        background: var(--surface);
        color: var(--text);
      }}
      .intv-btn-primary {{
        border: 1px solid transparent;
        background: linear-gradient(90deg, var(--primary), #8b5cf6);
        color: #fff;
        box-shadow: 0 10px 20px rgba(124,58,237,.25);
      }}
      .intv-form-card {{
        border: 1px solid var(--border);
        border-radius: 22px;
        background: var(--surface);
        box-shadow: 0 14px 34px rgba(15,18,34,.09);
        padding: 18px;
      }}
      .intv-form {{
        display: grid;
        gap: 14px;
      }}
      .intv-grid {{
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 12px;
      }}
      .intv-field label {{
        display: block;
        margin-bottom: 6px;
        font-size: 11px;
        letter-spacing: .06em;
        text-transform: uppercase;
        font-weight: 800;
        color: var(--muted);
      }}
      .intv-field input,
      .intv-field select,
      .intv-field textarea {{
        width: 100%;
        border: 1px solid var(--border);
        border-radius: 12px;
        background: var(--surface-2);
        color: var(--text);
        padding: 11px 12px;
        font-size: 14px;
      }}
      .intv-field textarea {{
        min-height: 180px;
        resize: vertical;
      }}
      .intv-field input:focus,
      .intv-field select:focus,
      .intv-field textarea:focus {{
        outline: none;
        border-color: var(--primary);
        box-shadow: 0 0 0 3px rgba(124,58,237,.14);
      }}
      .intv-help {{
        margin-top: 5px;
        font-size: 12px;
        color: var(--muted);
      }}
      .intv-mode-panel {{
        border: 1px solid var(--border);
        border-radius: 16px;
        background: var(--surface-2);
        padding: 14px;
      }}
      .intv-audio-grid {{
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
      }}
      .intv-audio-card {{
        border: 1px solid var(--border);
        border-radius: 14px;
        background: var(--surface);
        padding: 12px;
      }}
      .intv-audio-card-title {{
        margin: 0 0 4px;
        font-size: 13px;
        font-weight: 800;
        letter-spacing: .02em;
        color: var(--text);
      }}
      .intv-audio-card-sub {{
        margin: 0 0 10px;
        font-size: 12px;
        color: var(--muted);
      }}
      .intv-mode-panel.hidden {{
        display: none;
      }}
      .intv-rec-head {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 10px;
        margin-bottom: 10px;
      }}
      .intv-rec-title {{
        font-size: 14px;
        font-weight: 800;
        color: var(--text);
      }}
      .intv-rec-state {{
        font-size: 12px;
        font-weight: 700;
        color: var(--muted);
      }}
      .intv-rec-controls {{
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-bottom: 10px;
      }}
      .intv-rec-btn {{
        border-radius: 10px;
        border: 1px solid var(--border);
        background: var(--surface);
        color: var(--text);
        padding: 8px 10px;
        font-size: 12px;
        font-weight: 700;
        cursor: pointer;
      }}
      .intv-rec-btn.primary {{
        background: linear-gradient(90deg, var(--primary), #8b5cf6);
        color: #fff;
        border-color: transparent;
      }}
      .intv-rec-btn:disabled {{
        opacity: .45;
        cursor: not-allowed;
      }}
      .intv-rec-preview {{
        width: 100%;
        margin-top: 4px;
      }}
      .intv-rec-timer {{
        font-size: 12px;
        font-weight: 700;
        color: var(--muted);
      }}
      .intv-rec-error {{
        margin-top: 8px;
        font-size: 12px;
        color: #b91c1c;
      }}
      .intv-dropzone {{
        border: 1px dashed rgba(124,58,237,.45);
        border-radius: 12px;
        background: rgba(124,58,237,.06);
        padding: 12px;
        cursor: pointer;
      }}
      .intv-dropzone.drag {{
        border-color: rgba(124,58,237,.85);
        background: rgba(124,58,237,.12);
      }}
      .intv-drop-main {{
        font-size: 13px;
        font-weight: 700;
        color: var(--text);
      }}
      .intv-drop-main span {{
        color: var(--primary);
      }}
      .intv-drop-sub {{
        margin-top: 4px;
        font-size: 11px;
        color: var(--muted);
      }}
      .intv-upload-progress {{
        margin-top: 6px;
        height: 8px;
        border-radius: 999px;
        border: 1px solid var(--border);
        background: var(--surface);
        overflow: hidden;
      }}
      .intv-upload-bar {{
        width: 0%;
        height: 100%;
        background: linear-gradient(90deg, var(--primary), #8b5cf6);
        transition: width .18s ease;
      }}
      .intv-file-input {{
        position: absolute !important;
        width: 1px;
        height: 1px;
        padding: 0;
        margin: -1px;
        overflow: hidden;
        clip: rect(0, 0, 0, 0);
        white-space: nowrap;
        border: 0;
      }}
      .intv-footer {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 10px;
        border-top: 1px solid var(--border);
        padding-top: 12px;
      }}
      .intv-note {{
        font-size: 12px;
        color: var(--muted);
      }}
      .intv-alert {{
        border-radius: 14px;
        padding: 12px 14px;
        border: 1px solid var(--border);
        background: var(--surface);
        font-size: 13px;
      }}
      .intv-alert-success {{
        border-color: rgba(34,197,94,.4);
        background: rgba(240,253,244,.82);
        color: #166534;
      }}
      .intv-alert-error {{
        border-color: rgba(239,68,68,.35);
        background: rgba(254,242,242,.88);
        color: #991b1b;
      }}
      .hidden {{
        display: none !important;
      }}
      @media (max-width: 980px) {{
        .intv-grid {{
          grid-template-columns: 1fr;
        }}
        .intv-audio-grid {{
          grid-template-columns: 1fr;
        }}
      }}
      @media (max-width: 760px) {{
        .intv-page {{ padding-top: 16px; }}
        .intv-title {{ font-size: 26px; }}
        .intv-hero-row {{ flex-direction: column; }}
      }}
    </style>
    <div class="intv-page">
      <div class="intv-shell">
        <section class="intv-hero">
          <div class="intv-hero-row">
            <div>
              <div class="intv-kicker">Qualitative interview</div>
              <h1 class="intv-title">New Interview</h1>
              <div class="intv-sub">Capture qualitative responses with explicit consent and clear ethics controls before submission.</div>
              <div class="intv-badges">
                <span class="intv-pill">Consent-first</span>
                <span class="intv-pill">Supervisor review ready</span>
                {f"<span class='intv-pill'>Project #{project_id}</span>" if project_id else ""}
              </div>
            </div>
            <div>{back_link}</div>
          </div>
        </section>

        {success_block}
        {error_block}

        <section class="intv-form-card">
            <form method="POST" class="intv-form" id="interviewForm" data-initial-mode="{pref_mode}" enctype="multipart/form-data">
            <input type="hidden" name="project_id" value="{project_id or ''}" />
            <input type="hidden" name="audio_data_url" id="audio_data_url" value="{html.escape(pref_audio_data)}" />

            <div class="intv-field">
              <label for="enumerator_code">Enumerator code (recommended)</label>
              <input id="enumerator_code" name="enumerator_code" placeholder="Enter enumerator code" autocomplete="off" value="{html.escape(pref_enum)}" />
              <div class="intv-help">If valid, the interview is automatically bound to the assigned enumerator and project.</div>
            </div>

            <div class="intv-grid">
              <div class="intv-field">
                <label for="interview_mode">Interview mode</label>
                <select id="interview_mode" name="interview_mode">
                  <option value="TEXT" {"selected" if pref_mode == "TEXT" else ""}>Text</option>
                  <option value="AUDIO" {"selected" if pref_mode == "AUDIO" else ""}>Audio (if enabled)</option>
                </select>
              </div>
              <div class="intv-field">
                <label for="consent_obtained">Consent obtained</label>
                <select id="consent_obtained" name="consent_obtained" required>
                  <option value="" {"selected" if pref_consent == "" else ""}>Select…</option>
                  <option value="YES" {"selected" if pref_consent == "YES" else ""}>Yes</option>
                  <option value="NO" {"selected" if pref_consent == "NO" else ""}>No</option>
                </select>
              </div>
              <div class="intv-field" id="audioAllowedField">
                <label for="audio_recording_allowed">Audio recording allowed</label>
                <select id="audio_recording_allowed" name="audio_recording_allowed">
                  <option value="NO" {"selected" if pref_audio_allowed == "NO" else ""}>No</option>
                  <option value="YES" {"selected" if pref_audio_allowed == "YES" else ""}>Yes</option>
                </select>
              </div>
            </div>

            <div class="intv-mode-panel" id="noteModePanel">
              <div class="intv-field" style="margin:0">
                <label for="interview_text">Interview notes</label>
                <textarea id="interview_text" name="interview_text" rows="8" placeholder="Type interview notes here...">{html.escape(pref_notes)}</textarea>
              </div>
            </div>

            <div class="intv-mode-panel hidden" id="audioModePanel">
              <div class="intv-rec-head">
                <div>
                  <div class="intv-rec-title">Audio capture</div>
                  <div class="intv-help">Choose one method: record now or import an existing audio file.</div>
                </div>
                <div class="intv-rec-state" id="audioStatusText">Idle</div>
              </div>
              <div class="intv-audio-grid">
                <div class="intv-audio-card">
                  <p class="intv-audio-card-title">Option 1: Start recording</p>
                  <p class="intv-audio-card-sub">Use device microphone to record this interview live.</p>
                  <div class="intv-rec-controls">
                    <button class="intv-rec-btn primary" type="button" id="audioStartBtn">Start recording</button>
                    <button class="intv-rec-btn" type="button" id="audioPauseBtn" disabled>Pause</button>
                    <button class="intv-rec-btn" type="button" id="audioResumeBtn" disabled>Resume</button>
                    <button class="intv-rec-btn" type="button" id="audioStopBtn" disabled>Stop</button>
                    <button class="intv-rec-btn" type="button" id="audioClearBtn" disabled>Clear</button>
                    <span class="intv-rec-timer" id="audioTimer">00:00</span>
                  </div>
                </div>
                <div class="intv-audio-card">
                  <p class="intv-audio-card-title">Option 2: Import audio file</p>
                  <p class="intv-audio-card-sub">Upload any existing recording for automatic transcription.</p>
                  <div id="audioDropZone" class="intv-dropzone" tabindex="0">
                    <div class="intv-drop-main">Drop audio file here or <span>browse from device</span></div>
                    <div class="intv-drop-sub">MP3, WAV, M4A, AAC, FLAC, OGG, WEBM, MP4, MOV, AMR, 3GP, AIFF, WMA</div>
                  </div>
                  <input
                    class="intv-file-input"
                    id="audio_file_import"
                    name="audio_file_import"
                    type="file"
                    accept="audio/*,.mp3,.wav,.m4a,.aac,.flac,.ogg,.oga,.opus,.webm,.mp4,.mov,.amr,.3gp,.aiff,.aif,.wma"
                  />
                  <div class="intv-help" id="audioUploadName">No file selected.</div>
                  <div class="intv-upload-progress hidden" id="audioUploadProgress">
                    <div class="intv-upload-bar" id="audioUploadBar"></div>
                  </div>
                  <div class="intv-help hidden" id="audioUploadPct"></div>
                </div>
              </div>
              <audio id="audioPreview" class="intv-rec-preview hidden" controls></audio>
              <div class="intv-rec-error hidden" id="audioErrorText"></div>
              <div class="intv-help">Recorder or imported file can be used. Imported file is used if both are present.</div>
            </div>

            <div class="intv-footer">
              <div class="intv-note">Submission is blocked until consent is explicitly captured.</div>
              <button class="intv-btn intv-btn-primary" type="submit">Submit interview</button>
            </div>
          </form>
        </section>
      </div>
    </div>
    <script src="/static/interview_new.js"></script>
    <script src="/static/interview_upload.js"></script>
    """
    return ui_shell("New interview", html_page, show_project_switcher=False)


@app.route("/ui/interviews/<int:interview_id>", methods=["GET", "POST"])
def ui_interview_view(interview_id):
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    msg = ""
    err = ""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT qi.*, p.name AS project_name
            FROM qualitative_interviews qi
            LEFT JOIN projects p ON p.id = qi.project_id
            WHERE qi.id=?
            LIMIT 1
            """,
            (int(interview_id),),
        )
        row = cur.fetchone()
    if not row:
        return ui_shell("Interview not found", "<div class='card'><h2>Interview not found</h2></div>"), 404
    interview = dict(row)
    transcribe_ok, transcribe_reason = transcription_config_status()

    if request.method == "POST":
        try:
            action = (request.form.get("action") or "save_transcript").strip().lower()
            user = getattr(g, "user", None)
            user_id = int(user.get("id")) if user and user.get("id") else None
            if action == "transcribe":
                if not transcribe_ok:
                    raise ValueError(transcribe_reason)
                audio_url = (interview.get("audio_file_url") or "").strip()
                if not audio_url:
                    raise ValueError("No audio file found for this interview.")
                with get_conn() as conn:
                    conn.execute(
                        "UPDATE qualitative_interviews SET transcript_status='PENDING', updated_at=? WHERE id=?",
                        (now_iso(), int(interview_id)),
                    )
                    conn.commit()
                transcript_text = run_audio_transcription(audio_url)
                with get_conn() as conn:
                    conn.execute(
                        """
                        UPDATE qualitative_interviews
                        SET transcript_text=?, transcript_status='COMPLETED', transcript_approved_by=?, transcript_approved_at=?, updated_at=?
                        WHERE id=?
                        """,
                        (transcript_text, user_id, now_iso(), now_iso(), int(interview_id)),
                    )
                    conn.commit()
                msg = "Audio transcribed successfully."
            else:
                transcript_text = (request.form.get("transcript_text") or "").strip()
                with get_conn() as conn:
                    conn.execute(
                        """
                        UPDATE qualitative_interviews
                        SET transcript_text=?, transcript_status='COMPLETED', transcript_approved_by=?, transcript_approved_at=?, updated_at=?
                        WHERE id=?
                        """,
                        (transcript_text, user_id, now_iso(), now_iso(), int(interview_id)),
                    )
                    conn.commit()
                msg = "Transcript saved."
        except Exception as e:
            if (request.form.get("action") or "").strip().lower() == "transcribe":
                try:
                    with get_conn() as conn:
                        conn.execute(
                            "UPDATE qualitative_interviews SET transcript_status='NONE', updated_at=? WHERE id=?",
                            (now_iso(), int(interview_id)),
                        )
                        conn.commit()
                except Exception:
                    pass
            err = str(e)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT qi.*, p.name AS project_name
            FROM qualitative_interviews qi
            LEFT JOIN projects p ON p.id = qi.project_id
            WHERE qi.id=?
            LIMIT 1
            """,
            (int(interview_id),),
        )
        row2 = cur.fetchone()
        if row2:
            interview = dict(row2)

    project_id = int(interview.get("project_id") or 0) if interview.get("project_id") else None
    back_href = (
        f"{url_for('ui_project_interviews', project_id=project_id)}{key_q}"
        if project_id else f"/ui/projects{key_q}"
    )
    mode_text = (interview.get("interview_mode") or "TEXT").upper()
    consent_text = "Yes" if int(interview.get("consent_obtained") or 0) == 1 else "No"
    audio_allowed_text = "Yes" if int(interview.get("audio_recording_allowed") or 0) == 1 else "No"
    transcript_status = html.escape(interview.get("transcript_status") or "NONE")
    audio_url = (interview.get("audio_file_url") or "").strip()
    audio_url_safe = html.escape(audio_url)
    can_transcribe_audio = bool(audio_url and (interview.get("transcript_status") or "").upper() != "PENDING")
    transcribe_title = ""
    if not audio_url:
        transcribe_title = "No audio file attached"
    elif (interview.get("transcript_status") or "").upper() == "PENDING":
        transcribe_title = "Transcription in progress"
    success_block = (
        "<div class='intv-view-alert intv-view-alert-success'><b>Success:</b> " + html.escape(msg) + "</div>"
        if msg else ""
    )
    error_block = (
        "<div class='intv-view-alert intv-view-alert-error'><b>Error:</b> " + html.escape(err) + "</div>"
        if err else ""
    )
    upload_action = url_for("ui_interview_audio", interview_id=int(interview_id))
    upload_form = f"""
      <form method="POST" action="{upload_action}" enctype="multipart/form-data" class="intv-view-upload" id="intvAudioUploadForm" data-ajax-upload="1">
        <input type="hidden" name="confirm_audio_upload" value="YES" />
        <div id="intvAudioDropZone" class="intv-view-dropzone" tabindex="0">
          <div class="intv-view-drop-main">Drop audio file here or <span>browse</span></div>
          <div class="intv-view-drop-sub">Supports common audio formats including MP3, WAV, M4A, AAC, FLAC, OGG, WEBM, MP4, MOV.</div>
        </div>
        <input id="intvAudioUploadInput" type="file" name="audio" accept="audio/*,.mp3,.wav,.m4a,.aac,.flac,.ogg,.oga,.opus,.webm,.mp4,.mov,.amr,.3gp,.aiff,.aif,.wma" required />
        <div class="intv-view-upload-progress hidden" id="intvAudioUploadProgress"><div class="intv-view-upload-bar" id="intvAudioUploadBar"></div></div>
        <div class="intv-view-muted" id="intvAudioUploadText">No file selected.</div>
        <button class="intv-view-btn intv-view-btn-soft" type="submit" id="intvAudioUploadBtn">Import audio file</button>
      </form>
      <div class="intv-view-muted">Upload replaces existing audio for this interview.</div>
    """
    audio_block = (
        f"""
        <div class="intv-view-audio-player">
          <audio controls preload="metadata" src="{audio_url_safe}" style="width:100%"></audio>
          <div class="intv-view-audio-actions">
            <a class="intv-view-btn intv-view-btn-soft" href="{audio_url_safe}" target="_blank" rel="noopener">Open audio</a>
            <a class="intv-view-btn intv-view-btn-soft" href="{audio_url_safe}" download>Download</a>
            <form method="POST" style="display:inline">
              <input type="hidden" name="action" value="transcribe" />
              <button class="intv-view-btn intv-view-btn-soft" type="submit" {'disabled' if not can_transcribe_audio else ''} {f'title=\"{html.escape(transcribe_title)}\"' if transcribe_title else ''} {'style=\"opacity:.6;cursor:not-allowed\"' if not can_transcribe_audio else ''}>Transcribe audio</button>
            </form>
          </div>
          {upload_form}
        </div>
        """
        if audio_url else
        f"<div class='intv-view-muted'>No audio file attached to this interview yet.</div>{upload_form}"
    )

    html_page = f"""
    <style>
      .intv-view-page {{
        min-height:100vh;
        background:
          radial-gradient(900px 430px at -5% -10%, rgba(124,58,237,.12), transparent 62%),
          radial-gradient(760px 360px at 110% 0%, rgba(99,102,241,.10), transparent 58%),
          linear-gradient(180deg, #f7f5ff 0%, #f3f4f9 100%);
        padding:26px 0 36px;
      }}
      html[data-theme="dark"] .intv-view-page {{
        background:
          radial-gradient(900px 430px at -5% -10%, rgba(124,58,237,.24), transparent 62%),
          radial-gradient(760px 360px at 110% 0%, rgba(99,102,241,.18), transparent 58%),
          linear-gradient(180deg, #0f1221 0%, #121529 100%);
      }}
      .intv-view-shell {{
        max-width:1080px;
        margin:0 auto;
        padding:0 16px;
        display:grid;
        gap:14px;
      }}
      .intv-view-hero {{
        border:1px solid rgba(124,58,237,.3);
        border-radius:22px;
        background:linear-gradient(136deg, rgba(124,58,237,.15) 0%, rgba(255,255,255,.96) 46%, rgba(221,214,254,.58) 100%);
        box-shadow:0 18px 40px rgba(15,18,34,.1);
        padding:20px;
        display:flex;
        justify-content:space-between;
        align-items:flex-start;
        gap:12px;
      }}
      html[data-theme="dark"] .intv-view-hero {{
        background:linear-gradient(136deg, rgba(124,58,237,.35) 0%, rgba(18,21,44,.95) 46%, rgba(44,49,80,.9) 100%);
        border-color:rgba(167,139,250,.33);
      }}
      .intv-view-kicker {{
        display:inline-flex;
        padding:5px 10px;
        border-radius:999px;
        background:rgba(124,58,237,.14);
        color:var(--primary);
        font-size:11px;
        font-weight:800;
        letter-spacing:.07em;
        text-transform:uppercase;
      }}
      .intv-view-title {{
        margin:10px 0 4px;
        font-size:30px;
        font-weight:900;
        letter-spacing:-.02em;
        color:#111827;
      }}
      html[data-theme="dark"] .intv-view-title {{ color:#f8fafc; }}
      .intv-view-sub {{
        font-size:14px;
        color:#475569;
      }}
      html[data-theme="dark"] .intv-view-sub {{ color:#cbd5e1; }}
      .intv-view-btn {{
        border-radius:12px;
        padding:10px 14px;
        font-size:13px;
        font-weight:700;
        text-decoration:none;
        display:inline-flex;
        align-items:center;
        gap:8px;
        border:1px solid var(--border);
        background:var(--surface);
        color:var(--text);
      }}
      .intv-view-btn-soft {{
        border-radius:10px;
        padding:7px 10px;
        font-size:12px;
      }}
      .intv-view-alert {{
        border-radius:14px;
        padding:12px 14px;
        border:1px solid var(--border);
        background:var(--surface);
        font-size:13px;
      }}
      .intv-view-alert-success {{
        border-color:rgba(34,197,94,.4);
        background:rgba(240,253,244,.82);
        color:#166534;
      }}
      .intv-view-alert-error {{
        border-color:rgba(239,68,68,.35);
        background:rgba(254,242,242,.88);
        color:#991b1b;
      }}
      .intv-view-grid {{
        display:grid;
        grid-template-columns:repeat(2, minmax(0, 1fr));
        gap:14px;
      }}
      .intv-view-card {{
        border:1px solid var(--border);
        border-radius:18px;
        background:var(--surface);
        box-shadow:0 14px 34px rgba(15,18,34,.09);
        padding:16px;
      }}
      .intv-view-h3 {{
        margin:0 0 10px;
        font-size:16px;
        font-weight:800;
        color:var(--text);
      }}
      .intv-view-meta {{
        display:grid;
        gap:8px;
        font-size:13px;
      }}
      .intv-view-muted {{
        color:var(--muted);
        font-size:12px;
      }}
      .intv-view-audio-player {{
        display:grid;
        gap:10px;
      }}
      .intv-view-audio-actions {{
        display:flex;
        flex-wrap:wrap;
        gap:8px;
      }}
      .intv-view-upload {{
        display:grid;
        gap:8px;
      }}
      .intv-view-dropzone {{
        border:1px dashed rgba(124,58,237,.45);
        border-radius:12px;
        background: rgba(124,58,237,.06);
        padding:11px 12px;
        cursor:pointer;
      }}
      .intv-view-dropzone.drag {{
        border-color: rgba(124,58,237,.85);
        background: rgba(124,58,237,.12);
      }}
      .intv-view-drop-main {{
        font-size:13px;
        font-weight:700;
        color:var(--text);
      }}
      .intv-view-drop-main span {{
        color:var(--primary);
      }}
      .intv-view-drop-sub {{
        margin-top:4px;
        font-size:11px;
        color:var(--muted);
      }}
      .intv-view-upload input[type="file"] {{
        width:100%;
        border:1px solid var(--border);
        border-radius:12px;
        background:var(--surface-2);
        color:var(--text);
        padding:10px 12px;
        font-size:13px;
      }}
      .intv-view-upload-progress {{
        height:8px;
        border-radius:999px;
        border:1px solid var(--border);
        background:var(--surface);
        overflow:hidden;
      }}
      .intv-view-upload-bar {{
        width:0%;
        height:100%;
        background:linear-gradient(90deg, var(--primary), #8b5cf6);
        transition:width .18s ease;
      }}
      .intv-view-text {{
        white-space:pre-wrap;
        font-size:14px;
        line-height:1.45;
        color:var(--text);
      }}
      .intv-view-form {{
        display:grid;
        gap:10px;
      }}
      .intv-view-form textarea {{
        width:100%;
        border:1px solid var(--border);
        border-radius:12px;
        background:var(--surface-2);
        color:var(--text);
        padding:11px 12px;
        font-size:14px;
        min-height:190px;
      }}
      .intv-view-form textarea:focus {{
        outline:none;
        border-color:var(--primary);
        box-shadow:0 0 0 3px rgba(124,58,237,.14);
      }}
      @media (max-width: 860px) {{
        .intv-view-grid {{ grid-template-columns:1fr; }}
        .intv-view-hero {{ flex-direction:column; }}
        .intv-view-title {{ font-size:26px; }}
      }}
    </style>
    <div class="intv-view-page">
      <div class="intv-view-shell">
        <section class="intv-view-hero">
          <div>
            <div class="intv-view-kicker">Interview review</div>
            <h1 class="intv-view-title">Interview #{interview.get('id')}</h1>
            <div class="intv-view-sub">Project: {html.escape(interview.get('project_name') or '—')}</div>
          </div>
          <a class="intv-view-btn" href="{back_href}">Back to interviews</a>
        </section>

        {success_block}
        {error_block}

        <section class="intv-view-grid">
          <article class="intv-view-card">
            <h3 class="intv-view-h3">Interview metadata</h3>
            <div class="intv-view-meta">
              <div><b>Mode:</b> {html.escape(mode_text)}</div>
              <div><b>Consent:</b> {consent_text}</div>
              <div><b>Audio allowed:</b> {audio_allowed_text}</div>
              <div><b>Transcript status:</b> {transcript_status}</div>
              <div class="intv-view-muted"><b>Created:</b> {html.escape(interview.get('created_at') or '')}</div>
              <div class="intv-view-muted"><b>Updated:</b> {html.escape(interview.get('updated_at') or '')}</div>
              {f"<div class='intv-view-muted'><b>Transcription:</b> {html.escape(transcribe_reason)}</div>" if not transcribe_ok else ""}
            </div>
          </article>
          <article class="intv-view-card">
            <h3 class="intv-view-h3">Audio playback</h3>
            {audio_block}
          </article>
        </section>

        <section class="intv-view-card">
          <h3 class="intv-view-h3">Interview notes</h3>
          <div class="intv-view-text">{html.escape(interview.get('interview_text') or '')}</div>
        </section>

        <section class="intv-view-card">
          <h3 class="intv-view-h3">Transcript</h3>
          <form method="POST" class="intv-view-form">
            <textarea name="transcript_text" rows="8" placeholder="Paste or edit transcript here...">{html.escape(interview.get('transcript_text') or '')}</textarea>
            <div style="display:flex; justify-content:flex-end;">
              <button class="btn btn-primary" type="submit">Save transcript</button>
            </div>
          </form>
        </section>
      </div>
    </div>
    <script src="/static/interview_upload.js"></script>
    """
    return ui_shell("Interview", html_page, show_project_switcher=False)


@app.route("/ui/interviews/<int:interview_id>/audio", methods=["POST"])
def ui_interview_audio(interview_id):
    err = ""
    try:
        user = getattr(g, "user", None)
        is_admin = bool(user)
        consent_confirm = (request.form.get("confirm_audio_upload") or "").strip().upper()
        if consent_confirm != "YES":
            raise ValueError("Audio upload must be explicitly confirmed.")
        file = request.files.get("audio")
        if not file:
            raise ValueError("Audio file required.")
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, audio_recording_allowed, assignment_id, project_id FROM qualitative_interviews WHERE id=? LIMIT 1",
                (int(interview_id),),
            )
            row = cur.fetchone()
        if not row:
            raise ValueError("Interview not found.")
        if int(row["audio_recording_allowed"] or 0) != 1:
            raise ValueError("Audio recording was not permitted for this interview.")
        if not is_admin:
            enum_code = (request.form.get("enumerator_code") or "").strip()
            if not enum_code:
                raise ValueError("Enumerator code required.")
            ctx = prj.validate_enumerator_code(enum_code)
            if not ctx.get("ok"):
                raise ValueError(ctx.get("error") or "Invalid enumerator code.")
            if int(ctx.get("project_id") or 0) != int(row["project_id"] or 0):
                raise ValueError("Enumerator code does not match this interview.")
            if row["assignment_id"] and int(ctx.get("assignment_id") or 0) != int(row["assignment_id"] or 0):
                raise ValueError("Enumerator assignment mismatch.")
        filename = secure_filename(file.filename or "audio.wav")
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        save_name = f"interview_{interview_id}_{ts}_{filename}"
        save_path = os.path.join(UPLOAD_DIR, save_name)
        file.save(save_path)
        rel = f"/uploads/{save_name}"
        with get_conn() as conn:
            conn.execute(
                "UPDATE qualitative_interviews SET audio_file_url=?, audio_confirmed=1, updated_at=? WHERE id=?",
                (rel, now_iso(), int(interview_id)),
            )
            conn.commit()
        return redirect(url_for("ui_interview_view", interview_id=int(interview_id)))
    except Exception as e:
        err = str(e)
    return ui_shell("Audio upload", f"<div class='card'><h2>Upload failed</h2><div class='muted'>{html.escape(err)}</div></div>")

@app.route("/ui/assignments/<int:assignment_id>/qr", methods=["GET"])
def ui_assignment_qr(assignment_id):
    gate = admin_gate()
    if gate:
        return gate
    code_full = ""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT code_full FROM enumerator_assignments WHERE id=? LIMIT 1",
            (int(assignment_id),),
        )
        row = cur.fetchone()
        if row:
            code_full = row["code_full"] or ""
    if not code_full:
        return ui_shell("Assignment QR", "<div class='card'><h2>Code not found</h2></div>"), 404
    try:
        import qrcode
        img = qrcode.make(code_full)
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)
    except Exception:
        return ui_shell("Assignment QR", "<div class='card'><h2>QR generation failed</h2></div>"), 500
    return send_file(buf, mimetype="image/png")


@app.route("/ui/templates", methods=["GET", "POST"])
def ui_templates():
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    msg = ""
    err = ""
    is_admin = bool(ADMIN_KEY and request.args.get("key") == ADMIN_KEY)
    org_id = current_org_id() if (REQUIRE_SUPERVISOR_KEY and not is_admin) else None
    if org_id is None:
        try:
            sess_org = session.get("org_id")
            org_id = int(sess_org) if sess_org is not None else None
        except Exception:
            org_id = None
    project_id = request.args.get("project_id") or ""
    project_id = int(project_id) if str(project_id).isdigit() else None
    project_id = resolve_project_context(project_id)
    if project_id is None and org_id is not None:
        try:
            project_id = prj.get_default_project_id(int(org_id))
        except Exception:
            project_id = None
    project = prj.get_project(int(project_id)) if project_id is not None else None
    project_locked = project_is_locked(project) if project else False

    if PROJECT_REQUIRED and project_id is None:
        projects = prj.list_projects(200, organization_id=current_org_id())
        options = "".join(
            [f"<option value='{p.get('id')}'>{html.escape(p.get('name') or 'Project')}</option>" for p in projects]
        )
        html_view = f"""
        <div class="card">
          <h1 class="h1">Templates</h1>
          <div class="muted">Select a project to manage templates.</div>
          <form method="GET" style="margin-top:12px">
            <select name="project_id" required>
              <option value="">Choose project</option>
              {options}
            </select>
            <button class="btn btn-primary" type="submit" style="margin-left:8px">Open</button>
          </form>
        </div>
        """
        return ui_shell("Templates", html_view, show_project_switcher=False)

    # Create template
    if request.method == "POST":
        try:
            if project_locked:
                raise ValueError("Archived projects are read-only.")
            name = (request.form.get("name") or "").strip()
            desc = (request.form.get("description") or "").strip()
            created_by = (request.form.get("created_by") or "").strip()
            template_version = (request.form.get("template_version") or "v1").strip()

            if not name:
                raise ValueError("Template name is required.")
            if PROJECT_REQUIRED and project_id is None:
                raise ValueError("Select a project for this template.")

            require_enum_code = 1 if request.form.get(
                "require_enumerator_code") == "on" else 0
            enable_gps = 1 if request.form.get("enable_gps") == "on" else 0
            enable_consent = 1 if request.form.get("enable_consent") == "on" else 0
            enable_attestation = 1 if request.form.get("enable_attestation") == "on" else 0
            is_sensitive = 1 if request.form.get("is_sensitive") == "on" else 0
            restricted_exports = 1 if request.form.get("restricted_exports") == "on" else 0
            redacted_fields = (request.form.get("redacted_fields") or "").strip()

            tid = tpl.create_template(
                name=name,
                description=desc,
                is_active=1,
                require_enumerator_code=require_enum_code,
                enable_gps=enable_gps,
                enable_coverage=0,
                coverage_scheme_id=None,
                project_id=(
                    project_id if project_id is not None else prj.get_default_project_id(current_org_id())),
                created_by=created_by or "Supervisor",
                source="manual",
                template_version=template_version,
                enable_consent=enable_consent,
                enable_attestation=enable_attestation,
                is_sensitive=is_sensitive,
                restricted_exports=restricted_exports,
                redacted_fields=redacted_fields,
            )
            ensure_share_token(int(tid))  # make sure share token exists
            msg = f"Template created successfully (ID: {tid})."
        except Exception as e:
            err = str(e)

    # Load templates
    templates = tpl.list_templates(300, project_id=project_id)
    assignment_filter = (request.args.get("assignment_mode") or "ALL").strip().upper()
    if assignment_filter and assignment_filter != "ALL":
        templates = [t for t in templates if (t[7] or "INHERIT").strip().upper() == assignment_filter]

    # Build table rows
    rows_html = []
    for (tid, name, desc, created_at, created_by, updated_at, source, assignment_mode) in templates:
        cfg = tpl.get_template_config(int(tid))
        token = cfg.get("share_token") or ""
        is_active = int(cfg.get("is_active", 1))
        subs = template_submissions_count(int(tid))
        assignment_label = {
            "INHERIT": "Inherits project policy",
            "OPTIONAL": "Assignment optional",
            "REQUIRED_PROJECT": "Assignment required",
            "REQUIRED_TEMPLATE": "Template assignment required",
        }.get((assignment_mode or "INHERIT").strip().upper(), "Inherits project policy")

        rows_html.append(
            f"""
            <tr>
              <td><span class="template-id">#{tid}</span></td>
              <td>
                <div class="template-name">{name}</div>
                <div class="template-desc">{(desc or "No description provided.")}</div>
                <div class="template-meta">Created by {created_by or "—"} · {source or "manual"} · {assignment_label}</div>
              </td>
              <td class="muted">{created_at}</td>
              <td>
                <span class="status-badge {'status-active' if is_active == 1 else 'status-archived'}">
                  {"🟢 Active" if is_active == 1 else "🔴 Archived"}
                </span>
              </td>
              <td><span class="submission-count">{subs}</span></td>
              <td>
                <div class="action-buttons">
                  <a class="btn" href="{url_for('ui_template_manage', template_id=tid)}{key_q}">Manage</a>
                  <a class="btn" href="{url_for('ui_template_preview', template_id=tid)}{key_q}">👁️ Preview</a>
                  <a class="btn btn-primary" href="{url_for('ui_template_share', template_id=tid)}{key_q}">Share</a>
                </div>
              </td>
            </tr>
            """
        )

    # Page HTML
    html = f"""
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=Poppins:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
      .premium-header{{
        background:linear-gradient(135deg, rgba(124,58,237,.15) 0%, rgba(124,58,237,.05) 100%);
        border-radius:24px;
        padding:32px;
        margin-bottom:24px;
        border:1px solid rgba(124,58,237,.2);
      }}
      .premium-header h1{{
        margin:0 0 8px 0;
        font-size:28px;
        font-weight:850;
        color:var(--primary);
      }}
      .premium-header .subtitle{{
        font-size:14px;
        color:var(--muted);
        line-height:1.5;
        margin:0;
      }}
      .alert-box{{
        padding:16px 20px;
        border-radius:16px;
        margin-bottom:24px;
        border-left:4px solid;
        font-weight:600;
      }}
      .alert-success{{
        border-color:#2ecc71;
        background:rgba(46, 204, 113, .1);
        color:#27ae60;
      }}
      .alert-error{{
        border-color:#e74c3c;
        background:rgba(231, 76, 60, .1);
        color:#c0392b;
      }}
      .form-section{{
        background:var(--surface);
        border:1px solid var(--border);
        border-radius:20px;
        padding:28px;
        margin-bottom:24px;
      }}
      .form-section h2{{
        margin-top:0;
        margin-bottom:6px;
        font-size:20px;
        font-weight:800;
      }}
      .form-group{{
        margin-bottom:24px;
      }}
      .form-group label{{
        display:block;
        font-weight:700;
        margin-bottom:8px;
        font-size:14px;
        color:var(--text);
        font-family:"Inter", system-ui, -apple-system, sans-serif;
      }}
      .form-group input,
      .form-group textarea{{
        padding:12px 14px;
        border-radius:12px;
        border:1px solid rgba(124,58,237,.22);
        background:linear-gradient(180deg, #ffffff 0%, #f7f8fc 100%);
        color:var(--text);
        font-size:14px;
        font-family:"Inter", system-ui, -apple-system, sans-serif;
        transition:all 0.3s ease;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.85);
        width:100%;
      }}
      html[data-theme="dark"] .form-group input,
      html[data-theme="dark"] .form-group textarea{{
        background:linear-gradient(180deg, rgba(30,41,59,.9) 0%, rgba(17,24,39,.92) 100%);
        border-color:rgba(167,139,250,.3);
        color:#e5e7eb;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.02);
      }}
      .form-group input:focus,
      .form-group textarea:focus{{
        outline:none;
        border-color:rgba(124,58,237,.72);
        box-shadow:0 0 0 4px rgba(124,58,237,.14);
      }}
      .checkbox-group{{
        display:flex;
        gap:14px;
        margin-bottom:16px;
        align-items:flex-start;
      }}
      .checkbox-group input{{
        width:auto !important;
        margin-top:3px;
        cursor:pointer;
      }}
      .checkbox-group label{{
        margin:0;
        font-weight:600;
        cursor:pointer;
        font-family:"Inter", system-ui, -apple-system, sans-serif;
      }}
      .checkbox-group .muted{{
        font-weight:400;
        font-size:14px;
      }}
      .submit-btn{{
        padding:10px 24px;
        font-size:14px;
        font-weight:700;
        border:none;
        border-radius:10px;
        background:linear-gradient(135deg, var(--primary), var(--primary-500));
        color:#fff;
        cursor:pointer;
        transition:all 0.3s ease;
        box-shadow:0 12px 30px rgba(124,58,237,.35);
      }}
      .submit-btn:hover{{
        box-shadow:0 16px 40px rgba(124,58,237,.45);
        transform:translateY(-2px);
      }}
      .templates-section{{
        background:var(--surface);
        border:1px solid var(--border);
        border-radius:20px;
        padding:28px;
      }}
      .templates-section h2{{
        margin-top:0;
        margin-bottom:6px;
        font-size:20px;
        font-weight:800;
      }}
      .table-wrapper{{
        margin-top:24px;
        overflow:auto;
        border-radius:12px;
        border:1px solid var(--border);
      }}
      .table{{
        width:100%;
        border-collapse:collapse;
      }}
      .table th{{
        background:var(--surface-2);
        padding:12px;
        text-align:left;
        font-weight:700;
        font-size:13px;
        border-bottom:2px solid var(--border);
        color:var(--text);
      }}
      .table td{{
        padding:12px;
        border-bottom:1px solid var(--border);
        vertical-align:middle;
      }}
      .table tr:hover{{
        background:var(--surface-2);
      }}
      .template-id{{
        font-weight:850;
        color:var(--primary);
        font-size:14px;
      }}
      .template-name{{
        font-weight:800;
        font-size:14px;
        margin-bottom:4px;
      }}
      .template-desc{{
        color:var(--muted);
        font-size:13px;
        line-height:1.4;
      }}
      .template-meta{{
        color:var(--muted);
        font-size:12px;
        margin-top:6px;
      }}
      .status-badge{{
        display:inline-block;
        padding:6px 10px;
        border-radius:999px;
        font-weight:700;
        font-size:12px;
        border:1px solid;
      }}
      .status-active{{
        background:rgba(46, 204, 113, .12);
        border-color:#2ecc71;
        color:#27ae60;
      }}
      .status-archived{{
        background:rgba(231, 76, 60, .12);
        border-color:#e74c3c;
        color:#c0392b;
      }}
      .status-draft{{
        background:rgba(245, 158, 11, .12);
        border-color:#f59e0b;
        color:#b45309;
      }}
      .submission-count{{
        font-weight:800;
        font-size:15px;
        color:var(--primary);
      }}
      .action-buttons{{
        display:flex;
        gap:8px;
        flex-wrap:wrap;
        align-items:center;
      }}
      .action-buttons .btn{{
        padding:8px 12px;
        font-size:13px;
        border-radius:8px;
        transition:all 0.3s ease;
        white-space:nowrap;
        display:inline-block;
        text-align:center;
        line-height:1;
      }}
      .empty-state{{
        text-align:center;
        padding:48px 20px;
        color:var(--muted);
      }}
      .empty-state svg{{
        width:64px;
        height:64px;
        margin-bottom:16px;
        opacity:0.3;
      }}
    </style>

    <div class="premium-header">
      <h1>Templates</h1>
      <p class="subtitle">
        Create survey templates, add questions, and generate share links for enumerators.
      </p>
    </div>

    {"<div class='alert-box alert-error'>Archived projects are read-only. Template creation is disabled.</div>" if project_locked else ""}
    {"<div class='alert-box alert-success'>" + msg + "</div>" if msg else ""}
    {"<div class='alert-box alert-error'>" + err + "</div>" if err else ""}

    <div class="form-section">
      <h2>Create a new template</h2>
      <p class="muted" style="margin-bottom:24px">
        Start with a title, optional description, and choose whether GPS or enumerator code is required.
      </p>

      <form method="POST">
        <div class="form-group">
          <label>Template name</label>
          <input type="text" name="name" placeholder="e.g., Facility Assessment (PHC)" required />
        </div>

        <div class="form-group">
          <label>Description (optional)</label>
          <textarea name="description" placeholder="Briefly describe what this template is for..." rows="4"></textarea>
        </div>

        <div class="form-group">
          <label>Template version</label>
          <input type="text" name="template_version" value="v1" />
        </div>

        <div class="form-group">
          <label>Created by</label>
          <input type="text" name="created_by" placeholder="Supervisor or team name" />
        </div>

        <div class="form-group">
          <label style="margin-bottom:14px">Settings</label>
          <div class="checkbox-group">
            <input type="checkbox" id="require_enum" name="require_enumerator_code" />
            <div>
              <label for="require_enum" style="margin:0">Require enumerator code</label>
              <div class="muted" style="margin-top:4px">(recommended for controlled projects)</div>
            </div>
          </div>
          <div class="checkbox-group">
            <input type="checkbox" id="enable_gps_chk" name="enable_gps" />
            <div>
              <label for="enable_gps_chk" style="margin:0">Enable GPS capture</label>
              <div class="muted" style="margin-top:4px">(optional verification)</div>
            </div>
          </div>
        </div>

        <div class="form-group">
          <label style="margin-bottom:14px">Ethics & data governance</label>
          <div class="checkbox-group">
            <input type="checkbox" id="enable_consent_chk" name="enable_consent" />
            <div>
              <label for="enable_consent_chk" style="margin:0">Include consent question</label>
              <div class="muted" style="margin-top:4px">(adds a Yes/No consent field)</div>
            </div>
          </div>
          <div class="checkbox-group">
            <input type="checkbox" id="enable_attestation_chk" name="enable_attestation" />
            <div>
              <label for="enable_attestation_chk" style="margin:0">Enumerator attestation</label>
              <div class="muted" style="margin-top:4px">(requires enumerator confirmation)</div>
            </div>
          </div>
          <div class="checkbox-group">
            <input type="checkbox" id="is_sensitive_chk" name="is_sensitive" />
            <div>
              <label for="is_sensitive_chk" style="margin:0">Sensitive data</label>
              <div class="muted" style="margin-top:4px">(flag for ethics review)</div>
            </div>
          </div>
          <div class="checkbox-group">
            <input type="checkbox" id="restricted_exports_chk" name="restricted_exports" />
            <div>
              <label for="restricted_exports_chk" style="margin:0">Restrict exports</label>
              <div class="muted" style="margin-top:4px">(redact responses in exports)</div>
            </div>
          </div>
          <div style="margin-top:10px">
            <label>Redacted fields (optional)</label>
            <input name="redacted_fields" placeholder="Comma-separated keywords or question IDs" />
            <div class="muted" style="margin-top:4px">Example: name, phone, q_12</div>
          </div>
        </div>

        <button type="submit" class="submit-btn" {"disabled" if project_locked else ""}>Create Template</button>
      </form>
    </div>

    <div class="templates-section">
      <h2>Your templates</h2>
      <p class="muted" style="margin-bottom:0">Manage questions, preview forms, and share links.</p>
      <form method="GET" style="margin-top:16px; display:flex; gap:10px; flex-wrap:wrap; align-items:center;">
        {"<input type='hidden' name='project_id' value='" + str(project_id) + "'/>" if project_id is not None else ""}
        <label class="muted">Assignment policy</label>
        <select name="assignment_mode" style="max-width:260px;">
          <option value="ALL" {"selected" if assignment_filter == "ALL" else ""}>All</option>
          <option value="INHERIT" {"selected" if assignment_filter == "INHERIT" else ""}>Inherit project policy</option>
          <option value="OPTIONAL" {"selected" if assignment_filter == "OPTIONAL" else ""}>Assignment optional</option>
          <option value="REQUIRED_PROJECT" {"selected" if assignment_filter == "REQUIRED_PROJECT" else ""}>Assignment required</option>
          <option value="REQUIRED_TEMPLATE" {"selected" if assignment_filter == "REQUIRED_TEMPLATE" else ""}>Template assignment required</option>
        </select>
        <button class="btn btn-sm" type="submit">Filter</button>
      </form>

      <div class="table-wrapper">
        <table class="table">
          <thead>
            <tr>
              <th style="width:90px">ID</th>
              <th>Template</th>
              <th style="width:150px">Created</th>
              <th style="width:120px">Status</th>
              <th style="width:120px">Submissions</th>
              <th style="width:300px">Actions</th>
            </tr>
          </thead>
          <tbody>
            {("".join(rows_html) if rows_html else "<tr><td colspan='6' style='padding:48px'><div class='empty-state'><div style='font-weight:600;font-size:16px;color:var(--text);margin-bottom:8px'>No templates yet</div><div>Create your first template above to get started</div></div></td></tr>")}
          </tbody>
        </table>
      </div>
    </div>
    """

    return ui_shell("Templates", html, show_project_switcher=False)


@app.route("/ui/templates/<int:template_id>/share")
def ui_template_share(template_id):
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""

    cfg = tpl.get_template_config(int(template_id))
    if not cfg:
        return ui_shell("Template not found", "<div class='card'><h2>Template not found</h2></div>"), 404
    template_name = cfg.get("name", f"Template {template_id}")
    is_active = int(cfg.get("is_active", 1))
    assignment_mode = (cfg.get("assignment_mode") or "INHERIT").strip().upper()
    assignment_label = {
        "INHERIT": "Inherits project policy",
        "OPTIONAL": "Assignment optional",
        "REQUIRED_PROJECT": "Assignment required",
        "REQUIRED_TEMPLATE": "Template assignment required",
    }.get(assignment_mode, "Inherits project policy")
    assignment_badge = f"<span style='padding:6px 10px;border-radius:999px;border:1px solid var(--border);background:var(--surface-2);font-weight:700;'>{assignment_label}</span>"

    token = ensure_share_token(int(template_id))
    share_url = request.host_url.rstrip(
        "/") + share_path_for_template_row(cfg, token)

    submissions = template_submissions_count(int(template_id))

    qr_view_url = url_for("ui_template_qr_png",
                          template_id=template_id) + key_q
    qr_download_url = url_for("ui_template_qr_png", template_id=template_id) + \
        "?download=1" + (("&key=" + ADMIN_KEY) if ADMIN_KEY else "")
    actions_url = url_for("ui_template_share_actions",
                          template_id=template_id) + key_q
    open_form_url = share_path_for_template_row(cfg, token)

    status_badge = (
        "<span style='padding:6px 10px;border-radius:999px;border:1px solid rgba(46,204,113,.35);background:rgba(46,204,113,.07);color:#0a5;'>ACTIVE</span>"
        if is_active == 1
        else "<span style='padding:6px 10px;border-radius:999px;border:1px solid rgba(231,76,60,.35);background:rgba(231,76,60,.06);color:#b00;'>DISABLED</span>"
    )

    html = f"""
    <style>
      .share-panel-actions .btn{{padding:10px 14px; font-size:14px}}
      .share-panel-actions .btn.btn-primary{{box-shadow:0 10px 24px rgba(124,58,237,.25)}}
      .share-section-title{{font-size:18px; font-weight:800}}
      .toggle-row{{display:flex; gap:12px; align-items:center}}
      .toggle-text{{display:flex; flex-direction:column; gap:2px}}
      .toggle-title{{font-weight:800; letter-spacing:-.2px}}
      .toggle-help{{color:var(--muted); font-size:13px}}
      .toggle-switch{{position:relative; width:52px; height:30px; display:inline-block}}
      .toggle-switch input{{opacity:0; width:0; height:0}}
      .toggle-slider{{
        position:absolute; inset:0; background:var(--surface-2); border:1px solid var(--border);
        border-radius:999px; transition:all .2s ease;
      }}
      .toggle-slider::after{{
        content:""; position:absolute; width:22px; height:22px; left:3px; top:3px;
        background:#fff; border-radius:50%; box-shadow:0 4px 10px rgba(0,0,0,.15); transition:all .2s ease;
      }}
      .toggle-switch input:checked + .toggle-slider{{background:rgba(124,58,237,.18); border-color:rgba(124,58,237,.5)}}
      .toggle-switch input:checked + .toggle-slider::after{{transform:translateX(22px); background:#8E5CFF}}
    </style>

    <div class="stack">

      <div class="card">
        <div class="row" style="justify-content:space-between; align-items:flex-start; gap:14px;">
          <div>
            <div class="h1">Share Panel</div>
            <div class="muted">Template: <b>{template_name}</b></div>
            <div style="margin-top:10px">{status_badge} {assignment_badge} <span class="muted" style="margin-left:10px">Submissions: <b>{submissions}</b></span></div>
          </div>
          <div class="row share-panel-actions">
            <a class="btn" href="{url_for('ui_template_manage', template_id=template_id)}{key_q}">Back to Builder</a>
            <a class="btn btn-primary" href="{open_form_url}" target="_blank">Open Enumerator Form</a>
          </div>
        </div>
      </div>

      <div class="card">
        <div class="h2 share-section-title">Share link</div>
        <div class="muted" style="margin-top:6px">Send this link to enumerators. Anyone with the link can submit.</div>

        <div style="
          margin-top:12px;
          font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
          font-size:13px;
          word-break:break-all;
          padding:12px;
          border-radius:14px;
          border:1px solid var(--border);
          background:var(--surface);
        ">{share_url}</div>

        <div class="row" style="margin-top:12px">
          <button class="btn btn-primary" type="button" data-copy="{share_url}">Copy Link</button>

          <form method="POST" action="{actions_url}" style="margin:0">
            <input type="hidden" name="action" value="rotate_link"/>
            <button class="btn" type="submit" onclick="return confirm('Rotate link? Old link will stop working.')">Rotate Link</button>
          </form>
        </div>

        <div class="muted" style="margin-top:10px">
          Use <b>Rotate Link</b> if the link is shared beyond your team.
        </div>
      </div>

      <div class="card">
        <div class="h2 share-section-title">QR code</div>
        <div class="muted" style="margin-top:6px">Scan on another phone to open the form instantly.</div>

        <div class="row" style="margin-top:12px; align-items:center; gap:18px;">
          <div style="padding:14px; border:1px solid var(--border); border-radius:16px; background:var(--surface-2);">
            <img src="{qr_view_url}" alt="QR Code" style="width:220px; height:220px; display:block;" />
          </div>
          <div class="stack">
            <a class="btn" href="{qr_download_url}">Download QR (PNG)</a>
            <div class="muted" style="max-width:52ch">
              Tip: Print this QR for team onboarding or pin inside a group chat.
            </div>
          </div>
        </div>
      </div>

      <div class="card">
        <div class="h2 share-section-title">Access control</div>
        <div class="muted" style="margin-top:6px">
          Disable the template to stop new submissions immediately (share link will stop working).
        </div>

        <form method="POST" action="{actions_url}" style="margin-top:12px">
          <input type="hidden" name="action" value="toggle_active"/>
          <div class="toggle-row">
            <label class="toggle-switch" aria-label="Allow submissions">
              <input type="checkbox" name="is_active" {"checked" if is_active == 1 else ""}/>
              <span class="toggle-slider"></span>
            </label>
            <div class="toggle-text">
              <div class="toggle-title">Allow submissions</div>
              <div class="toggle-help">Keep this on to accept new responses.</div>
            </div>
          </div>
          <div style="margin-top:12px">
            <button class="btn" type="submit">Save</button>
          </div>
        </form>

        <div class="row" style="margin-top:12px">
          <a class="btn" href="{url_for('ui_template_archive', template_id=template_id)}{key_q}"
             onclick="return confirm('Archive this template? Share link will stop working.')">
             Archive template
          </a>
        </div>
      </div>

    </div>
    """

    return ui_shell("Share Panel", html)


@app.route("/ui/templates/<int:template_id>/qr.png")
def ui_template_qr_png(template_id):
    gate = admin_gate()
    if gate:
        return gate

    cfg = tpl.get_template_config(int(template_id))
    token = ensure_share_token(int(template_id))
    share_url = request.host_url.rstrip(
        "/") + share_path_for_template_row(cfg, token)

    try:
        import io
        import qrcode

        img = qrcode.make(share_url)
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)

        download = request.args.get("download", "") == "1"
        if download:
            return send_file(
                buf,
                mimetype="image/png",
                as_attachment=True,
                download_name=f"openfield-template-{template_id}-qr.png",
            )

        return send_file(buf, mimetype="image/png")
    except Exception as e:
        return (
            "QR generator not available. Install: python3 -m pip install \"qrcode[pil]\"\n\n"
            f"Details: {str(e)}",
            500,
        )


@app.route("/ui/templates/<int:template_id>/share/actions", methods=["POST"])
def ui_template_share_actions(template_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return ui_shell("Read-only", "<div class='card'><h2>Archived projects are read-only.</h2></div>"), 403

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""

    action = (request.form.get("action") or "").strip().lower()

    if action == "toggle_active":
        # Checkbox returns "on" when checked
        is_active = 1 if request.form.get("is_active") == "on" else 0
        tpl.set_template_config(int(template_id), is_active=is_active)

    elif action == "rotate_link":
        import secrets
        new_token = secrets.token_urlsafe(16)
        tpl.set_template_config(int(template_id), share_token=new_token)

    return redirect(url_for("ui_template_share", template_id=template_id) + key_q)


@app.route("/ui/templates/<int:template_id>/preview")
def ui_template_preview(template_id):
    gate = admin_gate()
    if gate:
        return gate

    token = ensure_share_token(int(template_id))
    if not token:
        return render_template_string("<h2>Cannot preview</h2><p>Missing share_token column.</p>"), 400

    cfg = tpl.get_template_config(int(template_id))
    if not cfg:
        return ui_shell("Template not found", "<div class='card'><h2>Template not found</h2></div>"), 404
    qs = "?preview=1"
    if ADMIN_KEY:
        qs += f"&key={ADMIN_KEY}"
    return redirect(share_path_for_template_row(cfg, token) + qs)


@app.route("/ui/templates/<int:template_id>/manage", methods=["GET", "POST"])
def ui_template_manage(template_id):
    gate = admin_gate()
    if gate:
        return gate
    from html import escape as html_escape

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    err = ""
    msg = ""

    # Load config + template basics (safe)
    cfg = tpl.get_template_config(int(template_id))
    if not cfg:
        return ui_shell("Template not found", "<div class='card'><h2>Template not found</h2></div>"), 404

    # Save template settings
    if request.method == "POST":
        try:
            action = (request.form.get("action") or "").strip().lower()
            if action == "publish_template":
                tpl.set_template_config(int(template_id), is_active=1)
                try:
                    with get_conn() as conn:
                        cur = conn.cursor()
                        cur.execute(
                            "SELECT project_id FROM survey_templates WHERE id=? LIMIT 1",
                            (int(template_id),),
                        )
                        row = cur.fetchone()
                        pid = row["project_id"] if row else None
                        if pid:
                            conn.execute(
                                "UPDATE projects SET status='ACTIVE', is_active=1 WHERE id=?",
                                (int(pid),),
                            )
                            conn.commit()
                except Exception:
                    pass
                msg = "Template published."
                cfg = tpl.get_template_config(int(template_id))
                return redirect(url_for("ui_template_manage", template_id=template_id) + key_qs())
            name = (request.form.get("name") or "").strip()
            description = (request.form.get("description") or "").strip()
            template_version = (request.form.get("template_version") or "v1").strip()

            is_active = 1 if request.form.get("is_active") == "on" else 0
            require_enum_code = 1 if request.form.get(
                "require_enumerator_code") == "on" else 0
            enable_gps = 1 if request.form.get("enable_gps") == "on" else 0
            enable_consent = 1 if request.form.get("enable_consent") == "on" else 0
            enable_attestation = 1 if request.form.get("enable_attestation") == "on" else 0
            is_sensitive = 1 if request.form.get("is_sensitive") == "on" else 0
            restricted_exports = 1 if request.form.get("restricted_exports") == "on" else 0
            redacted_fields = (request.form.get("redacted_fields") or "").strip()
            assignment_mode = (request.form.get("assignment_mode") or "INHERIT").strip().upper()
            collect_email = 1 if request.form.get("collect_email") == "on" else 0
            limit_one_response = 1 if request.form.get("limit_one_response") == "on" else 0
            allow_edit_response = 1 if request.form.get("allow_edit_response") == "on" else 0
            show_summary_charts = 1 if request.form.get("show_summary_charts") == "on" else 0
            confirmation_message = (request.form.get("confirmation_message") or "").strip()

            tpl.set_template_config(
                int(template_id),
                name=(name or cfg.get("name") or "Untitled Template"),
                description=description,
                is_active=is_active,
                require_enumerator_code=require_enum_code,
                enable_gps=enable_gps,
                assignment_mode=assignment_mode,
                template_version=template_version,
                enable_consent=enable_consent,
                enable_attestation=enable_attestation,
                is_sensitive=is_sensitive,
                restricted_exports=restricted_exports,
                redacted_fields=redacted_fields,
                collect_email=collect_email,
                limit_one_response=limit_one_response,
                allow_edit_response=allow_edit_response,
                show_summary_charts=show_summary_charts,
                confirmation_message=confirmation_message,
            )
            msg = "Template settings saved."
            cfg = tpl.get_template_config(int(template_id))
        except Exception as e:
            err = str(e)

    # Ensure share token exists
    token = ensure_share_token(int(template_id))
    share_url = request.host_url.rstrip(
        "/") + share_path_for_template_row(cfg, token)

    # Counts
    subs = template_submissions_count(int(template_id))
    assignment_mode = (cfg.get("assignment_mode") or "INHERIT").strip().upper()
    assignment_label = {
        "INHERIT": "Inherits project policy",
        "OPTIONAL": "Assignment optional",
        "REQUIRED_PROJECT": "Assignment required",
        "REQUIRED_TEMPLATE": "Template assignment required",
    }.get(assignment_mode, "Inherits project policy")
    assignment_badge = f"<span style='padding:6px 10px;border-radius:999px;border:1px solid var(--border);background:var(--surface-2);font-weight:700;'>{assignment_label}</span>"

    # Questions
    questions = tpl.get_template_questions(int(template_id))

    def _is_section_marker(text: str) -> bool:
        t = (text or "").strip()
        return t.startswith("## ") or t.upper().startswith("[SECTION]")

    def _parse_section_marker(text: str) -> dict:
        t = (text or "").strip()
        if t.upper().startswith("[SECTION]"):
            payload = t[len("[SECTION]"):].strip()
            title = payload
            desc = ""
            if "|" in payload:
                title, desc = payload.split("|", 1)
            return {"title": title.strip() or "Section", "desc": desc.strip()}
        if t.startswith("## "):
            rest = t[3:].strip()
            if "\n" in rest:
                title, desc = rest.split("\n", 1)
                return {"title": title.strip() or "Section", "desc": desc.strip()}
            return {"title": rest.strip() or "Section", "desc": ""}
        return {"title": t.strip() or "Section", "desc": ""}

    def _is_media_marker(text: str) -> bool:
        t = (text or "").strip()
        return t.upper().startswith("[IMAGE] ") or t.upper().startswith("[VIDEO] ")

    def _parse_media_marker(text: str) -> dict:
        raw = (text or "").strip()
        upper = raw.upper()
        kind = "IMAGE" if upper.startswith("[IMAGE]") else "VIDEO"
        payload = raw[len("[IMAGE]"):].strip() if kind == "IMAGE" else raw[len("[VIDEO]"):].strip()
        parts = payload.split("|", 1)
        url = parts[0].strip()
        caption = parts[1].strip() if len(parts) > 1 else ""
        return {"kind": kind, "url": url, "caption": caption}

    # Build question cards
    choice_types = {"SINGLE_CHOICE", "MULTI_CHOICE", "DROPDOWN"}
    q_cards = []
    for row in questions:
        qid = row[0]
        qtext = row[1]
        qtype = row[2] if len(row) > 2 else "TEXT"
        order_no = row[3] if len(row) > 3 else ""
        is_required = row[4] if len(row) > 4 else 0
        is_section = _is_section_marker(qtext)
        is_media = _is_media_marker(qtext)

        display_title = qtext
        actions = []
        choices = q_choices(qid) if qtype in choice_types else []
        option_rows = []
        for c in choices:
            ctext = c[2]
            option_rows.append(
                f"""
                <div class="q-option">
                  <span class="q-option-dot"></span>
                  <input class="q-option-input" value="{html_escape(ctext)}" placeholder="Option" />
                  <button class="q-option-remove" type="button" onclick="removeOption(this)">✕</button>
                </div>
                """
            )

        options_display = "grid" if (qtype in choice_types or qtype == "YESNO") else "none"
        options_body = ""
        add_button = ""
        if qtype in choice_types:
            options_body = "".join(option_rows) if option_rows else '<div class="muted">No options yet.</div>'
            add_button = (
                f'<div class="row" style="gap:8px;">'
                f'<button class="btn btn-sm btn-ghost q-add-option" data-action="add" type="button">Add option</button>'
                f'<button class="btn btn-sm btn-ghost q-add-option" data-action="other" type="button">Add “Other”</button>'
                f'</div>'
            )
        elif qtype == "YESNO":
            options_body = '<div class="muted">Yes / No</div>'
        else:
            options_body = '<div class="muted">Add options for choice questions.</div>'
            add_button = (
                f'<div class="row" style="gap:8px; display:none;">'
                f'<button class="btn btn-sm btn-ghost q-add-option" data-action="add" type="button">Add option</button>'
                f'<button class="btn btn-sm btn-ghost q-add-option" data-action="other" type="button">Add “Other”</button>'
                f'</div>'
            )

        # Preview control (what enumerator sees)
        preview_html = ""
        if (qtype or "").upper() == "LONGTEXT":
            preview_html = "<textarea class=\"q-preview-input\" rows=\"2\" placeholder=\"Long answer text\"></textarea>"
        elif (qtype or "").upper() == "NUMBER":
            preview_html = "<input class=\"q-preview-input\" type=\"number\" placeholder=\"Enter a number\" />"
        elif (qtype or "").upper() == "DATE":
            preview_html = "<input class=\"q-preview-input\" type=\"date\" />"
        elif (qtype or "").upper() == "EMAIL":
            preview_html = "<input class=\"q-preview-input\" type=\"email\" placeholder=\"name@example.com\" />"
        elif (qtype or "").upper() == "PHONE":
            preview_html = "<input class=\"q-preview-input\" type=\"tel\" placeholder=\"Phone number\" />"
        elif (qtype or "").upper() == "YESNO":
            preview_html = (
                "<div class=\"q-preview-options\">"
                "<label class=\"q-option-line\"><input type=\"radio\" name=\"yn\" /> Yes</label>"
                "<label class=\"q-option-line\"><input type=\"radio\" name=\"yn\" /> No</label>"
                "</div>"
            )
        elif (qtype or "").upper() == "SINGLE_CHOICE":
            items = [f"<label class='q-option-line'><input type='radio' name='opt_{qid}' /><span>{html_escape(c[2])}</span></label>" for c in choices] if choices else []
            preview_html = f"<div class='q-preview-options'>{''.join(items) if items else '<div class=\"muted\">Add options below.</div>'}</div>"
        elif (qtype or "").upper() == "MULTI_CHOICE":
            items = [f"<label class='q-option-line'><input type='checkbox' /><span>{html_escape(c[2])}</span></label>" for c in choices] if choices else []
            preview_html = f"<div class='q-preview-options'>{''.join(items) if items else '<div class=\"muted\">Add options below.</div>'}</div>"
        elif (qtype or "").upper() == "DROPDOWN":
            opts = "".join([f"<option>{html_escape(c[2])}</option>" for c in choices]) if choices else "<option>Add options below</option>"
            preview_html = f"<select class='q-preview-input'><option selected disabled>Select option</option>{opts}</select>"
        else:
            preview_html = "<input class=\"q-preview-input\" placeholder=\"Short answer text\" />"

        choices_html = f"""
        <div class="q-options" style="display:{options_display}">
          <div class="q-options-label">Options</div>
          <div class="q-options-list">
            {options_body}
          </div>
          {add_button}
        </div>
        """

        if is_section:
            sec = _parse_section_marker(qtext)
            actions.append(
                f"<button class=\"btn btn-sm section-edit-btn\" type=\"button\" "
                f"data-section-id=\"{qid}\" "
                f"data-title=\"{html_escape(sec.get('title') or 'Section')}\" "
                f"data-desc=\"{html_escape(sec.get('desc') or '')}\">Edit section</button>"
            )
            q_cards.append(
                f"""
                <div class="gf-card section-card" data-qid="{qid}" draggable="true">
                  <div class="section-head">
                    <div class="section-pill">Section {order_no}</div>
                    <div class="row" style="gap:8px;">
                      {''.join(actions)}
                      <a class="btn btn-sm" href="{url_for('ui_question_delete', template_id=template_id, question_id=qid)}{key_q}" onclick="return confirm('Delete this section?')">Delete</a>
                    </div>
                  </div>
                  <div class="section-title">{html_escape(sec.get('title') or 'Untitled section')}</div>
                  <div class="section-desc">{html_escape(sec.get('desc') or 'Description (optional)')}</div>
                </div>
                """
            )
            continue
        elif is_media:
            media = _parse_media_marker(qtext)
            display_title = f"{media.get('kind', 'Media').title()} block"
            actions.append(
                f"<a class=\"btn btn-sm\" href=\"{url_for('ui_question_edit', template_id=template_id, question_id=qid)}{key_q}\">Edit</a>"
            )
            q_cards.append(
                f"""
                <div class="gf-card media-card" data-qid="{qid}" draggable="true">
                  <div class="section-head">
                    <div class="section-pill">{media.get('kind','MEDIA')}</div>
                    <div class="row" style="gap:8px;">
                      {''.join(actions)}
                      <a class="btn btn-sm" href="{url_for('ui_question_delete', template_id=template_id, question_id=qid)}{key_q}" onclick="return confirm('Delete this media block?')">Delete</a>
                    </div>
                  </div>
                  <div class="section-title">{html_escape(media.get('caption') or display_title)}</div>
                  <div class="section-desc">{html_escape(media.get('url') or '')}</div>
                </div>
                """
            )
            continue
        else:
            actions.append(
                f"<button class=\"btn btn-sm\" type=\"button\" onclick=\"saveQuestion({qid})\">Save</button>"
            )
            actions.append(
                f"<a class=\"btn btn-sm\" href=\"{url_for('ui_question_duplicate', template_id=template_id, question_id=qid)}{key_q}\">Duplicate</a>"
            )
            actions.append(
                f"<a class=\"btn btn-sm\" data-delete=\"1\" href=\"{url_for('ui_question_delete', template_id=template_id, question_id=qid)}{key_q}\" onclick=\"return confirm('Delete this question?')\">Delete</a>"
            )

        q_cards.append(
            f"""
            <div class="gf-card" data-qid="{qid}" draggable="true">
              <div class="gf-card-head">
                <div class="gf-order">
                  <span class="drag-handle" title="Drag to reorder">⋮⋮</span>
                  <span class="order-pill">{order_no}</span>
                </div>
                <div class="gf-type">
                  <select class="q-type">
                    <option value="TEXT" {"selected" if (qtype or 'TEXT').upper()=='TEXT' else ""}>Short answer</option>
                    <option value="LONGTEXT" {"selected" if (qtype or 'TEXT').upper()=='LONGTEXT' else ""}>Paragraph</option>
                    <option value="SINGLE_CHOICE" {"selected" if (qtype or 'TEXT').upper()=='SINGLE_CHOICE' else ""}>Multiple choice</option>
                    <option value="MULTI_CHOICE" {"selected" if (qtype or 'TEXT').upper()=='MULTI_CHOICE' else ""}>Checkboxes</option>
                    <option value="DROPDOWN" {"selected" if (qtype or 'TEXT').upper()=='DROPDOWN' else ""}>Dropdown</option>
                    <option value="YESNO" {"selected" if (qtype or 'TEXT').upper()=='YESNO' else ""}>Yes / No</option>
                    <option value="NUMBER" {"selected" if (qtype or 'TEXT').upper()=='NUMBER' else ""}>Number</option>
                    <option value="DATE" {"selected" if (qtype or 'TEXT').upper()=='DATE' else ""}>Date</option>
                    <option value="EMAIL" {"selected" if (qtype or 'TEXT').upper()=='EMAIL' else ""}>Email</option>
                    <option value="PHONE" {"selected" if (qtype or 'TEXT').upper()=='PHONE' else ""}>Phone</option>
                  </select>
                </div>
              </div>
              <div class="gf-question">
                <div class="q-text rich-text" contenteditable="true" data-placeholder="Untitled Question">{display_title}</div>
                <div class="q-hint">
                  {"Short answer text" if (qtype or 'TEXT').upper()=='TEXT' else "Paragraph answer text" if (qtype or 'TEXT').upper()=='LONGTEXT' else ""}
                </div>
                <div class="q-preview" data-preview-for="{qid}">
                  {preview_html}
                </div>
                {choices_html}
              </div>
              <div class="gf-card-actions">
                <div class="row" style="gap:10px;">
                  {"".join(actions)}
                </div>
                <label class="row" style="gap:8px;">
                  <span class="muted">Required</span>
                  <input class="q-required" type="checkbox" {"checked" if int(is_required)==1 else ""}/>
                </label>
              </div>
            </div>
            """
        )

    status_badge = (
        "<span style='display:inline-block; padding:6px 10px; border-radius:999px; border:1px solid var(--border); background:rgba(46,204,113,.12); font-weight:800;'>Active</span>"
        if int(cfg.get("is_active", 1)) == 1
        else "<span style='display:inline-block; padding:6px 10px; border-radius:999px; border:1px solid var(--border); background:rgba(231,76,60,.12); font-weight:800;'>Archived</span>"
    )

    try:
        quick_add_url = url_for("ui_question_quick_add", template_id=template_id) + key_q
    except Exception:
        quick_add_url = f"/ui/templates/{template_id}/questions/quick-add{key_q}"

    page_html = f"""
    <style>
      .builder-page{{
        min-height:100vh;
        background:
          radial-gradient(900px 420px at -10% -8%, rgba(124,58,237,.12), transparent 60%),
          radial-gradient(760px 360px at 110% 0%, rgba(99,102,241,.10), transparent 58%),
          linear-gradient(180deg, #F7F5FF 0%, #F3F4F9 100%);
      }}
      html[data-theme="dark"] .builder-page{{
        background:
          radial-gradient(900px 420px at -10% -8%, rgba(124,58,237,.22), transparent 60%),
          radial-gradient(760px 360px at 110% 0%, rgba(99,102,241,.18), transparent 58%),
          linear-gradient(180deg, #0F1221 0%, #111426 100%);
      }}
      .builder-container{{
        max-width:1200px;
        margin:0 auto;
        padding:24px;
      }}
      .builder-shell{{
        display:grid;
        grid-template-columns:minmax(0,1fr) 70px;
        gap:18px;
        align-items:start;
      }}
      .builder-main{{
        display:grid;
        gap:16px;
      }}
      .builder-shell .card{{
        border-radius:18px;
        border:1px solid var(--border);
        background:var(--surface);
        box-shadow:0 12px 30px rgba(15,18,34,.08);
      }}
      .builder-shell label{{
        display:block;
        margin-bottom:6px;
        font-size:12px;
        letter-spacing:.04em;
        text-transform:uppercase;
        font-weight:800;
        color:var(--muted);
      }}
      .builder-shell input,
      .builder-shell textarea,
      .builder-shell select{{
        border:1px solid rgba(124,58,237,.22);
        border-radius:12px;
        background:linear-gradient(180deg, #ffffff 0%, #f8f8fc 100%);
        width:100%;
        padding:11px 12px;
        color:var(--text);
        box-shadow: inset 0 1px 0 rgba(255,255,255,.85);
        transition:border-color .18s ease, box-shadow .18s ease, background .18s ease;
      }}
      html[data-theme="dark"] .builder-shell input,
      html[data-theme="dark"] .builder-shell textarea,
      html[data-theme="dark"] .builder-shell select{{
        background:linear-gradient(180deg, rgba(30,41,59,.9) 0%, rgba(17,24,39,.92) 100%);
        border-color:rgba(167,139,250,.3);
        color:#e5e7eb;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.02);
      }}
      .builder-shell input:focus,
      .builder-shell textarea:focus,
      .builder-shell select:focus{{
        outline:none;
        border-color:rgba(124,58,237,.72);
        box-shadow:0 0 0 4px rgba(124,58,237,.14);
      }}
      .builder-hero{{
        position:relative;
        overflow:hidden;
        padding:20px;
        border-color:rgba(124,58,237,.35) !important;
        background:linear-gradient(138deg, rgba(124,58,237,.14) 0%, rgba(255,255,255,.95) 48%, rgba(221,214,254,.6) 100%);
      }}
      html[data-theme="dark"] .builder-hero{{
        background:linear-gradient(138deg, rgba(124,58,237,.32) 0%, rgba(22,25,47,.92) 48%, rgba(56,44,95,.55) 100%);
      }}
      .builder-hero:before{{
        content:"";
        position:absolute;
        width:280px;
        height:280px;
        right:-95px;
        top:-140px;
        border-radius:50%;
        background:radial-gradient(circle, rgba(167,139,250,.32), transparent 65%);
      }}
      .builder-hero-top{{
        position:relative;
        z-index:2;
        display:flex;
        justify-content:space-between;
        align-items:flex-start;
        gap:14px;
      }}
      .builder-kicker{{
        display:inline-flex;
        align-items:center;
        border-radius:999px;
        padding:5px 10px;
        background:rgba(124,58,237,.14);
        color:var(--primary);
        font-size:11px;
        letter-spacing:.07em;
        text-transform:uppercase;
        font-weight:800;
      }}
      .builder-title{{
        margin:10px 0 6px;
        letter-spacing:-.02em;
      }}
      .builder-sub{{
        max-width:660px;
      }}
      .builder-hero-actions{{
        display:flex;
        gap:10px;
        flex-wrap:wrap;
        justify-content:flex-end;
      }}
      .builder-kpis{{
        position:relative;
        z-index:2;
        margin-top:14px;
        display:grid;
        gap:10px;
        grid-template-columns:repeat(3,minmax(0,1fr));
      }}
      .builder-kpi{{
        border:1px solid rgba(124,58,237,.24);
        border-radius:12px;
        background:rgba(255,255,255,.72);
        padding:10px 12px;
      }}
      html[data-theme="dark"] .builder-kpi{{
        background:rgba(20,24,44,.7);
        border-color:rgba(167,139,250,.30);
      }}
      .builder-kpi .k-label{{
        display:block;
        font-size:11px;
        letter-spacing:.06em;
        text-transform:uppercase;
        color:var(--muted);
        font-weight:800;
      }}
      .builder-kpi .k-val{{
        display:block;
        margin-top:4px;
        font-size:19px;
        font-weight:800;
        color:var(--text);
      }}
      .settings-head{{
        justify-content:space-between;
        align-items:flex-start;
      }}
      .settings-badges{{
        display:flex;
        gap:8px;
        flex-wrap:wrap;
        justify-content:flex-end;
        align-items:center;
      }}
      .share-card{{
        background:var(--surface-2) !important;
        border-style:dashed !important;
      }}
      .share-url{{
        font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
        font-size:13px;
        word-break:break-all;
        padding:12px;
        border-radius:14px;
        border:1px solid var(--border);
        background:var(--surface);
      }}
      .toolbar-actions{{
        display:flex;
        gap:8px;
        flex-wrap:wrap;
        justify-content:flex-end;
      }}
      .danger-zone{{
        border-color:rgba(239,68,68,.35) !important;
      }}
      .danger-actions{{
        margin-top:14px;
        display:flex;
        gap:10px;
        flex-wrap:wrap;
      }}
      .gf-toolbar{{
        position: sticky;
        top: 120px;
        margin-left: 16px;
        display:flex;
        flex-direction:column;
        gap:10px;
        padding:10px;
        border-radius:16px;
        border:1px solid var(--border);
        background:var(--surface);
        box-shadow:0 12px 28px rgba(15,18,34,.08);
        height: fit-content;
      }}
      .gf-tool{{
        width:44px;
        height:44px;
        border-radius:12px;
        border:1px solid var(--border);
        background:var(--surface-2);
        display:flex;
        align-items:center;
        justify-content:center;
        font-size:18px;
        cursor:pointer;
        transition:transform .12s ease, box-shadow .12s ease;
      }}
      .gf-tool:hover{{
        transform:translateY(-1px);
        box-shadow:0 8px 18px rgba(15,18,34,.12);
      }}
      .builder-modal{{
        position:fixed;
        inset:0;
        background:rgba(15,23,42,.48);
        backdrop-filter: blur(6px);
        display:none;
        align-items:center;
        justify-content:center;
        z-index:300;
        padding:20px;
      }}
      .builder-modal.show{{
        display:flex;
        animation:modalFade .18s ease-out;
      }}
      @keyframes modalFade{{
        from{{opacity:0;}}
        to{{opacity:1;}}
      }}
      .builder-modal-card{{
        width:min(760px, 100%);
        background:linear-gradient(160deg, #ffffff 0%, #F8F4FF 100%);
        border-radius:22px;
        border:1px solid #DDD6FE;
        padding:22px;
        box-shadow:0 26px 60px rgba(15,18,34,.24);
        animation:modalCardIn .2s ease-out;
      }}
      @keyframes modalCardIn{{
        from{{transform:translateY(12px) scale(.98); opacity:0;}}
        to{{transform:translateY(0) scale(1); opacity:1;}}
      }}
      html[data-theme="dark"] .builder-modal-card{{
        background:linear-gradient(160deg, #12152A 0%, #1A1E36 100%);
        border-color:#363B63;
      }}
      .section-modal-card{{
        position:relative;
        overflow:hidden;
      }}
      .section-modal-card:before{{
        content:"";
        position:absolute;
        top:0;
        left:0;
        right:0;
        height:4px;
        background:linear-gradient(90deg, var(--primary), #A78BFA);
      }}
      .section-modal-head{{
        display:flex;
        justify-content:space-between;
        align-items:flex-start;
        gap:16px;
        padding:6px 0 14px;
        border-bottom:1px solid rgba(124,58,237,.2);
      }}
      .section-modal-kicker{{
        display:inline-flex;
        align-items:center;
        gap:8px;
        padding:5px 10px;
        border-radius:999px;
        background:rgba(124,58,237,.12);
        color:var(--primary);
        font-size:11px;
        font-weight:800;
        letter-spacing:.06em;
        text-transform:uppercase;
      }}
      .section-modal-title{{
        margin-top:10px !important;
        font-weight:800;
      }}
      .section-modal-sub{{
        margin-top:4px;
        font-size:14px;
      }}
      .section-modal-close{{
        width:38px;
        height:38px;
        border-radius:12px;
        border:1px solid var(--border);
        background:var(--surface);
        color:var(--muted);
        display:grid;
        place-items:center;
        font-size:18px;
        cursor:pointer;
      }}
      .section-modal-close:hover{{
        color:var(--primary);
        border-color:var(--primary);
        box-shadow:0 8px 18px rgba(124,58,237,.2);
      }}
      .section-modal-grid{{
        margin-top:18px;
        display:grid;
        gap:14px;
      }}
      .section-modal-field label{{
        font-size:12px;
        font-weight:800;
        letter-spacing:.05em;
        text-transform:uppercase;
        color:var(--muted);
        display:block;
        margin-bottom:6px;
      }}
      .section-modal-field input,
      .section-modal-field textarea{{
        background:linear-gradient(180deg, #ffffff 0%, #f8f8fc 100%);
        border-color:rgba(124,58,237,.22);
        border-width:1px;
        border-style:solid;
        width:100%;
        padding:12px 14px;
        border-radius:12px;
        font-size:15px;
        color:var(--text);
        box-shadow: inset 0 1px 0 rgba(255,255,255,.85);
      }}
      html[data-theme="dark"] .section-modal-field input,
      html[data-theme="dark"] .section-modal-field textarea{{
        background:linear-gradient(180deg, rgba(30,41,59,.9) 0%, rgba(17,24,39,.92) 100%);
        border-color:rgba(167,139,250,.3);
        color:#e5e7eb;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.02);
      }}
      .section-modal-field input:focus,
      .section-modal-field textarea:focus{{
        outline:none;
        border-color:rgba(124,58,237,.72);
        box-shadow:0 0 0 4px rgba(124,58,237,.14);
      }}
      .section-modal-footnote{{
        margin-top:2px;
        font-size:12px;
        color:var(--muted);
      }}
      .section-modal-actions{{
        margin-top:6px;
        display:flex;
        justify-content:flex-end;
        gap:10px;
        align-items:center;
        border-top:1px solid var(--border);
        padding-top:14px;
      }}
      @media (max-width: 640px){{
        .builder-modal{{padding:14px;}}
        .builder-modal-card{{padding:16px; border-radius:16px;}}
        .section-modal-head{{gap:10px;}}
        .section-modal-close{{width:34px; height:34px; border-radius:10px;}}
      }}
      @media (max-width: 980px){{
        .builder-container{{padding:18px 12px;}}
        .builder-shell{{grid-template-columns:1fr;}}
        .builder-kpis{{grid-template-columns:1fr;}}
        .builder-hero-top{{flex-direction:column;}}
        .builder-hero-actions{{justify-content:flex-start;}}
        .toolbar-actions{{justify-content:flex-start;}}
        .gf-toolbar{{
          position:static;
          margin-left:0;
          flex-direction:row;
          flex-wrap:wrap;
          width:100%;
          justify-content:flex-start;
        }}
      }}
      .drag-handle{{
        display:inline-block;
        cursor:grab;
        color:var(--muted);
        font-weight:800;
      }}
      .gf-list{{display:grid; gap:16px;}}
      .gf-card{{
        background:var(--surface);
        border:1px solid var(--border);
        border-radius:16px;
        padding:16px;
        box-shadow:0 12px 28px rgba(15,18,34,.06);
        border-left:4px solid var(--primary);
      }}
      .gf-card.dragging{{opacity:.6;}}
      .gf-card.editing{{box-shadow:0 18px 40px rgba(15,18,34,.14); outline:2px solid rgba(124,58,237,.35);}}
      .gf-card-head{{display:flex; justify-content:space-between; align-items:center; gap:12px;}}
      .gf-order{{display:flex; align-items:center; gap:10px;}}
      .order-pill{{display:inline-flex; align-items:center; justify-content:center; width:28px; height:28px; border-radius:8px; background:var(--primary-soft); color:var(--primary); font-weight:800;}}
      .gf-type select{{min-width:180px;}}
      .gf-question{{margin-top:12px; display:grid; gap:10px;}}
      .gf-question input.q-text{{font-size:16px; font-weight:600;}}
      .rich-text{{
        min-height:38px;
        padding:8px 10px;
        border:1px solid var(--border);
        border-radius:10px;
        background:var(--surface-2);
        font-size:16px;
        font-weight:600;
        outline:none;
      }}
      .rich-text:empty:before{{
        content: attr(data-placeholder);
        color: var(--muted);
      }}
      .rich-toolbar{{
        position: sticky;
        top: 96px;
        z-index: 40;
        display:flex;
        align-items:center;
        gap:8px;
        padding:8px 10px;
        border:1px solid var(--border);
        border-radius:12px;
        background:var(--surface);
        box-shadow:0 10px 24px rgba(15,18,34,.08);
      }}
      .rich-btn{{
        width:34px;
        height:34px;
        border-radius:10px;
        border:1px solid var(--border);
        background:var(--surface-2);
        font-weight:700;
        cursor:pointer;
      }}
      .q-hint{{font-size:12px; color:var(--muted);}}
      .q-preview{{margin-top:6px;}}
      .q-preview-input{{width:100%; padding:10px 12px; border:1px solid var(--border); border-radius:10px; background:var(--surface-2);}}
      .q-preview-options{{display:grid; gap:8px; align-items:start; justify-items:start; text-align:left;}}
      .q-option-line{{display:flex; align-items:center; gap:10px; padding:6px 8px; border-radius:8px;}}
      .q-option-line input{{margin-top:2px;}}
      .q-option-line span{{display:block; line-height:1.35; white-space:nowrap;}}
      .q-preview-options{{display:grid; gap:8px;}}
      .q-preview-options label{{display:flex; align-items:center; gap:8px; font-size:14px;}}
      .q-options{{margin-top:6px; display:grid; gap:8px;}}
      .q-options-label{{font-size:12px; font-weight:800; text-transform:uppercase; letter-spacing:.08em; color:var(--muted);}}
      .q-options-list{{display:grid; gap:8px;}}
      .q-option{{display:flex; align-items:center; gap:10px; padding:8px 10px; border:1px solid var(--border); border-radius:10px;}}
      .q-option-dot{{width:10px; height:10px; border-radius:50%; border:2px solid var(--primary);}}
      .q-option-input{{flex:1; border:none; background:transparent; padding:6px;}}
      .q-option-input:focus{{outline:none;}}
      .q-option-remove{{border:none; background:transparent; color:var(--muted); cursor:pointer;}}
      .gf-card-actions{{margin-top:14px; display:flex; justify-content:space-between; align-items:center; border-top:1px solid var(--border); padding-top:12px;}}
      .section-card{{border-left-color:rgba(59,130,246,.5);}}
      .media-card{{border-left-color:rgba(16,185,129,.5);}}
      .section-head{{display:flex; justify-content:space-between; align-items:center; gap:10px;}}
      .section-pill{{padding:4px 10px; border-radius:999px; background:var(--surface-2); border:1px solid var(--border); font-size:12px; font-weight:700;}}
      .section-title{{font-weight:700; font-size:16px; margin-top:10px;}}
      .section-desc{{color:var(--muted); margin-top:6px;}}
      .btn.btn-sm {{
        padding: 8px 12px;
        font-size: 12px;
        border-radius: 10px;
        font-weight: 600;
      }}
      .btn-muted {{
        color: var(--muted);
      }}
      .bulk-actions {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 12px;
      }}
      .switch{{
        position:relative;
        width:46px;
        height:26px;
      }}
      .switch input{{display:none;}}
      .switch .slider{{
        position:absolute;
        inset:0;
        background:var(--border);
        border-radius:999px;
        transition:all .2s ease;
      }}
      .switch .slider:before{{
        content:"";
        position:absolute;
        width:20px;
        height:20px;
        left:3px;
        top:3px;
        background:#fff;
        border-radius:50%;
        transition:all .2s ease;
        box-shadow:0 2px 6px rgba(0,0,0,.2);
      }}
      .switch input:checked + .slider{{
        background:var(--primary);
      }}
      .switch input:checked + .slider:before{{
        transform:translateX(20px);
      }}
    </style>

    <div class="builder-page">
      <div class="builder-container">
    <div class="builder-shell">
      <div class="builder-main">

      <div class="card builder-hero">
        <div class="builder-hero-top">
          <div>
            <div class="builder-kicker">Template workspace</div>
            <h1 class="h1 builder-title">Template Builder</h1>
            <div class="muted builder-sub">Configure your template, build questions, and share a link to enumerators.</div>
          </div>
          <div class="builder-hero-actions">
            <a class="btn" href="/ui/templates{key_q}">Back to Templates</a>
            <a class="btn btn-primary" href="{url_for('ui_template_share', template_id=template_id)}{key_q}">Share Link</a>
          </div>
        </div>
        <div class="builder-kpis">
          <div class="builder-kpi"><span class="k-label">Questions</span><span class="k-val">{len(q_cards)}</span></div>
          <div class="builder-kpi"><span class="k-label">Submissions</span><span class="k-val">{subs}</span></div>
          <div class="builder-kpi"><span class="k-label">Status</span><span class="k-val">{"Active" if int(cfg.get("is_active", 1)) == 1 else "Inactive"}</span></div>
        </div>
      </div>

      {"<div class='card' style='border-color: rgba(46, 204, 113, .35)'><b>Success:</b> " + msg + "</div>" if msg else ""}
      {"<div class='card' style='border-color: rgba(231, 76, 60, .35)'><b>Error:</b> " + err + "</div>" if err else ""}

      <div class="card" id="templateSettings">
        <div class="row settings-head">
          <div>
            <h2 class="h2">Template settings</h2>
            <div class="muted" style="margin-top:6px">Control enumerator requirements and verification options.</div>
          </div>
          <div class="settings-badges">
            {status_badge}
            {assignment_badge}
            <div style="font-weight:800">Submissions: <span style="color:var(--primary)">{subs}</span></div>
          </div>
        </div>

        <form method="POST" class="stack" style="margin-top:18px">
          <div>
            <label>Template name</label>
            <input name="name" value="{(cfg.get("name") or "")}" />
          </div>

          <div>
            <label>Description</label>
            <textarea name="description">{(cfg.get("description") or "")}</textarea>
            <div class="muted" style="margin-top:6px">Shown to enumerators at the top of the form (optional).</div>
          </div>

          <div>
            <label>Template version</label>
            <input name="template_version" value="{(cfg.get("template_version") or "v1")}" />
          </div>

          <div class="row">
            <label class="row" style="gap:10px">
              <input type="checkbox" name="is_active" style="width:auto" {"checked" if int(cfg.get("is_active", 1)) == 1 else ""}/>
              <span><b>Active</b> <span class="muted">(Share link works)</span></span>
            </label>
          </div>

          <div class="row">
            <label class="row" style="gap:10px">
              <input type="checkbox" name="require_enumerator_code" style="width:auto" {"checked" if int(cfg.get("require_enumerator_code", 0)) == 1 else ""}/>
              <span><b>Require enumerator code</b> <span class="muted">(recommended for controlled projects)</span></span>
            </label>
          </div>

          <div class="row">
            <label class="row" style="gap:10px">
              <input type="checkbox" name="collect_email" style="width:auto" {"checked" if int(cfg.get("collect_email", 0) or 0) == 1 else ""}/>
              <span><b>Collect respondent email</b> <span class="muted">(required field on the form)</span></span>
            </label>
          </div>

          <div class="row">
            <label class="row" style="gap:10px">
              <input type="checkbox" name="limit_one_response" style="width:auto" {"checked" if int(cfg.get("limit_one_response", 0) or 0) == 1 else ""}/>
              <span><b>Limit to 1 response per email</b> <span class="muted">(enforced on submit)</span></span>
            </label>
          </div>

          <div class="row">
            <label class="row" style="gap:10px">
              <input type="checkbox" name="allow_edit_response" style="width:auto" {"checked" if int(cfg.get("allow_edit_response", 0) or 0) == 1 else ""}/>
              <span><b>Allow edit after submit</b> <span class="muted">(shows “Edit response” link on success page)</span></span>
            </label>
          </div>

          <div class="row">
            <label class="row" style="gap:10px">
              <input type="checkbox" name="show_summary_charts" style="width:auto" {"checked" if int(cfg.get("show_summary_charts", 0) or 0) == 1 else ""}/>
              <span><b>Show summary charts to respondents</b> <span class="muted">(public response summary page)</span></span>
            </label>
          </div>

          <div class="row">
            <label class="row" style="gap:10px">
              <input type="checkbox" name="enable_gps" style="width:auto" {"checked" if int(cfg.get("enable_gps", 0)) == 1 else ""}/>
              <span><b>Enable GPS capture</b> <span class="muted">(optional field verification)</span></span>
            </label>
          </div>

          <div class="row">
            <label class="row" style="gap:10px">
              <input type="checkbox" name="enable_consent" style="width:auto" {"checked" if int(cfg.get("enable_consent", 0)) == 1 else ""}/>
              <span><b>Include consent question</b> <span class="muted">(adds a Yes/No consent field)</span></span>
            </label>
          </div>

          <div class="row">
            <label class="row" style="gap:10px">
              <input type="checkbox" name="enable_attestation" style="width:auto" {"checked" if int(cfg.get("enable_attestation", 0)) == 1 else ""}/>
              <span><b>Enumerator attestation</b> <span class="muted">(requires enumerator confirmation)</span></span>
            </label>
          </div>

          <div class="row">
            <label class="row" style="gap:10px">
              <input type="checkbox" name="is_sensitive" style="width:auto" {"checked" if int(cfg.get("is_sensitive", 0)) == 1 else ""}/>
              <span><b>Sensitive data</b> <span class="muted">(ethics review recommended)</span></span>
            </label>
          </div>

          <div class="row">
            <label class="row" style="gap:10px">
              <input type="checkbox" name="restricted_exports" style="width:auto" {"checked" if int(cfg.get("restricted_exports", 0)) == 1 else ""}/>
              <span><b>Restrict exports</b> <span class="muted">(redact responses in exports)</span></span>
            </label>
          </div>

          <div>
            <label>Redacted fields (optional)</label>
            <input name="redacted_fields" value="{(cfg.get("redacted_fields") or "")}" />
            <div class="muted" style="margin-top:6px">Comma-separated keywords or question IDs (e.g., name, phone, q_12).</div>
          </div>

          <div>
            <label>Confirmation message</label>
            <textarea name="confirmation_message" rows="3">{(cfg.get("confirmation_message") or "")}</textarea>
            <div class="muted" style="margin-top:6px">Shown to respondents after a successful submission.</div>
          </div>

          <div>
            <label>Assignment strictness</label>
            <select name="assignment_mode">
              <option value="INHERIT" {"selected" if (cfg.get("assignment_mode") or "INHERIT").upper() == "INHERIT" else ""}>Inherit project policy</option>
              <option value="OPTIONAL" {"selected" if (cfg.get("assignment_mode") or "").upper() == "OPTIONAL" else ""}>Assignment optional</option>
              <option value="REQUIRED_PROJECT" {"selected" if (cfg.get("assignment_mode") or "").upper() == "REQUIRED_PROJECT" else ""}>Assignment required</option>
              <option value="REQUIRED_TEMPLATE" {"selected" if (cfg.get("assignment_mode") or "").upper() == "REQUIRED_TEMPLATE" else ""}>Template assignment required</option>
            </select>
            <div class="muted" style="margin-top:6px">Overrides the project-wide assignment policy for this form.</div>
          </div>

          <div class="card share-card">
            <div style="font-weight:800; margin-bottom:6px">Enumerator share link</div>
            <div class="muted" style="margin-bottom:10px">Send this link to enumerators. They can submit responses without supervisor access.</div>
            <div class="share-url">
              {share_url}
            </div>
            <div class="row" style="margin-top:10px">
              <a class="btn" href="{url_for('ui_template_preview', template_id=template_id)}{key_q}">Preview Form</a>
              <a class="btn btn-primary" href="{url_for('ui_template_share', template_id=template_id)}{key_q}">Open Share Page</a>
              <form method="POST" style="margin:0">
                <input type="hidden" name="action" value="publish_template" />
                <button class="btn btn-primary" type="submit">Publish</button>
              </form>
            </div>
          </div>

          <div class="row">
            <button class="btn btn-primary" type="submit">Save settings</button>
          </div>
        </form>
      </div>

      <div class="card">
        <div class="row" style="justify-content:space-between">
          <div>
            <h2 class="h2">Questions</h2>
            <div class="muted" style="margin-top:6px">
              Edit directly in each question card. Change type, add options, and click Save.
            </div>
          </div>
          <div class="toolbar-actions">
            <a class="btn btn-primary btn-sm" data-add-question="1" href="{url_for('ui_question_add', template_id=template_id)}{key_q}">Add Question</a>
            <a class="btn btn-ghost btn-sm" href="{url_for('ui_import_text', template_id=template_id)}{key_q}">Import Text</a>
            <a class="btn btn-ghost btn-sm" href="{url_for('ui_import_docx', template_id=template_id)}{key_q}">Import .docx</a>
            <a class="btn btn-ghost btn-sm" href="{url_for('ui_import_pdf', template_id=template_id)}{key_q}">Import PDF</a>
          </div>
        </div>
        <div class="rich-toolbar" id="richToolbar" style="margin-top:14px;">
          <button class="rich-btn" type="button" data-rich="bold" title="Bold">B</button>
          <button class="rich-btn" type="button" data-rich="italic" title="Italic"><em>I</em></button>
          <button class="rich-btn" type="button" data-rich="link" title="Insert link">🔗</button>
          <button class="rich-btn" type="button" data-rich="clear" title="Clear formatting"><span style="text-decoration:line-through;">T</span></button>
        </div>
        <div class="gf-list" style="margin-top:16px;">
          {("".join(q_cards) if q_cards else "<div class='muted' style='padding:18px'>No questions yet. Click <b>Add Question</b> to start.</div>")}
        </div>
      </div>

      <div class="card danger-zone">
        <h2 class="h2">Archive / Delete</h2>
        <div class="muted" style="margin-top:6px">
          Archive to pause new submissions. Hard delete is only available if there are no submissions.
        </div>

        <div class="danger-actions">
          <a class="btn btn-sm" href="{url_for('ui_template_archive', template_id=template_id)}{key_q}"
             onclick="return confirm('Archive this template? Share link will stop working.')">
             📦 Archive
          </a>

          <a class="btn btn-sm" href="{url_for('ui_template_delete', template_id=template_id)}{key_q}">
             🧹 Soft delete
          </a>

          <a class="btn btn-sm" href="{url_for('ui_template_hard_delete', template_id=template_id)}{key_q}"
             onclick="return confirm('Hard delete? Only allowed if there are no submissions. Proceed?')">
             🗑️ Hard delete
          </a>
        </div>
      </div>

      </div>
      <div class="builder-modal" id="sectionModal">
        <div class="builder-modal-card section-modal-card">
          <div class="section-modal-head">
            <div>
              <div class="section-modal-kicker">Section builder</div>
              <div class="h2 section-modal-title" style="margin:0">Add section</div>
              <div class="muted section-modal-sub">Create a clean break in your form with a title and guidance text.</div>
            </div>
            <button class="section-modal-close" type="button" onclick="closeSectionModal()" aria-label="Close section dialog">✕</button>
          </div>

          <div class="section-modal-grid">
            <input type="hidden" id="sectionId" />
            <div class="section-modal-field">
              <label for="sectionTitle">Section title</label>
              <input id="sectionTitle" placeholder="Untitled section" />
            </div>
            <div class="section-modal-field">
              <label for="sectionDesc">Description (optional)</label>
              <textarea id="sectionDesc" rows="3" placeholder="Explain what this section is about"></textarea>
            </div>
            <div class="section-modal-footnote">Tip: keep section titles short and action-oriented for better completion rates.</div>
            <div class="section-modal-actions">
              <button class="btn" type="button" onclick="closeSectionModal()">Cancel</button>
              <button class="btn btn-primary" type="button" onclick="saveSection()">Save section</button>
            </div>
          </div>
        </div>
      </div>
      <div class="gf-toolbar" aria-label="Form tools">
        <a class="gf-tool" title="Add question" data-add-question="1" href="{url_for('ui_question_add', template_id=template_id)}{key_q}">＋</a>
        <a class="gf-tool" title="Import questions" href="{url_for('ui_import_text', template_id=template_id)}{key_q}">📄</a>
        <a class="gf-tool" title="Edit title & description" href="#templateSettings">Tt</a>
        <button class="gf-tool" type="button" title="Add image" onclick="addMediaBlock('image')">🖼️</button>
        <button class="gf-tool" type="button" title="Add video" onclick="addMediaBlock('video')">🎞️</button>
        <button class="gf-tool" type="button" title="Add section" onclick="addSectionMarker()">≡</button>
      </div>
    </div>
      </div>
    </div>

    <script>
      window.BUILDER_CONFIG = {{
        quickAddUrl: "{quick_add_url}",
        reorderUrl: "{url_for('ui_questions_reorder', template_id=template_id)}{key_q}",
        deleteUrlBase: "{url_for('ui_question_delete', template_id=template_id, question_id=0)}{key_q}",
        duplicateUrlBase: "{url_for('ui_question_duplicate', template_id=template_id, question_id=0)}{key_q}",
        inlineUpdateBase: "{url_for('ui_question_inline_update', template_id=template_id, question_id=0)}{key_q}",
        sectionAddUrl: "{url_for('ui_section_add', template_id=template_id)}{key_q}",
        sectionUpdateBase: "{url_for('ui_section_update', template_id=template_id, question_id=0)}{key_q}",
        mediaAddUrl: "{url_for('ui_media_add', template_id=template_id)}{key_q}",
        mediaUploadUrl: "{url_for('ui_media_upload', template_id=template_id)}{key_q}"
      }};
    </script>
    <script src="/static/builder.js"></script>
    """

    return ui_shell("Template Builder", page_html)


@app.route("/ui/templates/<int:template_id>/archive")
def ui_template_archive(template_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return ui_shell("Read-only", "<div class='card'><h2>Archived projects are read-only.</h2></div>"), 403
    tpl.set_template_config(template_id, is_active=0)
    return redirect(url_for("ui_template_manage", template_id=template_id) + key_qs())


@app.route("/ui/templates/<int:template_id>/delete", methods=["GET", "POST"])
def ui_template_delete(template_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return ui_shell("Read-only", "<div class='card'><h2>Archived projects are read-only.</h2></div>"), 403

    err = ""
    if request.method == "POST":
        confirm = (request.form.get("confirm") or "").strip().upper()
        if confirm != "DELETE":
            err = "Type DELETE to confirm."
        else:
            tpl.soft_delete_template(int(template_id))
            return redirect(url_for("ui_templates") + key_qs())

    return ui_shell(
        "Delete Template",
        render_template_string(
            """
            <div class="card">
              <h1 class="h1">Soft delete template</h1>
              <div class="muted">This hides the template and disables the share link without removing data.</div>
            </div>
            {% if err %}<div class="card" style="border-color: rgba(231, 76, 60, .35)"><b>Error:</b> {{err}}</div>{% endif %}
            <div class="card">
              <form method="POST" class="stack">
                <div>
                  <label style="font-weight:800">Confirm delete</label>
                  <input name="confirm" placeholder="Type DELETE to confirm" />
                </div>
                <div class="row">
                  <button class="btn btn-primary" type="submit">Delete template</button>
                  <a class="btn" href="{{ url_for('ui_template_manage', template_id=template_id) }}{{kq}}">Cancel</a>
                </div>
              </form>
            </div>
            """,
            template_id=template_id,
            err=err,
            kq=key_qs(),
        ),
    )


@app.route("/ui/templates/<int:template_id>/hard-delete", methods=["GET", "POST"])
def ui_template_hard_delete(template_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return ui_shell("Read-only", "<div class='card'><h2>Archived projects are read-only.</h2></div>"), 403

    subs = template_submissions_count(template_id)
    if subs > 0:
        return (
            render_template_string(
                """
                <h2>Hard delete blocked</h2>
                <p>This template already has submissions ({{subs}}). Hard delete is not allowed.</p>
                <p><a href="{{ url_for('ui_template_manage', template_id=template_id) }}{{kq}}">Back</a></p>
                """,
                subs=subs,
                template_id=template_id,
                kq=key_qs(),
            ),
            400,
        )

    err = ""
    if request.method == "POST":
        confirm = (request.form.get("confirm") or "").strip().upper()
        if confirm != "DELETE":
            err = "Type DELETE to confirm."
        else:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT id FROM template_questions WHERE template_id=?", (int(template_id),))
                qids = [int(r["id"]) for r in cur.fetchall()]
                if qids:
                    placeholders = ",".join(["?"] * len(qids))
                    if "template_question_id" in template_choices_cols():
                        cur.execute(
                            f"DELETE FROM template_question_choices WHERE template_question_id IN ({placeholders})",
                            qids,
                        )
                    cur.execute(
                        "DELETE FROM template_questions WHERE template_id=?", (int(template_id),))
                cur.execute("DELETE FROM survey_templates WHERE id=?",
                            (int(template_id),))
                conn.commit()
            return redirect(url_for("ui_templates") + key_qs())

    return ui_shell(
        "Hard Delete Template",
        render_template_string(
            """
            <div class="card">
              <h1 class="h1">Hard delete template</h1>
              <div class="muted">This permanently removes the template and its questions.</div>
            </div>
            {% if err %}<div class="card" style="border-color: rgba(231, 76, 60, .35)"><b>Error:</b> {{err}}</div>{% endif %}
            <div class="card">
              <form method="POST" class="stack">
                <div>
                  <label style="font-weight:800">Confirm delete</label>
                  <input name="confirm" placeholder="Type DELETE to confirm" />
                </div>
                <div class="row">
                  <button class="btn btn-primary" type="submit">Hard delete template</button>
                  <a class="btn" href="{{ url_for('ui_template_manage', template_id=template_id) }}{{kq}}">Cancel</a>
                </div>
              </form>
            </div>
            """,
            template_id=template_id,
            err=err,
            kq=key_qs(),
        ),
    )


# ---------------------------
# Question CRUD
# ---------------------------
def _normalize_template_question_order(template_id: int) -> None:
    cols = template_questions_cols()
    order_col = "display_order" if "display_order" in cols else (
        "order_no" if "order_no" in cols else "display_order")
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT id, {order_col} AS ord
            FROM template_questions
            WHERE template_id=?
            ORDER BY (ord IS NULL), ord, id
            """,
            (int(template_id),),
        )
        rows = cur.fetchall()
        for idx, r in enumerate(rows, start=1):
            cur.execute(
                f"UPDATE template_questions SET {order_col}=? WHERE id=? AND template_id=?",
                (int(idx), int(r["id"]), int(template_id)),
            )
        conn.commit()


def _shift_template_question_order(template_id: int, question_id: int, curr_order: int, new_order: int) -> int:
    cols = template_questions_cols()
    order_col = "display_order" if "display_order" in cols else (
        "order_no" if "order_no" in cols else "display_order")
    new_order = int(new_order or 1)
    curr_order = int(curr_order or 1)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM template_questions WHERE template_id=?", (int(template_id),))
        total = int(cur.fetchone()["c"] or 0)
        if total <= 0:
            return curr_order
        if new_order < 1:
            new_order = 1
        if new_order > total:
            new_order = total
        if new_order == curr_order:
            return new_order
        if new_order < curr_order:
            cur.execute(
                f"""
                UPDATE template_questions
                SET {order_col} = {order_col} + 1
                WHERE template_id=? AND {order_col}>=? AND {order_col}<? AND id<>?
                """,
                (int(template_id), int(new_order), int(curr_order), int(question_id)),
            )
        else:
            cur.execute(
                f"""
                UPDATE template_questions
                SET {order_col} = {order_col} - 1
                WHERE template_id=? AND {order_col}<=? AND {order_col}>? AND id<>?
                """,
                (int(template_id), int(new_order), int(curr_order), int(question_id)),
            )
        conn.commit()
    return new_order


def _shift_template_question_order_for_insert(template_id: int, new_order: int) -> None:
    cols = template_questions_cols()
    order_col = "display_order" if "display_order" in cols else (
        "order_no" if "order_no" in cols else "display_order")
    new_order = int(new_order or 1)
    if new_order < 1:
        new_order = 1
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            UPDATE template_questions
            SET {order_col} = {order_col} + 1
            WHERE template_id=? AND {order_col}>=?
            """,
            (int(template_id), int(new_order)),
        )
        conn.commit()


@app.route("/ui/templates/<int:template_id>/questions/add", methods=["GET", "POST"])
def ui_question_add(template_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return ui_shell("Read-only", "<div class='card'><h2>Archived projects are read-only.</h2></div>"), 403

    err = ""
    if request.method == "POST":
        try:
            qtext = request.form.get("question_text", "").strip()
            if not qtext:
                raise ValueError("Question text is required.")
            qtype = request.form.get("question_type", "TEXT").strip()
            is_required = 1 if request.form.get("is_required") == "on" else 0
            order_no = request.form.get("order_no", "").strip()
            order_no = int(order_no) if order_no else None
            help_text = (request.form.get("help_text") or "").strip()
            validation = {}
            min_len = (request.form.get("min_length") or "").strip()
            max_len = (request.form.get("max_length") or "").strip()
            min_val = (request.form.get("min_value") or "").strip()
            max_val = (request.form.get("max_value") or "").strip()
            pattern = (request.form.get("pattern") or "").strip()
            if min_len.isdigit():
                validation["min_length"] = int(min_len)
            if max_len.isdigit():
                validation["max_length"] = int(max_len)
            if min_val:
                try:
                    validation["min_value"] = float(min_val)
                except Exception:
                    pass
            if max_val:
                try:
                    validation["max_value"] = float(max_val)
                except Exception:
                    pass
            if pattern:
                validation["pattern"] = pattern
            validation_json = json.dumps(validation) if validation else None
            if order_no is not None:
                _shift_template_question_order_for_insert(int(template_id), int(order_no))
            tpl.add_template_question(
                template_id,
                qtext,
                qtype,
                order_no=order_no,
                is_required=is_required,
                help_text=help_text or None,
                validation_json=validation_json,
            )
            _normalize_template_question_order(int(template_id))
            return redirect(url_for("ui_template_manage", template_id=template_id) + key_qs())
        except Exception as e:
            err = str(e)

    return render_template_string(
        """
        <h2>Add Question</h2>
        <p><a href="{{ url_for('ui_template_manage', template_id=template_id) }}{{kq}}">Back</a></p>
        {% if err %}<p style="color:#b00"><b>Error:</b> {{err}}</p>{% endif %}
        <form method="POST">
          <label><b>Question text</b></label><br/>
          <input name="question_text" style="width:100%; padding:10px" />

          <br/><br/>
          <label><b>Type</b></label><br/>
          <select name="question_type" style="width:100%; padding:10px">
            <option>TEXT</option>
            <option>LONGTEXT</option>
            <option>YESNO</option>
            <option>NUMBER</option>
            <option>DATE</option>
            <option>EMAIL</option>
            <option>PHONE</option>
            <option>SINGLE_CHOICE</option>
            <option>DROPDOWN</option>
            <option>MULTI_CHOICE</option>
          </select>

          <br/><br/>
          <label><b>Order</b> (optional)</label><br/>
          <input name="order_no" type="number" style="width:100%; padding:10px" />

          <br/><br/>
          <label><input type="checkbox" name="is_required" /> Required</label>

          <br/><br/>
          <label><b>Help text</b> (optional)</label><br/>
          <textarea name="help_text" rows="2" style="width:100%; padding:10px"></textarea>

          <br/><br/>
          <label><b>Validation (optional)</b></label><br/>
          <div class="row" style="gap:10px">
            <input name="min_length" type="number" placeholder="Min length" style="flex:1; padding:10px" />
            <input name="max_length" type="number" placeholder="Max length" style="flex:1; padding:10px" />
          </div>
          <div class="row" style="gap:10px; margin-top:10px">
            <input name="min_value" type="number" step="any" placeholder="Min value" style="flex:1; padding:10px" />
            <input name="max_value" type="number" step="any" placeholder="Max value" style="flex:1; padding:10px" />
          </div>
          <div style="margin-top:10px">
            <input name="pattern" placeholder="Regex pattern (advanced)" style="width:100%; padding:10px" />
          </div>

          <br/><br/>
          <button type="submit" style="padding:10px 14px">Save</button>
        </form>
        """,
        template_id=template_id,
        kq=key_qs(),
        err=err,
    )


@app.route("/ui/templates/<int:template_id>/questions/<int:question_id>/edit", methods=["GET", "POST"])
def ui_question_edit(template_id, question_id):
    gate = admin_gate()
    if gate:
        return gate

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM template_questions WHERE id=? AND template_id=? LIMIT 1",
            (int(question_id), int(template_id)),
        )
        q = cur.fetchone()
    if not q:
        return render_template_string("<h2>Question not found</h2>"), 404
    q = row_to_dict(q)
    validation = _parse_validation_json(q.get("validation_json"))

    cols = template_questions_cols()
    order_col = "display_order" if "display_order" in cols else (
        "order_no" if "order_no" in cols else "display_order")
    curr_order = row_get(q, order_col, 1)

    err = ""
    if request.method == "POST":
        try:
            qtext = request.form.get("question_text", "").strip()
            if not qtext:
                raise ValueError("Question text is required.")
            qtype = request.form.get("question_type", "TEXT").strip().upper()
            is_required = 1 if request.form.get("is_required") == "on" else 0
            help_text = (request.form.get("help_text") or "").strip()
            vdata = {}
            min_len = (request.form.get("min_length") or "").strip()
            max_len = (request.form.get("max_length") or "").strip()
            min_val = (request.form.get("min_value") or "").strip()
            max_val = (request.form.get("max_value") or "").strip()
            pattern = (request.form.get("pattern") or "").strip()
            if min_len.isdigit():
                vdata["min_length"] = int(min_len)
            if max_len.isdigit():
                vdata["max_length"] = int(max_len)
            if min_val:
                try:
                    vdata["min_value"] = float(min_val)
                except Exception:
                    pass
            if max_val:
                try:
                    vdata["max_value"] = float(max_val)
                except Exception:
                    pass
            if pattern:
                vdata["pattern"] = pattern
            validation_json = json.dumps(vdata) if vdata else None

            order_val = request.form.get("order_no", "").strip()
            order_val = int(order_val) if order_val else int(curr_order)

            if int(order_val) != int(curr_order):
                order_val = _shift_template_question_order(int(template_id), int(question_id), int(curr_order), int(order_val))

            with get_conn() as conn:
                cur = conn.cursor()
                cols = template_questions_cols()
                fields = [f"question_text=?", "question_type=?", "is_required=?", f"{order_col}=?"]
                values = [qtext, qtype, is_required, int(order_val)]
                if "help_text" in cols:
                    fields.append("help_text=?")
                    values.append(help_text or None)
                if "validation_json" in cols:
                    fields.append("validation_json=?")
                    values.append(validation_json)
                values.extend([int(question_id), int(template_id)])
                cur.execute(
                    f"""
                    UPDATE template_questions
                    SET {", ".join(fields)}
                    WHERE id=? AND template_id=?
                    """,
                    tuple(values),
                )
                conn.commit()

            _normalize_template_question_order(int(template_id))
            return redirect(url_for("ui_template_manage", template_id=template_id) + key_qs())
        except Exception as e:
            err = str(e)

    html_page = render_template_string(
        """
        <style>
          .q-edit-hero{background:linear-gradient(135deg, rgba(124,58,237,.14), rgba(124,58,237,.04)); border-radius:18px; padding:18px; border:1px solid rgba(124,58,237,.12);}
          .q-edit-grid{display:grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap:16px;}
          .q-edit-card{border-radius:16px; border:1px solid rgba(124,58,237,.12); background:var(--surface); padding:16px; box-shadow:0 14px 28px rgba(15,18,34,.06);}
          .q-edit-label{font-weight:800; font-size:12px; text-transform:uppercase; letter-spacing:.08em; color:var(--muted);}
          .q-pill{display:inline-flex; align-items:center; gap:6px; padding:4px 10px; border-radius:999px; background:var(--primary-soft); color:var(--primary); font-size:11px; font-weight:700; border:1px solid rgba(124,58,237,.2);}
        </style>
        <div class="card q-edit-hero">
          <div class="row" style="justify-content:space-between; align-items:center;">
            <div>
              <h1 class="h1" style="margin:0">Edit question</h1>
              <div class="muted">Refine wording, input type, and validation rules.</div>
            </div>
            <div class="row" style="gap:8px;">
              <a class="btn" href="{{ url_for('ui_template_manage', template_id=template_id) }}{{kq}}">Back to template</a>
              <span class="q-pill">Template #{{template_id}}</span>
            </div>
          </div>
        </div>
        {% if err %}<div class="card" style="border-color: rgba(231, 76, 60, .35)"><b>Error:</b> {{err}}</div>{% endif %}

        <form method="POST" class="q-edit-grid" style="margin-top:16px;">
          <div class="q-edit-card" style="grid-column:1 / -1;">
            <div class="q-edit-label">Question text</div>
            <input name="question_text" value="{{q.get('question_text','')}}" placeholder="Type your question..." />
          </div>

          <div class="q-edit-card">
            <div class="q-edit-label">Type</div>
            <select name="question_type">
              {% for t in types %}
                <option value="{{t}}" {% if (q.get('question_type','TEXT')|upper)==t %}selected{% endif %}>{{t}}</option>
              {% endfor %}
            </select>
          </div>

          <div class="q-edit-card">
            <div class="q-edit-label">Order</div>
            <input name="order_no" type="number" value="{{curr_order}}" />
            <div class="muted" style="margin-top:6px;">Lower numbers appear first.</div>
          </div>

          <div class="q-edit-card">
            <div class="q-edit-label">Required</div>
            <label class="row" style="gap:10px; margin-top:10px;">
              <input type="checkbox" name="is_required" {% if (q.get('is_required',0)|int)==1 %}checked{% endif %} />
              <span>Field must be answered</span>
            </label>
          </div>

          <div class="q-edit-card" style="grid-column:1 / -1;">
            <div class="q-edit-label">Help text (optional)</div>
            <textarea name="help_text" rows="2" placeholder="Add helper guidance for enumerators">{{ q.get('help_text','') }}</textarea>
          </div>

          <div class="q-edit-card" style="grid-column:1 / -1;">
            <div class="q-edit-label">Validation (optional)</div>
            <div class="q-edit-grid" style="margin-top:10px;">
              <input name="min_length" type="number" placeholder="Min length" value="{{ validation.get('min_length','') }}" />
              <input name="max_length" type="number" placeholder="Max length" value="{{ validation.get('max_length','') }}" />
              <input name="min_value" type="number" step="any" placeholder="Min value" value="{{ validation.get('min_value','') }}" />
              <input name="max_value" type="number" step="any" placeholder="Max value" value="{{ validation.get('max_value','') }}" />
              <input name="pattern" placeholder="Regex pattern (advanced)" value="{{ validation.get('pattern','') }}" />
            </div>
          </div>

          <div class="q-edit-card" style="grid-column:1 / -1;">
            <div class="row" style="justify-content:flex-end; gap:10px;">
              <a class="btn" href="{{ url_for('ui_template_manage', template_id=template_id) }}{{kq}}">Cancel</a>
              <button class="btn btn-primary" type="submit">Save changes</button>
            </div>
          </div>
        </form>
        """,
        template_id=template_id,
        question_id=question_id,
        kq=key_qs(),
        err=err,
        q=q,
        validation=validation,
        curr_order=curr_order,
        types=["TEXT", "LONGTEXT", "YESNO", "NUMBER", "DATE", "EMAIL",
               "PHONE", "SINGLE_CHOICE", "DROPDOWN", "MULTI_CHOICE"],
    )
    return ui_shell("Edit Question", html_page, show_project_switcher=False)


@app.route("/ui/templates/<int:template_id>/questions/<int:question_id>/inline-update", methods=["POST"])
def ui_question_inline_update(template_id, question_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return jsonify({"ok": False, "error": "Read-only"}), 403

    data = request.get_json(silent=True) or {}
    qtext = (data.get("question_text") or "").strip()
    qtype = (data.get("question_type") or "TEXT").strip().upper()
    is_required = 1 if bool(data.get("is_required")) else 0
    choices = data.get("choices", None)

    if not qtext:
        return jsonify({"ok": False, "error": "Question text is required."}), 400

    allowed = {"TEXT", "LONGTEXT", "YESNO", "NUMBER", "DATE", "EMAIL", "PHONE", "SINGLE_CHOICE", "DROPDOWN", "MULTI_CHOICE"}
    if qtype not in allowed:
        return jsonify({"ok": False, "error": "Invalid question type."}), 400

    with get_conn() as conn:
        conn.execute(
            "UPDATE template_questions SET question_text=?, question_type=?, is_required=? WHERE id=? AND template_id=?",
            (qtext, qtype, int(is_required), int(question_id), int(template_id)),
        )
        conn.commit()

    if choices is not None:
        if not isinstance(choices, list):
            return jsonify({"ok": False, "error": "Invalid choices format."}), 400
        normalized = [str(c).strip() for c in choices if str(c).strip()]
        if qtype == "YESNO" and not normalized:
            normalized = ["Yes", "No"]

        with get_conn() as conn:
            cur = conn.cursor()
            if "template_question_id" in template_choices_cols():
                cur.execute(
                    "DELETE FROM template_question_choices WHERE template_question_id=?",
                    (int(question_id),),
                )
            conn.commit()
        if qtype in ("SINGLE_CHOICE", "DROPDOWN", "MULTI_CHOICE", "YESNO"):
            for c in normalized:
                tpl.add_choice(int(question_id), c)

    return jsonify({"ok": True})


@app.route("/ui/templates/<int:template_id>/questions/<int:question_id>/duplicate")
def ui_question_duplicate(template_id, question_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return ui_shell("Read-only", "<div class='card'><h2>Archived projects are read-only.</h2></div>"), 403

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM template_questions WHERE id=? AND template_id=? LIMIT 1",
            (int(question_id), int(template_id)),
        )
        q = cur.fetchone()
    if not q:
        return ui_shell("Not found", "<div class='card'><h2>Question not found.</h2></div>"), 404
    q = row_to_dict(q)

    cols = template_questions_cols()
    order_col = "display_order" if "display_order" in cols else ("order_no" if "order_no" in cols else "display_order")
    curr_order = row_get(q, order_col, 1)
    new_order = int(curr_order) + 1
    _shift_template_question_order_for_insert(int(template_id), int(new_order))

    new_qid = tpl.add_template_question(
        int(template_id),
        q.get("question_text") or "Untitled Question",
        (q.get("question_type") or "TEXT").upper(),
        order_no=int(new_order),
        is_required=int(q.get("is_required") or 0),
        help_text=q.get("help_text") or None,
        validation_json=q.get("validation_json") or None,
    )

    if (q.get("question_type") or "").upper() in ("SINGLE_CHOICE", "DROPDOWN", "MULTI_CHOICE", "YESNO"):
        for c in q_choices(int(question_id)):
            tpl.add_choice(int(new_qid), c[2])

    _normalize_template_question_order(int(template_id))
    return redirect(url_for("ui_template_manage", template_id=template_id) + key_qs())

@app.route("/ui/templates/<int:template_id>/questions/<int:question_id>/delete")
def ui_question_delete(template_id, question_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return ui_shell("Read-only", "<div class='card'><h2>Archived projects are read-only.</h2></div>"), 403

    with get_conn() as conn:
        cur = conn.cursor()
        if "template_question_id" in template_choices_cols():
            cur.execute(
                "DELETE FROM template_question_choices WHERE template_question_id=?", (int(question_id),))
        cur.execute("DELETE FROM template_questions WHERE id=? AND template_id=?", (int(
            question_id), int(template_id)))
        conn.commit()

    _normalize_template_question_order(int(template_id))
    return redirect(url_for("ui_template_manage", template_id=template_id) + key_qs())


@app.route("/ui/templates/<int:template_id>/questions/bulk-delete", methods=["POST"])
def ui_questions_bulk_delete(template_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return ui_shell("Read-only", "<div class='card'><h2>Archived projects are read-only.</h2></div>"), 403

    subs = template_submissions_count(template_id)
    if subs > 0:
        return (
            render_template_string(
                """
                <h2>Bulk delete blocked</h2>
                <p>This template already has submissions ({{subs}}). Bulk delete is disabled.</p>
                <p><a href="{{ url_for('ui_template_manage', template_id=template_id) }}{{kq}}">Back</a></p>
                """,
                subs=subs,
                template_id=template_id,
                kq=key_qs(),
            ),
            400,
        )

    qids = request.form.getlist("qids")
    qids = [int(q) for q in qids if str(q).strip().isdigit()]
    if not qids:
        return redirect(url_for("ui_template_manage", template_id=template_id) + key_qs())

    with get_conn() as conn:
        cur = conn.cursor()
        placeholders = ",".join(["?"] * len(qids))
        if "template_question_id" in template_choices_cols():
            cur.execute(
                f"DELETE FROM template_question_choices WHERE template_question_id IN ({placeholders})",
                tuple(qids),
            )
        cur.execute(
            f"DELETE FROM template_questions WHERE id IN ({placeholders}) AND template_id=?",
            tuple(qids) + (int(template_id),),
        )
        conn.commit()

    _normalize_template_question_order(int(template_id))
    return redirect(url_for("ui_template_manage", template_id=template_id) + key_qs())


@app.route("/ui/templates/<int:template_id>/questions/reorder", methods=["POST"])
def ui_questions_reorder(template_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return jsonify({"ok": False, "error": "Read-only"}), 403
    data = request.get_json(silent=True) or {}
    order = data.get("order") or []
    if not isinstance(order, list):
        return jsonify({"ok": False, "error": "Invalid payload"}), 400
    cols = template_questions_cols()
    order_col = "display_order" if "display_order" in cols else ("order_no" if "order_no" in cols else "display_order")
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            for idx, qid in enumerate(order, start=1):
                if not str(qid).isdigit():
                    continue
                cur.execute(
                    f"UPDATE template_questions SET {order_col}=? WHERE id=? AND template_id=?",
                    (int(idx), int(qid), int(template_id)),
                )
            conn.commit()
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    try:
        _normalize_template_question_order(int(template_id))
    except Exception:
        pass
    return jsonify({"ok": True})


@app.route("/ui/templates/<int:template_id>/questions/quick-add", methods=["GET", "POST"])
def ui_question_quick_add(template_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return jsonify({"ok": False, "error": "Read-only"}), 403
    if request.method == "GET":
        try:
            qid = tpl.add_template_question(int(template_id), "Untitled Question", "TEXT", is_required=0)
            _normalize_template_question_order(int(template_id))
            return redirect(url_for("ui_template_manage", template_id=template_id) + key_qs())
        except Exception as e:
            return ui_shell("Add Question", f"<div class='card'><h2>Add failed</h2><div class='muted'>{html.escape(str(e))}</div></div>")
    data = request.get_json(silent=True) or {}
    qtext = (data.get("question_text") or "").strip()
    qtype = (data.get("question_type") or "TEXT").strip().upper()
    is_required = 1 if data.get("is_required") else 0
    choices = data.get("choices") or []
    if not qtext:
        return jsonify({"ok": False, "error": "Question text is required."}), 400
    allowed = {"TEXT","LONGTEXT","YESNO","NUMBER","DATE","EMAIL","PHONE","SINGLE_CHOICE","DROPDOWN","MULTI_CHOICE"}
    if qtype not in allowed:
        return jsonify({"ok": False, "error": "Invalid question type."}), 400
    try:
        qid = tpl.add_template_question(int(template_id), qtext, qtype, is_required=is_required)
        if qtype in ("SINGLE_CHOICE","DROPDOWN","MULTI_CHOICE"):
            for c in choices:
                ctext = (c or "").strip()
                if ctext:
                    tpl.add_choice(int(qid), ctext)
        _normalize_template_question_order(int(template_id))
        return jsonify({"ok": True, "question_id": qid})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/ui/templates/<int:template_id>/sections/add", methods=["POST"])
def ui_section_add(template_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return ui_shell("Read-only", "<div class='card'><h2>Archived projects are read-only.</h2></div>"), 403
    title = (request.form.get("section_title") or "").strip()
    desc = (request.form.get("section_desc") or "").strip()
    if not title:
        return redirect(url_for("ui_template_manage", template_id=template_id) + key_qs())
    marker = f"[SECTION] {title}"
    if desc:
        marker = f"{marker} | {desc}"
    try:
        tpl.add_template_question(int(template_id), marker, "TEXT", is_required=0)
    except Exception:
        pass
    return redirect(url_for("ui_template_manage", template_id=template_id) + key_qs())


@app.route("/ui/templates/<int:template_id>/sections/<int:question_id>/update", methods=["POST"])
def ui_section_update(template_id, question_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return ui_shell("Read-only", "<div class='card'><h2>Archived projects are read-only.</h2></div>"), 403
    title = (request.form.get("section_title") or "").strip()
    desc = (request.form.get("section_desc") or "").strip()
    if not title:
        return redirect(url_for("ui_template_manage", template_id=template_id) + key_qs())
    marker = f"[SECTION] {title}"
    if desc:
        marker = f"{marker} | {desc}"
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE template_questions SET question_text=? WHERE id=? AND template_id=?",
                (marker, int(question_id), int(template_id)),
            )
            conn.commit()
    except Exception:
        pass
    return redirect(url_for("ui_template_manage", template_id=template_id) + key_qs())


@app.route("/ui/templates/<int:template_id>/media/add", methods=["POST"])
def ui_media_add(template_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return ui_shell("Read-only", "<div class='card'><h2>Archived projects are read-only.</h2></div>"), 403
    media_type = (request.form.get("media_type") or "").strip().lower()
    media_url = (request.form.get("media_url") or "").strip()
    media_caption = (request.form.get("media_caption") or "").strip()
    if media_type not in ("image", "video") or not media_url:
        return redirect(url_for("ui_template_manage", template_id=template_id) + key_qs())
    prefix = "[IMAGE]" if media_type == "image" else "[VIDEO]"
    marker = f"{prefix} {media_url}"
    if media_caption:
        marker = f"{marker} | {media_caption}"
    try:
        tpl.add_template_question(int(template_id), marker, "TEXT", is_required=0)
    except Exception:
        pass
    return redirect(url_for("ui_template_manage", template_id=template_id) + key_qs())


@app.route("/ui/templates/<int:template_id>/media/upload", methods=["POST"])
def ui_media_upload(template_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return jsonify({"ok": False, "error": "Read-only"}), 403
    if "media" not in request.files:
        return jsonify({"ok": False, "error": "No file uploaded"}), 400
    f = request.files["media"]
    if not f.filename:
        return jsonify({"ok": False, "error": "Invalid filename"}), 400
    filename = secure_filename(f.filename)
    ext = os.path.splitext(filename)[1].lower()
    if ext not in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".mp4", ".webm", ".ogg"):
        return jsonify({"ok": False, "error": "Unsupported file type"}), 400
    token = secrets.token_urlsafe(8)
    stored = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{token}{ext}"
    path = os.path.join(UPLOAD_DIR, stored)
    try:
        f.save(path)
    except Exception:
        return jsonify({"ok": False, "error": "Upload failed"}), 500
    url = url_for("serve_upload", filename=stored)
    return jsonify({"ok": True, "url": url})


# ---------------------------
# Choices UI
# ---------------------------
@app.route("/ui/templates/<int:template_id>/questions/<int:question_id>/choices", methods=["GET", "POST"])
def ui_choices_manage(template_id, question_id):
    gate = admin_gate()
    if gate:
        return gate

    err = ""
    if request.method == "POST":
        try:
            action = (request.form.get("action") or "").strip()
            if action == "update_type":
                qtype = (request.form.get("question_type") or "TEXT").strip().upper()
                allowed = {"TEXT", "LONGTEXT", "YESNO", "NUMBER", "DATE", "EMAIL", "PHONE", "SINGLE_CHOICE", "DROPDOWN", "MULTI_CHOICE"}
                if qtype not in allowed:
                    raise ValueError("Invalid question type.")
                with get_conn() as conn:
                    conn.execute(
                        "UPDATE template_questions SET question_type=? WHERE id=? AND template_id=?",
                        (qtype, int(question_id), int(template_id)),
                    )
                    conn.commit()
            elif action == "update_choices":
                choices_map = {}
                for key, val in request.form.items():
                    if key.startswith("choice_") and key[len("choice_"):].isdigit():
                        choices_map[int(key[len("choice_"):])] = (val or "").strip()
                if not choices_map:
                    raise ValueError("No choices to update.")
                with get_conn() as conn:
                    cur = conn.cursor()
                    for cid, text in choices_map.items():
                        if not text:
                            continue
                        cur.execute(
                            "UPDATE template_question_choices SET choice_text=? WHERE id=? AND template_question_id=?",
                            (text, int(cid), int(question_id)),
                        )
                    conn.commit()
            else:
                choice_text = request.form.get("choice_text", "").strip()
                if not choice_text:
                    raise ValueError("Option text is required.")
                tpl.add_choice(question_id, choice_text)
        except Exception as e:
            err = str(e)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM template_questions WHERE id=? AND template_id=? LIMIT 1",
                    (int(question_id), int(template_id)))
        q = cur.fetchone()
    if not q:
        return render_template_string("<h2>Question not found</h2>"), 404
    q = row_to_dict(q)

    choices = q_choices(question_id)
    rows = []
    for c in choices:
        cid = c[0]
        ctext = c[2]
        rows.append(
            f"""
            <div class="choice-item">
              <input name="choice_{cid}" value="{html.escape(ctext)}" />
              <a class="btn btn-sm btn-danger" href='{url_for('ui_choice_delete', template_id=template_id, question_id=question_id, choice_id=cid)}{key_qs()}' 
                 onclick="return confirm('Delete this option?')" aria-label="Delete option">🗑</a>
            </div>
            """
        )

    return render_template_string(
        """
        <style>
          .choices-shell{display:grid; grid-template-columns:minmax(0,1.5fr) minmax(0,.7fr); gap:18px;}
          .choices-hero{background:linear-gradient(135deg, rgba(124,58,237,.16), rgba(124,58,237,.04)); border-radius:20px; padding:18px; border:1px solid rgba(124,58,237,.18);}
          .choices-card{border-radius:18px; border:1px solid rgba(124,58,237,.12); background:var(--surface); padding:16px; box-shadow:0 12px 26px rgba(15,18,34,.06);}
          .choices-label{font-weight:800; font-size:12px; text-transform:uppercase; letter-spacing:.08em; color:var(--muted);}
          .choices-side{display:grid; gap:16px;}
          .choice-meta{font-size:12px; color:var(--muted);}
          .choice-list{display:grid; gap:10px; margin-top:10px;}
          .choice-item{display:flex; gap:10px; align-items:center; padding:10px; border:1px solid var(--border); border-radius:12px; background:var(--surface);}
          .choice-item input{flex:1; border:none; background:transparent; padding:8px; font-weight:600;}
          .choice-item input:focus{outline:none; border:none; box-shadow:none;}
          .btn-danger{color:#b91c1c; border-color:rgba(185,28,28,.3); background:rgba(185,28,28,.08);}
          .sticky-side{position:sticky; top:110px;}
          .save-pill{display:inline-flex; align-items:center; gap:8px; padding:6px 10px; border-radius:999px; background:var(--primary-soft); color:var(--primary); font-size:11px; font-weight:700; border:1px solid rgba(124,58,237,.2);}
          @media (max-width: 900px){.choices-shell{grid-template-columns:1fr;}}
        </style>
        <div class="card choices-hero">
          <div class="row" style="justify-content:space-between; align-items:center;">
            <div>
              <h1 class="h1" style="margin:0">Choices</h1>
              <div class="muted">Edit question type and options in one focused panel.</div>
              <div class="choice-meta" style="margin-top:10px;"><b>Question:</b> {{q.get('question_text')}}</div>
            </div>
            <div class="row" style="gap:8px;">
              <span class="save-pill">{{ q.get('question_type','TEXT')|upper }}</span>
              <a class="btn" href="{{ url_for('ui_template_manage', template_id=template_id) }}{{kq}}">Back</a>
            </div>
          </div>
        </div>

        {% if err %}<div class="card" style="border-color: rgba(231, 76, 60, .35)"><b>Error:</b> {{err}}</div>{% endif %}

        <div class="choices-shell" style="margin-top:16px;">
          <div class="choices-card">
            <div class="row" style="justify-content:space-between; align-items:center;">
              <div>
                <h3 style="margin:0">Options</h3>
                <div class="muted">Click any option to edit it, then save.</div>
              </div>
              <button class="btn btn-primary" type="button" onclick="document.getElementById('choicesForm').submit()">Save changes</button>
            </div>
            <form id="choicesForm" method="POST" style="margin-top:12px;">
              <input type="hidden" name="action" value="update_choices" />
              <div class="choice-list">
                {% if rows %}
                  {{rows|safe}}
                {% else %}
                  <div class="muted" style="padding:12px 0">No options yet.</div>
                {% endif %}
              </div>
            </form>
          </div>

          <div class="choices-side sticky-side">
            <div class="choices-card">
              <div class="choices-label">Question type</div>
              <form method="POST" class="stack" style="margin-top:10px;">
                <input type="hidden" name="action" value="update_type" />
                <select name="question_type">
                  {% for t in types %}
                    <option value="{{t}}" {% if (q.get('question_type','TEXT')|upper)==t %}selected{% endif %}>{{t}}</option>
                  {% endfor %}
                </select>
                <button class="btn btn-primary" type="submit">Update type</button>
              </form>
              <div class="choice-meta" style="margin-top:8px;">
                Use Single choice / Dropdown / Multi choice to show options.
              </div>
            </div>

            <div class="choices-card">
              <div class="choices-label">Add new option</div>
              <form method="POST" class="stack" style="margin-top:10px;">
                <input type="hidden" name="action" value="add_choice" />
                <input name="choice_text" placeholder="Option text" />
                <button class="btn btn-primary" type="submit">Add option</button>
              </form>
            </div>
          </div>
        </div>
        """,
        template_id=template_id,
        question_id=question_id,
        kq=key_qs(),
        err=err,
        q=q,
        types=["TEXT", "LONGTEXT", "YESNO", "NUMBER", "DATE", "EMAIL",
               "PHONE", "SINGLE_CHOICE", "DROPDOWN", "MULTI_CHOICE"],
        rows="".join(
            rows) if rows else "<tr><td style='padding:10px' colspan='2'>No options yet.</td></tr>",
    )


@app.route("/ui/templates/<int:template_id>/questions/<int:question_id>/choices/<int:choice_id>/delete")
def ui_choice_delete(template_id, question_id, choice_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return ui_shell("Read-only", "<div class='card'><h2>Archived projects are read-only.</h2></div>"), 403

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM template_question_choices WHERE id=? AND template_question_id=?",
            (int(choice_id), int(question_id)),
        )
        conn.commit()

    return redirect(url_for("ui_choices_manage", template_id=template_id, question_id=question_id) + key_qs())


# ---------------------------
# Imports (Paste text / DOCX / PDF)
# ---------------------------
def _select_import_items(items, keep_list):
    keep = []
    for k in keep_list or []:
        if str(k).isdigit():
            keep.append(int(k))
    if not keep:
        return []
    keep_set = set(keep)
    return [item for idx, item in enumerate(items) if idx in keep_set]

@app.route("/ui/templates/<int:template_id>/import/text", methods=["GET", "POST"])
def ui_import_text(template_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return ui_shell("Read-only", "<div class='card'><h2>Archived projects are read-only.</h2></div>"), 403

    msg = ""
    err = ""
    preview_items = []
    raw_text = ""
    default_required = 0
    redirect_url = ""

    if request.method == "POST":
        try:
            stage = (request.form.get("stage") or "preview").strip().lower()
            raw_text = request.form.get("raw_text", "")
            default_required = 1 if request.form.get("default_required") in ("on", "1", "true") else 0

            if stage == "confirm":
                items = tpl.parse_questions_from_text(raw_text)
                keep = request.form.getlist("keep")
                items = _select_import_items(items, keep)
                if not items:
                    raise ValueError("No questions selected.")
                res = tpl.import_questions_from_items(
                    template_id, items, default_required=default_required
                )
                msg = f"Imported {res.get('added', 0)} question(s)."
                redirect_url = url_for("ui_template_manage", template_id=template_id) + key_qs()
                if res.get("errors"):
                    err = " | ".join(res["errors"][:5])
            else:
                if not raw_text.strip():
                    err = "Paste some text to preview."
                else:
                    preview_items = tpl.parse_questions_from_text(raw_text)
                    if not preview_items:
                        err = "No questions detected. Check formatting."
        except Exception as e:
            err = str(e)

    return render_template_string(
        """
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Poppins:wght@400;500;600;700;800&display=swap" rel="stylesheet">
        <style>
          :root{
            --surface:#ffffff;
            --surface-2:#f7f7fc;
            --text:#0f1222;
            --muted:#5f6470;
            --border:rgba(15,18,34,.12);
            --shadow:0 16px 40px rgba(15,18,34,.08);
            --primary:#8E5CFF;
            --primary-500:#7C3AED;
            --font-heading:"Poppins", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
            --font-body:"Inter", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
          }
          body{
            background:radial-gradient(900px 380px at 20% -10%, rgba(124,58,237,.12), transparent 60%), #fff;
            font-family:var(--font-body);
          }
          .import-wrap{max-width:980px; margin:0 auto; padding:24px 20px 64px;}
          .import-hero{
            background:
              radial-gradient(140px 120px at 15% 15%, rgba(124,58,237,.35), transparent 60%),
              linear-gradient(135deg, rgba(124,58,237,.22), rgba(15,18,34,.04));
            border:1px solid rgba(124,58,237,.35);
            border-radius:20px;
            padding:20px;
            box-shadow:var(--shadow);
          }
          .import-title{margin:0; font-size:24px; font-weight:800; letter-spacing:-.2px; color:var(--text); font-family:var(--font-heading);}
          .import-sub{margin-top:6px; color:var(--muted); line-height:1.6;}
          .import-card{
            margin-top:16px;
            background:var(--surface);
            border:1px solid var(--border);
            border-radius:18px;
            padding:20px;
            box-shadow:var(--shadow);
          }
          .import-note{
            display:flex;
            gap:10px;
            align-items:flex-start;
            background:var(--surface-2);
            border:1px dashed var(--border);
            border-radius:14px;
            padding:12px;
            color:var(--muted);
            font-size:13px;
          }
          .import-note code{background:#fff; padding:2px 6px; border-radius:8px; border:1px solid var(--border);}
          textarea{
            width:100%;
            min-height:220px;
            border-radius:14px;
            border:1px solid var(--border);
            padding:14px;
            font-size:14px;
            font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
            background:#fff;
          }
          .import-actions{display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-top:14px;}
          .import-tabs{display:flex; gap:8px; margin-top:10px; flex-wrap:wrap;}
          .import-tab{
            padding:6px 12px;
            border-radius:999px;
            border:1px solid var(--border);
            background:#fff;
            font-weight:600;
            font-size:12px;
            color:var(--muted);
            text-decoration:none;
          }
          .import-tab.active{
            color:#4c1d95;
            border-color:rgba(124,58,237,.35);
            background:rgba(124,58,237,.12);
          }
          .btn{
            padding:10px 14px;
            border-radius:12px;
            border:1px solid var(--border);
            background:#fff;
            font-weight:600;
            cursor:pointer;
          }
          .btn.primary{
            background:linear-gradient(135deg, var(--primary), var(--primary-500));
            color:#fff;
            border:none;
            box-shadow:0 10px 24px rgba(124,58,237,.25);
          }
          .status{
            margin-top:12px;
            padding:10px 12px;
            border-radius:12px;
            font-weight:600;
          }
          .status.ok{background:rgba(46,204,113,.12); color:#0a7a3e; border:1px solid rgba(46,204,113,.25);}
          .status.err{background:rgba(231,76,60,.12); color:#b00; border:1px solid rgba(231,76,60,.25);}
          .toast{
            position:fixed;
            right:18px;
            top:18px;
            padding:12px 16px;
            border-radius:12px;
            background:#1f2937;
            color:#fff;
            box-shadow:0 16px 40px rgba(15,18,34,.2);
            font-weight:700;
            z-index:9999;
            opacity:0;
            transform:translateY(-6px);
            transition:opacity .2s ease, transform .2s ease;
          }
          .toast.show{opacity:1; transform:translateY(0);}
          .back-link{color:var(--muted); text-decoration:none; font-weight:600; display:inline-block; margin-bottom:8px;}
          .preview-list{display:flex; flex-direction:column; gap:12px; margin-top:12px;}
          .preview-item{
            display:flex;
            gap:12px;
            padding:12px;
            border:1px solid var(--border);
            border-radius:12px;
            background:#fff;
          }
          .preview-item input{margin-top:4px;}
          .preview-q{font-weight:700; color:var(--text);}
          .preview-meta{font-size:12px; color:var(--muted); margin-top:4px;}
          .preview-choices{font-size:12px; color:var(--muted); margin-top:6px;}
        </style>

        <div class="import-wrap">
          <div class="import-hero">
            <a class="back-link" href="{{ url_for('ui_template_manage', template_id=template_id) }}{{kq}}">← Back to Template Builder</a>
            <h2 class="import-title">Import Questions (Paste Text)</h2>
            <div class="import-sub">Paste one question per line and we'll build the form instantly.</div>
            <div class="import-tabs">
              <a class="import-tab active" href="{{ url_for('ui_import_text', template_id=template_id) }}{{kq}}">Text</a>
              <a class="import-tab" href="{{ url_for('ui_import_docx', template_id=template_id) }}{{kq}}">Word</a>
              <a class="import-tab" href="{{ url_for('ui_import_pdf', template_id=template_id) }}{{kq}}">PDF</a>
            </div>
          </div>

          <div class="import-card">
            <div class="import-note">
              <div>Tip:</div>
              <div>Use this format to create choices automatically:
                <code>Facility Type: Hospital, Clinic, PHC</code>
              </div>
            </div>

            {% if msg %}
              <div class="toast" id="importToast">{{msg}}</div>
              <div class="status ok">Imported successfully. Redirecting to the template builder…</div>
            {% endif %}
            {% if err %}<div class="status err">{{err}}</div>{% endif %}

            <form method="POST" style="margin-top:14px">
              <input type="hidden" name="stage" value="preview"/>
              <textarea name="raw_text" placeholder="Example:\nFacility Type: Hospital, Clinic, PHC\nNumber of staff\nDoes the facility have a pharmacy?">{{ raw_text|e }}</textarea>

              <div class="import-actions">
                <label><input type="checkbox" name="default_required" {% if default_required %}checked{% endif %}/> Mark imported questions as required</label>
              </div>

              <div class="import-actions">
                <button type="submit" class="btn primary">Preview questions</button>
                <a class="btn" href="{{ url_for('ui_template_manage', template_id=template_id) }}{{kq}}">Cancel</a>
              </div>
            </form>
          </div>

          {% if preview_items %}
          <div class="import-card">
            <div class="import-note">
              <div>Preview:</div>
              <div>Detected {{ preview_items|length }} question(s). Uncheck any items you do not want to import.</div>
            </div>
            <form method="POST" style="margin-top:12px">
              <input type="hidden" name="stage" value="confirm"/>
              <input type="hidden" name="default_required" value="{{ '1' if default_required else '0' }}"/>
              <textarea name="raw_text" style="display:none">{{ raw_text|e }}</textarea>
              <div class="preview-list">
                {% for item in preview_items %}
                  <label class="preview-item">
                    <input type="checkbox" name="keep" value="{{ loop.index0 }}" checked/>
                    <div>
                      <div class="preview-q">{{ item.question_text }}</div>
                      <div class="preview-meta">
                        {{ item.question_type }}
                        {% if item.is_required or default_required %} - Required{% endif %}
                      </div>
                      {% if item.choices %}
                        <div class="preview-choices">Choices: {{ item.choices|join(', ') }}</div>
                      {% endif %}
                    </div>
                  </label>
                {% endfor %}
              </div>
              <div class="import-actions">
                <button type="submit" class="btn primary">Import selected</button>
              </div>
            </form>
          </div>
          {% endif %}
        </div>
        <script>
          (function() {
            const ok = document.getElementById("importToast");
            const redirectUrl = "{{ redirect_url }}";
            if(ok) {
              ok.classList.add("show");
              setTimeout(() => { ok.classList.remove("show"); }, 3000);
              if(redirectUrl) {
                setTimeout(() => { window.location.href = redirectUrl; }, 3200);
              }
            }
          })();
        </script>
        """,
        template_id=template_id,
        kq=key_qs(),
        msg=msg,
        err=err,
        raw_text=raw_text,
        default_required=default_required,
        preview_items=preview_items,
        redirect_url=redirect_url,
    )


@app.route("/ui/templates/<int:template_id>/import/docx", methods=["GET", "POST"])
def ui_import_docx(template_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return ui_shell("Read-only", "<div class='card'><h2>Archived projects are read-only.</h2></div>"), 403

    msg = ""
    err = ""
    preview_items = []
    source_file = ""
    default_required = 0

    if request.method == "POST":
        try:
            stage = (request.form.get("stage") or "preview").strip().lower()
            default_required = 1 if request.form.get("default_required") in ("on", "1", "true") else 0

            if stage == "confirm":
                source_file = secure_filename(request.form.get("source_file", ""))
                if not source_file:
                    raise ValueError("Missing uploaded file. Please upload again.")
                path = os.path.join(UPLOAD_DIR, source_file)
                if not os.path.exists(path):
                    raise ValueError("Uploaded file not found. Please upload again.")
                items = tpl.preview_questions_from_docx(path)
                keep = request.form.getlist("keep")
                items = _select_import_items(items, keep)
                if not items:
                    raise ValueError("No questions selected.")
                res = tpl.import_questions_from_items(
                    template_id, items, default_required=default_required
                )
                msg = f"Imported {res.get('added', 0)} question(s)."
                if res.get("errors"):
                    err = " | ".join(res["errors"][:5])
                try:
                    os.remove(path)
                except Exception:
                    pass
            else:
                if "docx" not in request.files:
                    raise ValueError("No file uploaded.")
                f = request.files["docx"]
                if not f.filename:
                    raise ValueError("Choose a .docx file.")
                filename = secure_filename(f.filename)
                if not filename.lower().endswith(".docx"):
                    raise ValueError("Only .docx files are supported.")
                token = secrets.token_urlsafe(6)
                stored = f"import_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{token}.docx"
                path = os.path.join(UPLOAD_DIR, stored)
                f.save(path)
                source_file = stored
                preview_items = tpl.preview_questions_from_docx(path)
                if not preview_items:
                    err = "No questions detected. Check formatting."
        except Exception as e:
            err = str(e)

    return render_template_string(
        """
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Poppins:wght@400;500;600;700;800&display=swap" rel="stylesheet">
        <style>
          :root{
            --surface:#ffffff;
            --surface-2:#f7f7fc;
            --text:#0f1222;
            --muted:#5f6470;
            --border:rgba(15,18,34,.12);
            --shadow:0 16px 40px rgba(15,18,34,.08);
            --primary:#8E5CFF;
            --primary-500:#7C3AED;
            --font-heading:"Poppins", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
            --font-body:"Inter", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
          }
          body{
            background:radial-gradient(900px 380px at 20% -10%, rgba(124,58,237,.12), transparent 60%), #fff;
            font-family:var(--font-body);
          }
          .import-wrap{max-width:980px; margin:0 auto; padding:24px 20px 64px;}
          .import-hero{
            background:
              radial-gradient(140px 120px at 15% 15%, rgba(124,58,237,.35), transparent 60%),
              linear-gradient(135deg, rgba(124,58,237,.22), rgba(15,18,34,.04));
            border:1px solid rgba(124,58,237,.35);
            border-radius:20px;
            padding:20px;
            box-shadow:var(--shadow);
          }
          .import-title{margin:0; font-size:24px; font-weight:800; letter-spacing:-.2px; color:var(--text); font-family:var(--font-heading);}
          .import-sub{margin-top:6px; color:var(--muted); line-height:1.6;}
          .import-card{
            margin-top:16px;
            background:var(--surface);
            border:1px solid var(--border);
            border-radius:18px;
            padding:20px;
            box-shadow:var(--shadow);
          }
          .import-note{
            display:flex;
            gap:10px;
            align-items:flex-start;
            background:var(--surface-2);
            border:1px dashed var(--border);
            border-radius:14px;
            padding:12px;
            color:var(--muted);
            font-size:13px;
          }
          .import-actions{display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-top:14px;}
          .import-tabs{display:flex; gap:8px; margin-top:10px; flex-wrap:wrap;}
          .import-tab{
            padding:6px 12px;
            border-radius:999px;
            border:1px solid var(--border);
            background:#fff;
            font-weight:600;
            font-size:12px;
            color:var(--muted);
            text-decoration:none;
          }
          .import-tab.active{
            color:#4c1d95;
            border-color:rgba(124,58,237,.35);
            background:rgba(124,58,237,.12);
          }
          .btn{
            padding:10px 14px;
            border-radius:12px;
            border:1px solid var(--border);
            background:#fff;
            font-weight:600;
            cursor:pointer;
          }
          .btn.primary{
            background:linear-gradient(135deg, var(--primary), var(--primary-500));
            color:#fff;
            border:none;
            box-shadow:0 10px 24px rgba(124,58,237,.25);
          }
          .status{
            margin-top:12px;
            padding:10px 12px;
            border-radius:12px;
            font-weight:600;
          }
          .status.ok{background:rgba(46,204,113,.12); color:#0a7a3e; border:1px solid rgba(46,204,113,.25);}
          .status.err{background:rgba(231,76,60,.12); color:#b00; border:1px solid rgba(231,76,60,.25);}
          .back-link{color:var(--muted); text-decoration:none; font-weight:600; display:inline-block; margin-bottom:8px;}
          .file-input{
            display:flex;
            align-items:center;
            gap:10px;
            padding:12px;
            border:1px solid var(--border);
            border-radius:12px;
            background:#fff;
            width:100%;
            box-sizing:border-box;
          }
          .file-input input[type="file"]{
            border:none;
            padding:0;
          }
          .preview-list{display:flex; flex-direction:column; gap:12px; margin-top:12px;}
          .preview-item{
            display:flex;
            gap:12px;
            padding:12px;
            border:1px solid var(--border);
            border-radius:12px;
            background:#fff;
          }
          .preview-item input{margin-top:4px;}
          .preview-q{font-weight:700; color:var(--text);}
          .preview-meta{font-size:12px; color:var(--muted); margin-top:4px;}
          .preview-choices{font-size:12px; color:var(--muted); margin-top:6px;}
        </style>

        <div class="import-wrap">
          <div class="import-hero">
            <a class="back-link" href="{{ url_for('ui_template_manage', template_id=template_id) }}{{kq}}">← Back to Template Builder</a>
            <h2 class="import-title">Import Questions (Word .docx)</h2>
            <div class="import-sub">Upload a document and we'll convert each paragraph into a question.</div>
            <div class="import-tabs">
              <a class="import-tab" href="{{ url_for('ui_import_text', template_id=template_id) }}{{kq}}">Text</a>
              <a class="import-tab active" href="{{ url_for('ui_import_docx', template_id=template_id) }}{{kq}}">Word</a>
              <a class="import-tab" href="{{ url_for('ui_import_pdf', template_id=template_id) }}{{kq}}">PDF</a>
            </div>
          </div>

          <div class="import-card">
            <div class="import-note">
              <div>Tip:</div>
              <div>Use headings or bullets for cleaner imports. Each paragraph becomes a question.</div>
            </div>

            {% if msg %}<div class="status ok">{{msg}}</div>{% endif %}
            {% if err %}<div class="status err">{{err}}</div>{% endif %}

            <form method="POST" enctype="multipart/form-data" style="margin-top:14px">
              <input type="hidden" name="stage" value="preview"/>
              <div class="file-input">
                <input type="file" name="docx" accept=".docx"/>
                <span class="muted">Accepted: .docx only</span>
              </div>

              <div class="import-actions">
                <label><input type="checkbox" name="default_required" {% if default_required %}checked{% endif %}/> Mark imported questions as required</label>
              </div>

              <div class="import-actions">
                <button type="submit" class="btn primary">Preview questions</button>
                <a class="btn" href="{{ url_for('ui_template_manage', template_id=template_id) }}{{kq}}">Cancel</a>
              </div>
            </form>
          </div>

          {% if preview_items %}
          <div class="import-card">
            <div class="import-note">
              <div>Preview:</div>
              <div>Detected {{ preview_items|length }} question(s). Uncheck any items you do not want to import.</div>
            </div>
            <form method="POST" style="margin-top:12px">
              <input type="hidden" name="stage" value="confirm"/>
              <input type="hidden" name="source_file" value="{{ source_file }}"/>
              <input type="hidden" name="default_required" value="{{ '1' if default_required else '0' }}"/>
              <div class="preview-list">
                {% for item in preview_items %}
                  <label class="preview-item">
                    <input type="checkbox" name="keep" value="{{ loop.index0 }}" checked/>
                    <div>
                      <div class="preview-q">{{ item.question_text }}</div>
                      <div class="preview-meta">
                        {{ item.question_type }}
                        {% if item.is_required or default_required %} - Required{% endif %}
                      </div>
                      {% if item.choices %}
                        <div class="preview-choices">Choices: {{ item.choices|join(', ') }}</div>
                      {% endif %}
                    </div>
                  </label>
                {% endfor %}
              </div>
              <div class="import-actions">
                <button type="submit" class="btn primary">Import selected</button>
                <a class="btn" href="{{ url_for('ui_template_manage', template_id=template_id) }}{{kq}}">Done</a>
              </div>
            </form>
          </div>
          {% endif %}
        </div>
        """,
        template_id=template_id,
        kq=key_qs(),
        msg=msg,
        err=err,
        source_file=source_file,
        default_required=default_required,
        preview_items=preview_items,
    )


@app.route("/ui/templates/<int:template_id>/import/pdf", methods=["GET", "POST"])
def ui_import_pdf(template_id):
    gate = admin_gate()
    if gate:
        return gate
    if template_project_locked(template_id):
        return ui_shell("Read-only", "<div class='card'><h2>Archived projects are read-only.</h2></div>"), 403

    msg = ""
    err = ""
    preview_items = []
    source_file = ""
    default_required = 0

    if request.method == "POST":
        try:
            stage = (request.form.get("stage") or "preview").strip().lower()
            default_required = 1 if request.form.get("default_required") in ("on", "1", "true") else 0

            if stage == "confirm":
                source_file = secure_filename(request.form.get("source_file", ""))
                if not source_file:
                    raise ValueError("Missing uploaded file. Please upload again.")
                path = os.path.join(UPLOAD_DIR, source_file)
                if not os.path.exists(path):
                    raise ValueError("Uploaded file not found. Please upload again.")
                items = tpl.preview_questions_from_pdf(path)
                keep = request.form.getlist("keep")
                items = _select_import_items(items, keep)
                if not items:
                    raise ValueError("No questions selected.")
                res = tpl.import_questions_from_items(
                    template_id, items, default_required=default_required
                )
                msg = f"Imported {res.get('added', 0)} question(s)."
                if res.get("errors"):
                    err = " | ".join(res["errors"][:5])
                try:
                    os.remove(path)
                except Exception:
                    pass
            else:
                if "pdf" not in request.files:
                    raise ValueError("No file uploaded.")
                f = request.files["pdf"]
                if not f.filename:
                    raise ValueError("Choose a .pdf file.")
                filename = secure_filename(f.filename)
                if not filename.lower().endswith(".pdf"):
                    raise ValueError("Only .pdf files are supported.")
                token = secrets.token_urlsafe(6)
                stored = f"import_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{token}.pdf"
                path = os.path.join(UPLOAD_DIR, stored)
                f.save(path)
                source_file = stored
                preview_items = tpl.preview_questions_from_pdf(path)
                if not preview_items:
                    err = "No questions detected. Check formatting."
        except Exception as e:
            err = str(e)

    return render_template_string(
        """
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Poppins:wght@400;500;600;700;800&display=swap" rel="stylesheet">
        <style>
          :root{
            --surface:#ffffff;
            --surface-2:#f7f7fc;
            --text:#0f1222;
            --muted:#5f6470;
            --border:rgba(15,18,34,.12);
            --shadow:0 16px 40px rgba(15,18,34,.08);
            --primary:#8E5CFF;
            --primary-500:#7C3AED;
            --font-heading:"Poppins", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
            --font-body:"Inter", system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
          }
          body{
            background:radial-gradient(900px 380px at 20% -10%, rgba(124,58,237,.12), transparent 60%), #fff;
            font-family:var(--font-body);
          }
          .import-wrap{max-width:980px; margin:0 auto; padding:24px 20px 64px;}
          .import-hero{
            background:
              radial-gradient(140px 120px at 15% 15%, rgba(124,58,237,.35), transparent 60%),
              linear-gradient(135deg, rgba(124,58,237,.22), rgba(15,18,34,.04));
            border:1px solid rgba(124,58,237,.35);
            border-radius:20px;
            padding:20px;
            box-shadow:var(--shadow);
          }
          .import-title{margin:0; font-size:24px; font-weight:800; letter-spacing:-.2px; color:var(--text); font-family:var(--font-heading);}
          .import-sub{margin-top:6px; color:var(--muted); line-height:1.6;}
          .import-card{
            margin-top:16px;
            background:var(--surface);
            border:1px solid var(--border);
            border-radius:18px;
            padding:20px;
            box-shadow:var(--shadow);
          }
          .import-note{
            display:flex;
            gap:10px;
            align-items:flex-start;
            background:var(--surface-2);
            border:1px dashed var(--border);
            border-radius:14px;
            padding:12px;
            color:var(--muted);
            font-size:13px;
          }
          .import-actions{display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-top:14px;}
          .import-tabs{display:flex; gap:8px; margin-top:10px; flex-wrap:wrap;}
          .import-tab{
            padding:6px 12px;
            border-radius:999px;
            border:1px solid var(--border);
            background:#fff;
            font-weight:600;
            font-size:12px;
            color:var(--muted);
            text-decoration:none;
          }
          .import-tab.active{
            color:#4c1d95;
            border-color:rgba(124,58,237,.35);
            background:rgba(124,58,237,.12);
          }
          .btn{
            padding:10px 14px;
            border-radius:12px;
            border:1px solid var(--border);
            background:#fff;
            font-weight:600;
            cursor:pointer;
          }
          .btn.primary{
            background:linear-gradient(135deg, var(--primary), var(--primary-500));
            color:#fff;
            border:none;
            box-shadow:0 10px 24px rgba(124,58,237,.25);
          }
          .status{
            margin-top:12px;
            padding:10px 12px;
            border-radius:12px;
            font-weight:600;
          }
          .status.ok{background:rgba(46,204,113,.12); color:#0a7a3e; border:1px solid rgba(46,204,113,.25);}
          .status.err{background:rgba(231,76,60,.12); color:#b00; border:1px solid rgba(231,76,60,.25);}
          .back-link{color:var(--muted); text-decoration:none; font-weight:600; display:inline-block; margin-bottom:8px;}
          .file-input{
            display:flex;
            align-items:center;
            gap:10px;
            padding:12px;
            border:1px solid var(--border);
            border-radius:12px;
            background:#fff;
            width:100%;
            box-sizing:border-box;
          }
          .file-input input[type="file"]{
            border:none;
            padding:0;
          }
          .preview-list{display:flex; flex-direction:column; gap:12px; margin-top:12px;}
          .preview-item{
            display:flex;
            gap:12px;
            padding:12px;
            border:1px solid var(--border);
            border-radius:12px;
            background:#fff;
          }
          .preview-item input{margin-top:4px;}
          .preview-q{font-weight:700; color:var(--text);}
          .preview-meta{font-size:12px; color:var(--muted); margin-top:4px;}
          .preview-choices{font-size:12px; color:var(--muted); margin-top:6px;}
        </style>

        <div class="import-wrap">
          <div class="import-hero">
            <a class="back-link" href="{{ url_for('ui_template_manage', template_id=template_id) }}{{kq}}">← Back to Template Builder</a>
            <h2 class="import-title">Import Questions (PDF)</h2>
            <div class="import-sub">Upload a PDF and we will detect questions from the text layer.</div>
            <div class="import-tabs">
              <a class="import-tab" href="{{ url_for('ui_import_text', template_id=template_id) }}{{kq}}">Text</a>
              <a class="import-tab" href="{{ url_for('ui_import_docx', template_id=template_id) }}{{kq}}">Word</a>
              <a class="import-tab active" href="{{ url_for('ui_import_pdf', template_id=template_id) }}{{kq}}">PDF</a>
            </div>
          </div>

          <div class="import-card">
            <div class="import-note">
              <div>Note:</div>
              <div>Scanned PDFs without selectable text may not import correctly. If needed, convert to text or Word first.</div>
            </div>

            {% if msg %}<div class="status ok">{{msg}}</div>{% endif %}
            {% if err %}<div class="status err">{{err}}</div>{% endif %}

            <form method="POST" enctype="multipart/form-data" style="margin-top:14px">
              <input type="hidden" name="stage" value="preview"/>
              <div class="file-input">
                <input type="file" name="pdf" accept=".pdf"/>
                <span class="muted">Accepted: .pdf only</span>
              </div>

              <div class="import-actions">
                <label><input type="checkbox" name="default_required" {% if default_required %}checked{% endif %}/> Mark imported questions as required</label>
              </div>

              <div class="import-actions">
                <button type="submit" class="btn primary">Preview questions</button>
                <a class="btn" href="{{ url_for('ui_template_manage', template_id=template_id) }}{{kq}}">Cancel</a>
              </div>
            </form>
          </div>

          {% if preview_items %}
          <div class="import-card">
            <div class="import-note">
              <div>Preview:</div>
              <div>Detected {{ preview_items|length }} question(s). Uncheck any items you do not want to import.</div>
            </div>
            <form method="POST" style="margin-top:12px">
              <input type="hidden" name="stage" value="confirm"/>
              <input type="hidden" name="source_file" value="{{ source_file }}"/>
              <input type="hidden" name="default_required" value="{{ '1' if default_required else '0' }}"/>
              <div class="preview-list">
                {% for item in preview_items %}
                  <label class="preview-item">
                    <input type="checkbox" name="keep" value="{{ loop.index0 }}" checked/>
                    <div>
                      <div class="preview-q">{{ item.question_text }}</div>
                      <div class="preview-meta">
                        {{ item.question_type }}
                        {% if item.is_required or default_required %} - Required{% endif %}
                      </div>
                      {% if item.choices %}
                        <div class="preview-choices">Choices: {{ item.choices|join(', ') }}</div>
                      {% endif %}
                    </div>
                  </label>
                {% endfor %}
              </div>
              <div class="import-actions">
                <button type="submit" class="btn primary">Import selected</button>
                <a class="btn" href="{{ url_for('ui_template_manage', template_id=template_id) }}{{kq}}">Done</a>
              </div>
            </form>
          </div>
          {% endif %}
        </div>
        """,
        template_id=template_id,
        kq=key_qs(),
        msg=msg,
        err=err,
        source_file=source_file,
        default_required=default_required,
        preview_items=preview_items,
    )


# ---------------------------
# Surveys + QA (Supervisor)
# ---------------------------
@app.route("/ui/surveys")
def ui_surveys():
    gate = admin_gate()
    if gate:
        return gate

    is_admin = bool(ADMIN_KEY and request.args.get("key") == ADMIN_KEY)
    org_id = current_org_id() if (REQUIRE_SUPERVISOR_KEY and not is_admin) else None
    if org_id is None:
        try:
            sess_org = session.get("org_id")
            org_id = int(sess_org) if sess_org is not None else None
        except Exception:
            org_id = None
    project_id = request.args.get("project_id") or ""
    project_id = int(project_id) if str(project_id).isdigit() else None
    project_id = resolve_project_context(project_id)
    if project_id is None and org_id is not None:
        try:
            project_id = prj.get_default_project_id(int(org_id))
        except Exception:
            project_id = None
    project_options = ""
    if PROJECT_REQUIRED and project_id is None:
        projects = prj.list_projects(200, organization_id=org_id)
        options = "".join(
            [f"<option value='{p.get('id')}'>{html.escape(p.get('name') or 'Project')}</option>" for p in projects]
        )
        html_view = f"""
        <div class="card">
          <h1 class="h1">Submissions</h1>
          <div class="muted">Select a project to view submissions.</div>
          <form method="GET" style="margin-top:12px">
            <select name="project_id" required>
              <option value="">Choose project</option>
              {options}
            </select>
            <button class="btn btn-primary" type="submit" style="margin-left:8px">Open</button>
          </form>
        </div>
        """
        return ui_shell("Submissions", html_view, show_project_switcher=False)
    try:
        projects = prj.list_projects(200, organization_id=org_id)
        for p in projects:
            pid = int(p.get("id"))
            status = (p.get("status") or "ACTIVE").upper()
            status_text = " [Archived]" if status == "ARCHIVED" else (" [Draft]" if status == "DRAFT" else "")
            selected = "selected" if project_id is not None and int(project_id) == pid else ""
            project_options += f"<option value='{pid}' {selected}>{p.get('name')}{status_text}</option>"
    except Exception:
        project_options = ""
    sup_id = current_supervisor_id()
    rows = sup.filter_surveys(
        limit=200,
        project_id=str(project_id) if project_id else "",
        supervisor_id=str(sup_id) if sup_id else "",
    )
    sync_map = {}
    meta_map = {}
    coverage_name_map = {}
    if rows and "sync_source" in surveys_cols():
        try:
            ids = [int(r[0]) for r in rows if r and str(r[0]).isdigit()]
            if ids:
                placeholders = ",".join(["?"] * len(ids))
                with get_conn() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        f"SELECT id, sync_source FROM surveys WHERE id IN ({placeholders})",
                        tuple(ids),
                    )
                    sync_map = {int(r["id"]): r["sync_source"] for r in cur.fetchall() if r["id"] is not None}
        except Exception:
            sync_map = {}
    if rows:
        try:
            ids = [int(r[0]) for r in rows if r and str(r[0]).isdigit()]
            if ids:
                placeholders = ",".join(["?"] * len(ids))
                with get_conn() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        f"""
                        SELECT id, review_status, sync_source, gps_lat, gps_lng, gps_accuracy, coverage_node_id
                        FROM surveys
                        WHERE id IN ({placeholders})
                        """,
                        tuple(ids),
                    )
                    meta_map = {int(r["id"]): dict(r) for r in cur.fetchall() if r["id"] is not None}
                cov_ids = [int(r.get("coverage_node_id")) for r in meta_map.values() if r.get("coverage_node_id")]
                if cov_ids:
                    placeholders = ",".join(["?"] * len(cov_ids))
                    with get_conn() as conn:
                        cur = conn.cursor()
                        cur.execute(
                            f"SELECT id, name FROM coverage_nodes WHERE id IN ({placeholders})",
                            tuple(cov_ids),
                        )
                        coverage_name_map = {int(r["id"]): r["name"] for r in cur.fetchall()}
        except Exception:
            meta_map = {}
            coverage_name_map = {}

    trs = []
    for idx, (sid, facility_name, template_id, survey_type, enumerator_name, status, created_at) in enumerate(rows, start=1):
        sync_source = (sync_map.get(int(sid)) or "").upper() if sync_map else ""
        sync_badge = ""
        if sync_source == "OFFLINE_SYNC":
            sync_badge = "<span class='sync-pill'>Offline Sync</span>"
        meta = meta_map.get(int(sid)) if meta_map else None
        review_status = (meta.get("review_status") if meta else None) or "PENDING"
        review_status = str(review_status).upper()
        review_badge = {
            "APPROVED": ("Approved", "review-approved"),
            "REJECTED": ("Rejected", "review-rejected"),
            "REVISION": ("Needs revision", "review-revision"),
            "PENDING": ("Pending review", "review-pending"),
        }.get(review_status, (review_status.title(), "review-pending"))
        gps_ok = False
        if meta:
            gps_ok = meta.get("gps_lat") is not None and meta.get("gps_lng") is not None
        cov_name = ""
        if meta and meta.get("coverage_node_id"):
            cov_name = coverage_name_map.get(int(meta.get("coverage_node_id")), "")
        trust_bits = []
        trust_bits.append("GPS ✓" if gps_ok else "GPS missing")
        if cov_name:
            trust_bits.append(cov_name)
        if sync_source == "OFFLINE_SYNC":
            trust_bits.append("Offline")
        trust_html = " ".join([f"<span class='trust-pill'>{html.escape(b)}</span>" for b in trust_bits])
        trs.append(
            f"""
            <tr>
              <td style="width:90px"><span class="q-title">{idx}</span></td>
              <td>
                <div class="q-title">{facility_name}</div>
                <div class="q-meta">Template: <b>{survey_type}</b></div>
                <div class="trust-row">{trust_html}</div>
              </td>
              <td>
                <div class="q-title">{enumerator_name}</div>
                <div class="q-meta">{created_at}</div>
              </td>
              <td>
                <span class="status-pill {status.lower()}">{status}</span>
                {sync_badge}
                <div style="margin-top:6px"><span class="review-pill {review_badge[1]}">{review_badge[0]}</span></div>
              </td>
              <td style="width:160px"><a class="btn btn-sm" href="{url_for('ui_survey_detail', survey_id=sid)}{key_qs()}">View</a></td>
            </tr>
            """
        )

    html_view = render_template_string(
        """
        <style>
          .questions-table {
            border-collapse: separate;
            border-spacing: 0 10px;
          }
          .questions-table thead th {
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: .08em;
            color: var(--muted);
            font-weight: 600;
            padding: 8px 14px;
          }
          .questions-table tbody tr {
            background: var(--surface);
            box-shadow: 0 10px 24px rgba(15,18,34,.06);
          }
          .questions-table tbody td {
            padding: 14px 16px;
            vertical-align: top;
          }
          .questions-table tbody tr td:first-child {
            border-top-left-radius: 12px;
            border-bottom-left-radius: 12px;
          }
          .questions-table tbody tr td:last-child {
            border-top-right-radius: 12px;
            border-bottom-right-radius: 12px;
          }
          .q-title {
            font-weight: 600;
            font-size: 14px;
            color: var(--text);
          }
          .q-meta {
            color: var(--muted);
            font-size: 12px;
            margin-top: 6px;
          }
          .btn.btn-sm {
            padding: 8px 12px;
            font-size: 12px;
            border-radius: 10px;
            font-weight: 600;
          }
          .status-pill {
            display:inline-flex;
            align-items:center;
            padding:6px 10px;
            border-radius:999px;
            font-size:12px;
            font-weight:700;
            border:1px solid var(--border);
            background:var(--surface-2);
          }
          .status-pill.completed {
            border-color: rgba(46,204,113,.35);
            background: rgba(46,204,113,.12);
            color: #0a7a3e;
          }
          .status-pill.draft {
            border-color: rgba(245,158,11,.35);
            background: rgba(245,158,11,.12);
            color: #b45309;
          }
          .sync-pill{
            display:inline-flex;
            align-items:center;
            padding:4px 8px;
            border-radius:999px;
            font-size:11px;
            font-weight:700;
            border:1px solid rgba(124,58,237,.35);
            background:rgba(124,58,237,.12);
            color:#5b21b6;
            margin-left:6px;
          }
          .review-pill{
            display:inline-flex;
            align-items:center;
            padding:4px 8px;
            border-radius:999px;
            font-size:11px;
            font-weight:700;
            border:1px solid transparent;
          }
          .review-approved{background:rgba(22,163,74,.14); border-color:rgba(22,163,74,.35); color:#15803d;}
          .review-rejected{background:rgba(220,38,38,.14); border-color:rgba(220,38,38,.35); color:#b91c1c;}
          .review-revision{background:rgba(245,158,11,.18); border-color:rgba(245,158,11,.35); color:#b45309;}
          .review-pending{background:rgba(148,163,184,.2); border-color:rgba(148,163,184,.45); color:#475569;}
          .trust-row{display:flex; flex-wrap:wrap; gap:6px; margin-top:6px;}
          .trust-pill{
            display:inline-flex;
            align-items:center;
            padding:4px 8px;
            border-radius:999px;
            font-size:11px;
            font-weight:700;
            border:1px solid var(--border);
            background:var(--surface-2);
            color:var(--muted);
          }
          .submissions-hero {
            background:
              radial-gradient(180px 140px at 10% 10%, rgba(124,58,237,.28), transparent 60%),
              linear-gradient(135deg, rgba(124,58,237,.18), rgba(15,18,34,.02));
            border:1px solid rgba(124,58,237,.28);
            border-radius:20px;
            padding:20px;
            box-shadow:0 16px 40px rgba(15,18,34,.08);
          }
          .submissions-title {
            margin:0;
            font-size:24px;
            font-weight:800;
            letter-spacing:-.2px;
          }
          .proj-inline-switcher{
            display:inline-flex;
            align-items:center;
            gap:8px;
            margin-top:10px;
            padding:6px 10px;
            border-radius:12px;
            border:1px solid var(--border);
            background:var(--surface);
          }
          .proj-inline-switcher label{
            font-size:11px;
            color:var(--muted);
            font-weight:700;
          }
          .proj-inline-switcher select{
            border:none;
            padding:6px 8px;
            font-size:12px;
            background:transparent;
            color:var(--text);
          }
        </style>

        <div class="submissions-hero">
          <div class="row" style="justify-content:space-between; align-items:flex-start; gap:12px;">
            <div>
              <h2 class="submissions-title">Submissions</h2>
              <div class="muted" style="margin-top:6px">Review enumerator submissions and open details.</div>
              <div class="proj-inline-switcher">
                <label>Project</label>
                <select id="projectSwitcherInline">
                  <option value="">All</option>
                  {{project_options|safe}}
                </select>
              </div>
            </div>
            <a class="btn btn-sm" style="margin-left:auto; margin-top:18px;" href="{{ url_for('ui_home') }}{{kq}}">Back to dashboard</a>
          </div>
        </div>

        <div style="margin-top:14px; overflow:auto;">
          <table class="table questions-table">
            <thead>
              <tr>
                <th style="width:90px">ID</th>
                <th>Facility</th>
                <th>Enumerator</th>
                <th>Status</th>
                <th style="width:160px">Action</th>
              </tr>
            </thead>
            <tbody>
              {{rows|safe}}
            </tbody>
          </table>
        </div>
        <script>
          (function(){
            const switcher = document.getElementById("projectSwitcherInline");
            if(!switcher) return;
            switcher.addEventListener("change", (e)=>{
              const val = e.target.value;
              if(!val){
                window.location.href = "/ui/surveys{{kq}}";
              } else {
                window.location.href = "/ui/surveys?project_id=" + val + "{{kq}}";
              }
            });
          })();
        </script>
        """,
        kq=key_qs(),
        project_options=project_options,
        rows="".join(
            trs) if trs else "<tr><td style='padding:10px' colspan='5'>No surveys yet.</td></tr>",
    )
    return ui_shell("Submissions", html_view, show_project_switcher=False)


@app.route("/ui/surveys/<int:survey_id>")
def ui_survey_detail(survey_id):
    gate = admin_gate()
    if gate:
        return gate

    header, answers, qa = sup.get_survey_details(int(survey_id))
    if not header:
        return render_template_string(
            "<h2>Survey not found</h2><p><a href='{{url_for(\"ui_surveys\")}}{{kq}}'>Back</a></p>",
            kq=key_qs(),
        ), 404

    h = {
        "survey_id": header[0],
        "facility_id": header[1],
        "facility_name": header[2],
        "template_id": header[3],
        "survey_type": header[4],
        "enumerator_name": header[5],
        "status": header[6],
        "created_at": header[7],
        "gps_lat": header[8] if len(header) > 8 else None,
        "gps_lng": header[9] if len(header) > 9 else None,
        "gps_accuracy": header[10] if len(header) > 10 else None,
        "gps_timestamp": header[11] if len(header) > 11 else None,
        "coverage_node_id": header[12] if len(header) > 12 else None,
        "coverage_node_name": header[13] if len(header) > 13 else None,
    }
    extra_cols = []
    for col in (
        "consent_obtained",
        "consent_timestamp",
        "attestation_text",
        "attestation_timestamp",
        "sync_source",
        "synced_at",
        "client_created_at",
        "client_uuid",
        "source",
        "review_status",
        "review_reason",
        "reviewed_at",
        "reviewed_by",
    ):
        if col in surveys_cols():
            extra_cols.append(col)
    if extra_cols:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                f"""
                SELECT {", ".join(extra_cols)}
                FROM surveys
                WHERE id=?
                LIMIT 1
                """,
                (int(survey_id),),
            )
            r = cur.fetchone()
            if r:
                for col in extra_cols:
                    h[col] = r[col]

    qa_d = qa.to_dict() if hasattr(qa, "to_dict") else qa.__dict__
    qa_flags = [f for f in (qa_d.get("flags") or []) if f]
    missing_required = qa_d.get("missing_required_questions") or []
    qa_missing_required_count = int(qa_d.get("missing_required_count") or 0)
    qa_empty_count = int(qa_d.get("empty_answer_count") or 0)
    qa_low_conf_count = int(qa_d.get("low_confidence_count") or 0)
    qa_total_answers = int(qa_d.get("total_answers") or 0)
    qa_gps_missing = bool(qa_d.get("gps_missing"))
    qa_gps_present = bool(qa_d.get("gps_present"))
    qa_suspicious = bool(qa_d.get("has_suspicious_values"))
    severity = float(qa_d.get("severity") or 0)
    if severity >= 0.7:
        sev_label = "High"
        sev_class = "sev-high"
    elif severity >= 0.4:
        sev_label = "Medium"
        sev_class = "sev-med"
    else:
        sev_label = "Low"
        sev_class = "sev-low"

    review_status = (h.get("review_status") or "PENDING").upper()
    review_reason = h.get("review_reason") or ""
    review_badge = {
        "APPROVED": ("Approved", "review-approved"),
        "REJECTED": ("Rejected", "review-rejected"),
        "REVISION": ("Needs revision", "review-revision"),
        "PENDING": ("Pending review", "review-pending"),
    }.get(review_status, (review_status.title(), "review-pending"))

    flag_pills = []
    for f in qa_flags:
        f_upper = str(f).upper()
        cls = "flag-pill"
        if "GPS" in f_upper:
            cls += " flag-gps"
        elif "DUPLICATE" in f_upper:
            cls += " flag-dup"
        elif "MISSING" in f_upper:
            cls += " flag-miss"
        else:
            cls += " flag-gen"
        flag_pills.append(f"<span class=\"{cls}\">{f}</span>")
    qa_flags_html = "".join(flag_pills) if flag_pills else "<span class='muted'>No QA flags</span>"

    gps_link = None
    if h.get("gps_lat") is not None and h.get("gps_lng") is not None:
        gps_link = f"https://maps.google.com/?q={h.get('gps_lat')},{h.get('gps_lng')}"

    ans_rows = []
    answer_count = len(answers)
    for idx, a in enumerate(answers, start=1):
        q_text = html.escape(str(a[2] or ""))
        a_text = html.escape(str(a[3] or ""))
        answer_html = a_text if a_text else "<span class='muted'>—</span>"
        ans_rows.append(
            f"""
            <div class="answer-card">
              <div class="answer-index">{idx}</div>
              <div class="answer-body">
                <div class="answer-q">{q_text}</div>
                <div class="answer-a">{answer_html}</div>
              </div>
            </div>
            """
        )

    error_rows = []
    error_count = 0
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, error_type, error_message, created_at
            FROM submission_errors
            WHERE survey_id=?
            ORDER BY id DESC
            LIMIT 50
            """,
            (int(survey_id),),
        )
        for r in cur.fetchall():
            error_count += 1
            err_type = (r["error_type"] or "system").lower()
            pill_class = "validation" if err_type == "validation" else "system"
            error_rows.append(
                f"<tr><td><span class='error-pill {pill_class}'>{html.escape(err_type)}</span></td>"
                f"<td>{html.escape(str(r['error_message'] or ''))}</td>"
                f"<td>{html.escape(str(r['created_at'] or ''))}</td></tr>"
            )

    return render_template_string(
        """
        <style>
          .detail-hero{
            background:
              radial-gradient(220px 160px at 12% 10%, rgba(14,116,144,.22), transparent 60%),
              linear-gradient(135deg, rgba(59,130,246,.18), rgba(15,18,34,.02));
            border:1px solid rgba(59,130,246,.25);
            border-radius:24px;
            padding:22px;
            box-shadow:0 18px 46px rgba(15,18,34,.10);
          }
          .detail-title{
            margin:0;
            font-size:26px;
            font-weight:800;
            letter-spacing:-.3px;
          }
          .detail-kicker{
            font-size:12px;
            text-transform:uppercase;
            letter-spacing:.14em;
            color:var(--muted);
            font-weight:700;
          }
          .detail-sub{
            margin-top:6px;
            color:var(--muted);
            font-size:13px;
          }
          .detail-shell{
            display:grid;
            grid-template-columns: minmax(0, 2fr) minmax(0, 1fr);
            gap:16px;
            margin-top:16px;
          }
          @media (max-width: 980px){
            .detail-shell{grid-template-columns:1fr;}
          }
          .chip-row{
            display:flex;
            flex-wrap:wrap;
            gap:8px;
            margin-top:10px;
          }
          .chip{
            display:inline-flex;
            align-items:center;
            padding:6px 10px;
            border-radius:999px;
            font-size:12px;
            font-weight:700;
            border:1px solid var(--border);
            background:var(--surface-2);
          }
          .status-pill{
            display:inline-flex;
            align-items:center;
            padding:6px 10px;
            border-radius:999px;
            font-size:12px;
            font-weight:700;
            border:1px solid var(--border);
            background:var(--surface-2);
          }
          .status-pill.completed{
            border-color:rgba(22,163,74,.35);
            background:rgba(22,163,74,.12);
            color:#15803d;
          }
          .status-pill.draft{
            border-color:rgba(245,158,11,.35);
            background:rgba(245,158,11,.12);
            color:#b45309;
          }
          .sev-pill{
            display:inline-flex;
            align-items:center;
            padding:6px 10px;
            border-radius:999px;
            font-size:12px;
            font-weight:800;
            border:1px solid transparent;
          }
          .sev-high{background:rgba(239,68,68,.15); border-color:rgba(239,68,68,.35); color:#b91c1c;}
          .sev-med{background:rgba(245,158,11,.15); border-color:rgba(245,158,11,.35); color:#b45309;}
          .sev-low{background:rgba(34,197,94,.15); border-color:rgba(34,197,94,.35); color:#15803d;}
          .review-pill{
            display:inline-flex;
            align-items:center;
            padding:6px 10px;
            border-radius:999px;
            font-size:12px;
            font-weight:800;
            border:1px solid transparent;
          }
          .review-approved{background:rgba(22,163,74,.14); border-color:rgba(22,163,74,.35); color:#15803d;}
          .review-rejected{background:rgba(220,38,38,.14); border-color:rgba(220,38,38,.35); color:#b91c1c;}
          .review-revision{background:rgba(245,158,11,.18); border-color:rgba(245,158,11,.35); color:#b45309;}
          .review-pending{background:rgba(148,163,184,.2); border-color:rgba(148,163,184,.45); color:#475569;}
          .section-header{
            display:flex;
            align-items:center;
            justify-content:space-between;
            gap:12px;
            padding-bottom:10px;
            border-bottom:1px solid var(--border);
            margin-bottom:14px;
          }
          .meta-grid{
            display:grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap:14px;
          }
          .meta-card{
            position:relative;
            border:1px solid var(--border);
            border-radius:16px;
            padding:14px;
            background:var(--surface);
            box-shadow:0 12px 28px rgba(15,18,34,.06);
            overflow:hidden;
          }
          .meta-card::before{
            content:"";
            position:absolute;
            top:0;
            left:0;
            right:0;
            height:3px;
            background:var(--primary);
            opacity:.75;
          }
          .meta-card.primary{
            border-color:var(--primary);
            background:linear-gradient(135deg, var(--primary-soft), rgba(15,18,34,.02));
          }
          .meta-label{
            font-size:11px;
            font-weight:800;
            color:var(--primary);
            text-transform:uppercase;
            letter-spacing:.12em;
          }
          .meta-value{
            margin-top:8px;
            font-weight:700;
            color:var(--text);
            font-size:15px;
          }
          .meta-muted{color:var(--muted); font-size:12px;}
          .detail-actions .btn{padding:8px 12px; font-size:12px; border-radius:10px;}
          .detail-actions .btn.danger{
            border-color:rgba(220,38,38,.35);
            color:#b91c1c;
            background:transparent;
          }
          .qa-summary{
            display:grid;
            gap:12px;
          }
          .qa-metrics{
            display:grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap:12px;
          }
          .qa-metric{
            position:relative;
            border:1px solid var(--border);
            border-radius:14px;
            padding:12px;
            background:var(--surface);
            box-shadow:0 10px 24px rgba(15,18,34,.06);
            overflow:hidden;
          }
          .qa-metric::before{
            content:"";
            position:absolute;
            left:0;
            top:0;
            bottom:0;
            width:4px;
            background:var(--primary);
            opacity:.75;
          }
          .qa-metric .label{
            font-size:11px;
            text-transform:uppercase;
            letter-spacing:.1em;
            color:var(--muted);
            font-weight:700;
          }
          .qa-metric .value{
            margin-top:6px;
            font-weight:800;
            font-size:18px;
            color:var(--primary);
          }
          .qa-block{
            border:1px solid var(--border);
            border-radius:14px;
            padding:12px;
            background:var(--surface);
          }
          .qa-flags{
            display:flex;
            flex-wrap:wrap;
            gap:6px;
          }
          .flag-pill{
            display:inline-flex;
            align-items:center;
            padding:6px 10px;
            border-radius:999px;
            font-size:12px;
            font-weight:700;
            border:1px solid var(--border);
            background:var(--surface-2);
            box-shadow:0 6px 14px rgba(15,18,34,.08);
          }
          .flag-gps{background:rgba(14,116,144,.14); border-color:rgba(14,116,144,.35);}
          .flag-dup{background:rgba(245,158,11,.18); border-color:rgba(245,158,11,.35);}
          .flag-miss{background:rgba(239,68,68,.18); border-color:rgba(239,68,68,.35);}
          .flag-gen{background:rgba(99,102,241,.14); border-color:rgba(99,102,241,.35);}
          .answers-grid{
            display:grid;
            gap:12px;
          }
          .answer-card{
            display:grid;
            grid-template-columns: 44px 1fr;
            gap:12px;
            align-items:flex-start;
            border:1px solid var(--border);
            border-left:4px solid var(--primary);
            border-radius:16px;
            padding:12px;
            background:var(--surface);
            box-shadow:0 10px 22px rgba(15,18,34,.06);
            transition:transform .12s ease, box-shadow .12s ease;
          }
          .answer-card:hover{
            transform:translateY(-2px);
            box-shadow:0 16px 34px rgba(15,18,34,.12);
          }
          .answer-index{
            width:36px;
            height:36px;
            border-radius:12px;
            background:var(--primary-soft);
            color:var(--primary);
            font-weight:800;
            display:flex;
            align-items:center;
            justify-content:center;
            font-size:13px;
          }
          .answer-q{
            font-weight:700;
            color:var(--text);
          }
          .answer-a{
            margin-top:6px;
            color:var(--text);
            white-space:pre-wrap;
          }
          .errors-table th{
            font-size:12px;
            text-transform:uppercase;
            letter-spacing:.08em;
            color:var(--muted);
          }
          .errors-table td{
            padding:12px;
          }
          .errors-table tbody tr:nth-child(odd){
            background:var(--surface-2);
          }
          .error-pill{
            display:inline-flex;
            align-items:center;
            padding:4px 8px;
            border-radius:999px;
            font-size:11px;
            font-weight:700;
            border:1px solid var(--border);
            text-transform:uppercase;
            letter-spacing:.06em;
          }
          .error-pill.system{
            background:rgba(220,38,38,.12);
            border-color:rgba(220,38,38,.35);
            color:#b91c1c;
          }
          .error-pill.validation{
            background:rgba(245,158,11,.12);
            border-color:rgba(245,158,11,.35);
            color:#b45309;
          }
          .signal-list{
            display:grid;
            gap:10px;
          }
          .signal-item{
            display:flex;
            align-items:flex-start;
            justify-content:space-between;
            gap:12px;
            border:1px solid var(--border);
            border-radius:12px;
            padding:10px 12px;
            background:var(--surface);
          }
          .signal-item .label{
            font-size:12px;
            text-transform:uppercase;
            letter-spacing:.12em;
            color:var(--muted);
            font-weight:700;
          }
          .signal-item .value{
            font-weight:700;
            color:var(--text);
            text-align:right;
          }
        </style>

        <div class="card">
          <div class="detail-hero">
            <div class="row" style="justify-content:space-between; align-items:flex-start; gap:12px;">
              <div>
                <div class="detail-kicker">Submission</div>
                <h1 class="detail-title">#{{h.survey_id}} · {{h.facility_name}}</h1>
                <div class="detail-sub">Enumerator: {{h.enumerator_name or '—'}} · Template: {{h.survey_type or '—'}}</div>
                <div class="chip-row">
                  <span class="status-pill {{h.status|lower}}">{{h.status}}</span>
                  <span class="sev-pill {{sev_class}}">Severity {{sev_label}} · {{'%.2f'|format(severity)}}</span>
                  <span class="review-pill {{ review_badge[1] }}">{{ review_badge[0] }}</span>
                  <span class="chip">Answers {{answer_count}}</span>
                  <span class="chip">Errors {{error_count}}</span>
                  <span class="chip">Submitted {{h.created_at}}</span>
                  {% if h.sync_source == 'OFFLINE_SYNC' %}
                    <span class="chip">Synced from offline</span>
                  {% endif %}
                </div>
              </div>
              <div class="row detail-actions" style="margin-left:auto;">
                <a class="btn" href="{{ url_for('ui_surveys') }}{{kq}}">Back</a>
                <a class="btn danger" href="{{ url_for('ui_survey_delete', survey_id=h.survey_id) }}{{kq}}">Delete</a>
              </div>
            </div>
          </div>
        </div>

        <div class="detail-shell">
          <div class="stack">
            <div class="card">
              <div class="section-header">
                <h2 class="h2">Overview</h2>
                <span class="muted">Reference #{{h.survey_id}}</span>
              </div>
              <div class="meta-grid">
                <div class="meta-card primary">
                  <div class="meta-label">Facility</div>
                  <div class="meta-value">{{h.facility_name}}</div>
                </div>
                <div class="meta-card primary">
                  <div class="meta-label">Enumerator</div>
                  <div class="meta-value">{{h.enumerator_name}}</div>
                </div>
                <div class="meta-card">
                  <div class="meta-label">Template</div>
                  <div class="meta-value">{{h.survey_type}}</div>
                </div>
                <div class="meta-card">
                  <div class="meta-label">Coverage</div>
                  <div class="meta-value">{{h.coverage_node_name or '—'}}</div>
                </div>
                <div class="meta-card">
                  <div class="meta-label">GPS</div>
                  <div class="meta-value">{{h.gps_lat or '—'}} , {{h.gps_lng or '—'}}</div>
                  <div class="meta-muted">Accuracy: {{h.gps_accuracy or '—'}} · {{h.gps_timestamp or '—'}}</div>
                  {% if gps_link %}
                    <div style="margin-top:8px;">
                      <a class="btn btn-sm" href="{{gps_link}}" target="_blank" rel="noopener">Open map</a>
                    </div>
                  {% endif %}
                </div>
                {% if h.sync_source or h.client_created_at or h.synced_at %}
                <div class="meta-card">
                  <div class="meta-label">Sync Source</div>
                  <div class="meta-value">
                    {% if h.sync_source == 'OFFLINE_SYNC' %}
                      Offline sync
                    {% elif h.sync_source %}
                      {{h.sync_source}}
                    {% else %}
                      Online
                    {% endif %}
                  </div>
                  <div class="meta-muted">Synced at: {{h.synced_at or '—'}}</div>
                  {% if h.client_created_at %}
                    <div class="meta-muted">Captured: {{h.client_created_at}}</div>
                  {% endif %}
                </div>
                {% endif %}
              </div>
            </div>

            <div class="card">
              <div class="section-header">
                <h2 class="h2">Answers</h2>
                <span class="muted">{{answer_count}} responses</span>
              </div>
              <div class="answers-grid">
                {{ans|safe}}
              </div>
            </div>

            <div class="card">
              <div class="section-header">
                <h2 class="h2">System Errors</h2>
              </div>
              <table class="table errors-table">
                <thead>
                  <tr>
                    <th style="width:140px">Type</th>
                    <th>Error</th>
                    <th style="width:160px">When</th>
                  </tr>
                </thead>
                <tbody>
                  {{errors|safe}}
                </tbody>
              </table>
            </div>
          </div>

          <div class="stack">
            <div class="card">
              <div class="section-header">
                <h2 class="h2">QA Summary</h2>
              </div>
              <div class="qa-summary">
                <div class="qa-metrics">
                  <div class="qa-metric">
                    <div class="label">Total answers</div>
                    <div class="value">{{qa_total_answers}}</div>
                  </div>
                  <div class="qa-metric">
                    <div class="label">Empty answers</div>
                    <div class="value">{{qa_empty_count}}</div>
                  </div>
                  <div class="qa-metric">
                    <div class="label">Missing required</div>
                    <div class="value">{{qa_missing_required_count}}</div>
                  </div>
                  <div class="qa-metric">
                    <div class="label">Low confidence</div>
                    <div class="value">{{qa_low_conf_count}}</div>
                  </div>
                  <div class="qa-metric">
                    <div class="label">GPS status</div>
                    <div class="value">{{'Missing' if qa_gps_missing else ('Present' if qa_gps_present else '—')}}</div>
                  </div>
                </div>
                <div class="qa-block">
                  <div class="meta-label">Flags</div>
                  <div class="qa-flags" style="margin-top:8px;">{{qa_flags_html|safe}}</div>
                </div>
                {% if missing_required %}
                  <div class="qa-block">
                    <div class="meta-label">Missing required questions</div>
                    <ul class="muted" style="margin-top:8px; padding-left:18px;">
                      {% for q in missing_required %}
                        <li>{{q}}</li>
                      {% endfor %}
                    </ul>
                  </div>
                {% endif %}
                <div class="qa-block">
                  <div class="meta-label">Suspicious values</div>
                  <div class="meta-value">{{'Yes' if qa_suspicious else 'No'}}</div>
                </div>
              </div>
            </div>

            <div class="card">
              <div class="section-header">
                <h2 class="h2">Trust Signals</h2>
              </div>
              <div class="signal-list">
                <div class="signal-item">
                  <div>
                    <div class="label">Collected by</div>
                    <div class="value">{{h.enumerator_name or '—'}}</div>
                  </div>
                  <div class="value">Enumerator</div>
                </div>
                <div class="signal-item">
                  <div>
                    <div class="label">Coverage</div>
                    <div class="value">{{h.coverage_node_name or '—'}}</div>
                  </div>
                  <div class="value">Area</div>
                </div>
                <div class="signal-item">
                  <div>
                    <div class="label">GPS</div>
                    <div class="value">{{'Verified' if qa_gps_present else 'Missing'}}</div>
                  </div>
                  <div class="value">{{h.gps_accuracy or '—'}}</div>
                </div>
                <div class="signal-item">
                  <div>
                    <div class="label">Submission</div>
                    <div class="value">{{'Offline sync' if h.sync_source == 'OFFLINE_SYNC' else 'Live submission'}}</div>
                  </div>
                  <div class="value">{{h.synced_at or h.created_at}}</div>
                </div>
                <div class="signal-item">
                  <div>
                    <div class="label">Review status</div>
                    <div class="value">{{ review_badge[0] }}</div>
                  </div>
                  <div class="value">{{h.reviewed_at or '—'}}</div>
                </div>
                {% if review_reason %}
                <div class="signal-item">
                  <div>
                    <div class="label">Review note</div>
                    <div class="value">{{review_reason}}</div>
                  </div>
                  <div class="value">{{h.reviewed_by or '—'}}</div>
                </div>
                {% endif %}
              </div>
            </div>

            <div class="card">
              <div class="section-header">
                <h2 class="h2">Consent & Attestation</h2>
              </div>
              <div class="meta-card">
                <div class="meta-label">Consent obtained</div>
                <div class="meta-value">{{'Yes' if h.consent_obtained == 1 else ('No' if h.consent_obtained == 0 else '—')}}</div>
                <div class="meta-muted">{% if h.consent_timestamp %}{{h.consent_timestamp}}{% else %}—{% endif %}</div>
              </div>
              <div class="meta-card" style="margin-top:12px;">
                <div class="meta-label">Attestation</div>
                <div class="meta-value">{{h.attestation_text or '—'}}</div>
                <div class="meta-muted">{% if h.attestation_timestamp %}{{h.attestation_timestamp}}{% else %}—{% endif %}</div>
              </div>
            </div>
          </div>
        </div>
        """,
        kq=key_qs(),
        h=h,
        severity=severity,
        sev_label=sev_label,
        sev_class=sev_class,
        qa_total_answers=qa_total_answers,
        qa_empty_count=qa_empty_count,
        qa_missing_required_count=qa_missing_required_count,
        qa_low_conf_count=qa_low_conf_count,
        qa_gps_missing=qa_gps_missing,
        qa_gps_present=qa_gps_present,
        qa_suspicious=qa_suspicious,
        qa_flags_html=qa_flags_html,
        missing_required=missing_required,
        gps_link=gps_link,
        answer_count=answer_count,
        error_count=error_count,
        errors="".join(error_rows) if error_rows else "<tr><td style='padding:10px' colspan='3' class='muted'>No system errors logged.</td></tr>",
        ans="".join(ans_rows) if ans_rows else "<div class='muted'>No answers.</div>",
        review_badge=review_badge,
        review_reason=review_reason,
    )


@app.route("/ui/surveys/<int:survey_id>/delete", methods=["GET", "POST"])
def ui_survey_delete(survey_id):
    gate = admin_gate()
    if gate:
        return gate

    header, _, _ = sup.get_survey_details(int(survey_id))
    if not header:
        return ui_shell("Survey not found", "<div class='card'><h2>Survey not found</h2></div>"), 404

    err = ""
    if request.method == "POST":
        confirm = (request.form.get("confirm") or "").strip().upper()
        if confirm != "DELETE":
            err = "Type DELETE to confirm."
        else:
            with get_conn() as conn:
                conn.execute(
                    "UPDATE surveys SET deleted_at=?, updated_at=? WHERE id=?",
                    (now_iso(), now_iso(), int(survey_id)),
                )
                conn.commit()
            return redirect(url_for("ui_surveys") + key_qs())

    html = render_template_string(
        """
        <div class="card">
          <h1 class="h1">Delete Submission</h1>
          <div class="muted">This hides the submission without removing data.</div>
        </div>
        {% if err %}<div class="card" style="border-color: rgba(231, 76, 60, .35)"><b>Error:</b> {{err}}</div>{% endif %}
        <div class="card">
          <form method="POST" class="stack">
            <div>
              <label style="font-weight:800">Confirm delete</label>
              <input name="confirm" placeholder="Type DELETE to confirm" />
            </div>
            <div class="row">
              <button class="btn btn-primary" type="submit">Delete submission</button>
              <a class="btn" href="{{ url_for('ui_survey_detail', survey_id=survey_id) }}{{kq}}">Cancel</a>
            </div>
          </form>
        </div>
        """,
        survey_id=survey_id,
        err=err,
        kq=key_qs(),
    )
    return ui_shell("Delete Submission", html)


@app.route("/ui/qa")
def ui_qa():
    gate = admin_gate()
    if gate:
        return gate

    project_id = request.args.get("project_id") or ""
    project_id = int(project_id) if str(project_id).isdigit() else None
    project_id = resolve_project_context(project_id)
    if PROJECT_REQUIRED and project_id is None:
        projects = prj.list_projects(200, organization_id=current_org_id())
        options = "".join(
            [f"<option value='{p.get('id')}'>{html.escape(p.get('name') or 'Project')}</option>" for p in projects]
        )
        html_view = f"""
        <div class="card">
          <h1 class="h1">QA Alerts</h1>
          <div class="muted">Select a project to view QA alerts.</div>
          <form method="GET" style="margin-top:12px">
            <select name="project_id" required>
              <option value="">Choose project</option>
              {options}
            </select>
            <button class="btn btn-primary" type="submit" style="margin-left:8px">Open</button>
          </form>
        </div>
        """
        return ui_shell("QA Alerts", html_view, show_project_switcher=False)

    sup_id = current_supervisor_id()
    alerts = sup.qa_alerts_dashboard(
        limit=100,
        project_id=str(project_id) if project_id else "",
        supervisor_id=str(sup_id) if sup_id else "",
    )
    total_alerts = len(alerts)
    high_count = 0
    med_count = 0
    low_count = 0
    enum_set = set()
    facility_set = set()
    trs = []
    for idx, a in enumerate(alerts, start=1):
        sev = float(a.severity or 0)
        if sev >= 0.7:
            sev_label = "High"
            sev_class = "sev-high"
            high_count += 1
        elif sev >= 0.4:
            sev_label = "Medium"
            sev_class = "sev-med"
            med_count += 1
        else:
            sev_label = "Low"
            sev_class = "sev-low"
            low_count += 1

        if a.enumerator_name:
            enum_set.add(a.enumerator_name)
        if a.facility_name:
            facility_set.add(a.facility_name)

        flags = [f for f in (a.flags or []) if f]
        if flags:
            flag_badges = []
            for f in flags:
                f_upper = str(f).upper()
                cls = "flag-pill"
                if "GPS" in f_upper:
                    cls += " flag-gps"
                elif "DUPLICATE" in f_upper:
                    cls += " flag-dup"
                elif "MISSING" in f_upper:
                    cls += " flag-miss"
                else:
                    cls += " flag-gen"
                flag_badges.append(f"<span class=\"{cls}\">{f}</span>")
            flag_html = "".join(flag_badges)
        else:
            flag_html = "<span class=\"muted\">—</span>"

        trs.append(
            f"""
            <tr>
              <td style="width:90px"><span class="q-title">{idx}</span></td>
              <td>
                <div class="q-title">{a.facility_name}</div>
                <div class="q-meta">Survey #{a.survey_id}</div>
              </td>
              <td>
                <div class="row" style="align-items:center; gap:8px">
                  <span class="sev-pill {sev_class}">{sev_label}</span>
                  <div class="q-title">{a.enumerator_name}</div>
                </div>
                <div class="q-meta">Severity score {sev:.2f}</div>
              </td>
              <td>{flag_html}</td>
              <td style="width:160px"><a class="btn btn-sm" href="{url_for('ui_survey_detail', survey_id=a.survey_id)}{key_qs()}">View</a></td>
            </tr>
            """
        )

    return render_template_string(
        """
        <style>
          .qa-shell {
            display: grid;
            gap: 16px;
          }
          .qa-hero {
            background:
              radial-gradient(180px 140px at 10% 10%, rgba(14,116,144,.28), transparent 60%),
              linear-gradient(135deg, rgba(59,130,246,.18), rgba(15,18,34,.02));
            border:1px solid rgba(59,130,246,.25);
            border-radius:24px;
            padding:22px;
            box-shadow:0 20px 50px rgba(15,18,34,.12);
          }
          .qa-title {
            margin:0;
            font-size:26px;
            font-weight:800;
            letter-spacing:-.3px;
          }
          .qa-sub {
            color: var(--muted);
            margin-top: 6px;
          }
          .qa-stats {
            display:grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap:12px;
            margin-top:14px;
          }
          .qa-stat {
            border:1px solid var(--border);
            border-radius:16px;
            padding:12px 14px;
            background:var(--surface);
            box-shadow:0 10px 24px rgba(15,18,34,.06);
          }
          .qa-stat .label {
            font-size:12px;
            text-transform: uppercase;
            letter-spacing:.08em;
            color: var(--muted);
          }
          .qa-stat .value {
            font-size:22px;
            font-weight:800;
            margin-top:6px;
          }
          .qa-table {
            border-collapse: separate;
            border-spacing: 0 10px;
          }
          .qa-table thead th {
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: .08em;
            color: var(--muted);
            font-weight: 600;
            padding: 8px 14px;
          }
          .qa-table tbody tr {
            background: var(--surface);
            box-shadow: 0 10px 24px rgba(15,18,34,.06);
            transition: transform .12s ease, box-shadow .12s ease;
          }
          .qa-table tbody tr:hover {
            transform: translateY(-2px);
            box-shadow: 0 16px 34px rgba(15,18,34,.12);
          }
          .qa-table tbody td {
            padding: 14px 16px;
            vertical-align: top;
          }
          .qa-table tbody tr td:first-child {
            border-top-left-radius: 12px;
            border-bottom-left-radius: 12px;
          }
          .qa-table tbody tr td:last-child {
            border-top-right-radius: 12px;
            border-bottom-right-radius: 12px;
          }
          .q-title {
            font-weight: 600;
            font-size: 14px;
            color: var(--text);
          }
          .q-meta {
            color: var(--muted);
            font-size: 12px;
            margin-top: 6px;
          }
          .btn.btn-sm {
            padding: 8px 12px;
            font-size: 12px;
            border-radius: 10px;
            font-weight: 600;
          }
          .flag-pill {
            display:inline-flex;
            align-items:center;
            padding:6px 10px;
            border-radius:999px;
            font-size:12px;
            font-weight:700;
            border:1px solid var(--border);
            background:var(--surface-2);
            margin-right:6px;
            margin-bottom:6px;
          }
          .flag-gps { background: rgba(14,116,144,.14); border-color: rgba(14,116,144,.35); }
          .flag-dup { background: rgba(245,158,11,.18); border-color: rgba(245,158,11,.35); }
          .flag-miss { background: rgba(239,68,68,.18); border-color: rgba(239,68,68,.35); }
          .flag-gen { background: rgba(99,102,241,.14); border-color: rgba(99,102,241,.35); }
          .sev-pill {
            display:inline-flex;
            align-items:center;
            padding:4px 10px;
            border-radius:999px;
            font-size:11px;
            font-weight:800;
            border:1px solid transparent;
          }
          .sev-high { background: rgba(239,68,68,.15); border-color: rgba(239,68,68,.35); color:#b91c1c; }
          .sev-med { background: rgba(245,158,11,.15); border-color: rgba(245,158,11,.35); color:#b45309; }
          .sev-low { background: rgba(34,197,94,.15); border-color: rgba(34,197,94,.35); color:#15803d; }
          .qa-empty {
            border:1px dashed var(--border);
            border-radius:16px;
            padding:18px;
            text-align:center;
            color: var(--muted);
          }
        </style>

        <div class="qa-shell">
          <div class="qa-hero">
            <div class="row" style="justify-content:space-between; align-items:flex-start; gap:12px;">
              <div>
                <h2 class="qa-title">QA Alerts</h2>
                <div class="qa-sub">Review surveys with missing data, GPS gaps, or suspicious values.</div>
              </div>
              <div class="row" style="margin-left:auto; margin-top:18px; gap:8px;">
                <a class="btn btn-sm" href="/ui/review{{kq}}">Review Console</a>
                <a class="btn btn-sm" href="{{ url_for('ui_home') }}{{kq}}">Back to dashboard</a>
              </div>
            </div>
            <div class="qa-stats">
              <div class="qa-stat">
                <div class="label">Total alerts</div>
                <div class="value">{{total_alerts}}</div>
              </div>
              <div class="qa-stat">
                <div class="label">High severity</div>
                <div class="value">{{high_count}}</div>
              </div>
              <div class="qa-stat">
                <div class="label">Medium severity</div>
                <div class="value">{{med_count}}</div>
              </div>
              <div class="qa-stat">
                <div class="label">Low severity</div>
                <div class="value">{{low_count}}</div>
              </div>
              <div class="qa-stat">
                <div class="label">Enumerators</div>
                <div class="value">{{enum_count}}</div>
              </div>
              <div class="qa-stat">
                <div class="label">Facilities</div>
                <div class="value">{{facility_count}}</div>
              </div>
            </div>
          </div>

          <div style="overflow:auto;">
            <table class="table qa-table">
              <thead>
                <tr>
                  <th style="width:90px">No.</th>
                  <th>Facility</th>
                  <th>Enumerator</th>
                  <th>Flags</th>
                  <th style="width:160px">Action</th>
                </tr>
              </thead>
              <tbody>
                {{rows|safe}}
              </tbody>
            </table>
          </div>
        </div>
        """,
        kq=key_qs(),
        total_alerts=total_alerts,
        high_count=high_count,
        med_count=med_count,
        low_count=low_count,
        enum_count=len(enum_set),
        facility_count=len(facility_set),
        rows="".join(
            trs) if trs else "<tr><td colspan='5'><div class='qa-empty'>No QA alerts yet. You're all clear.</div></td></tr>",
    )


@app.route("/ui/review", methods=["GET"])
def ui_review_console():
    gate = admin_gate()
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    is_admin = bool(ADMIN_KEY and request.args.get("key") == ADMIN_KEY)
    org_id = current_org_id() if (REQUIRE_SUPERVISOR_KEY and not is_admin) else None
    if org_id is None:
        try:
            sess_org = session.get("org_id")
            org_id = int(sess_org) if sess_org is not None else None
        except Exception:
            org_id = None
    project_id = (request.args.get("project_id") or "").strip()
    project_id_int = int(project_id) if project_id.isdigit() else None
    if project_id_int is None and org_id is not None:
        try:
            project_id_int = int(prj.get_default_project_id(int(org_id)))
        except Exception:
            project_id_int = None
    view = (request.args.get("view") or "pending").strip().lower()

    try:
        projects = prj.list_projects(200, organization_id=org_id)
    except Exception:
        projects = []
    project_options = ["<option value=''>All projects</option>"]
    for p in projects:
        pid = int(p.get("id") or 0)
        if not pid:
            continue
        selected = "selected" if project_id_int and pid == project_id_int else ""
        project_options.append(f"<option value='{pid}' {selected}>{p.get('name')}</option>")

    sup_id = current_supervisor_id()
    rows = sup.filter_surveys(
        status="COMPLETED",
        project_id=str(project_id_int) if project_id_int else "",
        supervisor_id=str(sup_id) if sup_id else "",
        limit=300,
    )

    ids = [int(r[0]) for r in rows if r and str(r[0]).isdigit()]
    meta_map = {}
    if ids:
        placeholders = ",".join(["?"] * len(ids))
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                f"""
                SELECT id, review_status, review_reason, reviewed_at, reviewed_by, sync_source, gps_lat, gps_lng
                FROM surveys
                WHERE id IN ({placeholders})
                """,
                tuple(ids),
            )
            meta_map = {int(r["id"]): dict(r) for r in cur.fetchall() if r["id"] is not None}

    qa_alerts = sup.qa_alerts_dashboard_filtered(
        limit=500,
        project_id=str(project_id_int) if project_id_int else "",
        supervisor_id=str(sup_id) if sup_id else "",
    )
    qa_map = {int(a.survey_id): a for a in qa_alerts}

    def _review_status(row_id: int) -> str:
        meta = meta_map.get(int(row_id)) if meta_map else {}
        rs = (meta.get("review_status") or "PENDING").upper()
        return rs

    def _is_attention(row_id: int) -> bool:
        if row_id in qa_map:
            return True
        return _review_status(row_id) == "REVISION"

    # Summary counts
    total_completed = len(rows)
    approved_count = sum(1 for sid in ids if _review_status(sid) == "APPROVED")
    rejected_count = sum(1 for sid in ids if _review_status(sid) == "REJECTED")
    revision_count = sum(1 for sid in ids if _review_status(sid) == "REVISION")
    pending_count = sum(1 for sid in ids if _review_status(sid) in ("PENDING", "", None))
    attention_count = sum(1 for sid in ids if _is_attention(sid))

    def _passes_view(sid: int) -> bool:
        rs = _review_status(sid)
        flags = qa_map.get(int(sid)).flags if sid in qa_map else []
        if view in ("all", ""):
            return True
        if view == "pending":
            return rs in ("PENDING", "", None)
        if view == "approved":
            return rs == "APPROVED"
        if view == "rejected":
            return rs == "REJECTED"
        if view == "revision":
            return rs == "REVISION"
        if view == "attention":
            return _is_attention(sid)
        if view == "gps":
            return any((f or "").upper() == "GPS_MISSING" for f in (flags or []))
        if view == "missing":
            return any("MISSING" in (f or "").upper() or "EMPTY" in (f or "").upper() for f in (flags or []))
        if view == "lowconf":
            return any((f or "").upper() == "LOW_CONFIDENCE" for f in (flags or []))
        return True

    def _review_badge(status: str):
        return {
            "APPROVED": ("Approved", "review-approved"),
            "REJECTED": ("Rejected", "review-rejected"),
            "REVISION": ("Needs revision", "review-revision"),
            "PENDING": ("Pending review", "review-pending"),
        }.get(status, (status.title(), "review-pending"))

    trs = []
    for (sid, facility_name, tplid, survey_type, enum_name, status, created_at) in rows:
        if not _passes_view(int(sid)):
            continue
        qa = qa_map.get(int(sid))
        flags = ", ".join(qa.flags) if qa else "—"
        sev = f"{qa.severity:.2f}" if qa else "—"
        meta = meta_map.get(int(sid)) if meta_map else {}
        rs = _review_status(int(sid))
        badge = _review_badge(rs)
        reason = html.escape(str(meta.get("review_reason") or ""))
        trs.append(
            f"""
            <tr>
              <td>#{sid}</td>
              <td>
                <div class="q-title">{facility_name}</div>
                <div class="q-meta">{survey_type}</div>
              </td>
              <td>
                <div class="q-title">{enum_name or '—'}</div>
                <div class="q-meta">{created_at}</div>
              </td>
              <td>
                <div class="q-meta">Flags: {html.escape(flags)}</div>
                <div class="q-meta">Severity: {sev}</div>
              </td>
              <td>
                <span class="review-pill {badge[1]}">{badge[0]}</span>
                {f"<div class='q-meta' style='margin-top:6px'>Note: {reason}</div>" if reason else ""}
              </td>
              <td style="min-width:260px">
                <form method="POST" action="/ui/review/action{key_q}" class="review-actions">
                  <input type="hidden" name="survey_id" value="{sid}" />
                  <input type="text" name="reason" placeholder="Reason (optional)" />
                  <div class="row" style="gap:6px; margin-top:6px;">
                    <button class="btn btn-sm" name="action" value="approve" type="submit">Approve</button>
                    <button class="btn btn-sm" name="action" value="reject" type="submit">Reject</button>
                    <button class="btn btn-sm" name="action" value="revision" type="submit">Send back</button>
                    <a class="btn btn-sm" href="/ui/surveys/{sid}{key_q}">View</a>
                  </div>
                </form>
              </td>
            </tr>
            """
        )

    def _view_btn(label, value):
        qs = f"?view={value}"
        if project_id_int:
            qs += f"&project_id={project_id_int}"
        if ADMIN_KEY:
            qs += f"&key={ADMIN_KEY}"
        return f"<a class='btn btn-sm {'btn-primary' if view==value else ''}' href='/ui/review{qs}'>{label}</a>"

    html_page = f"""
    <style>
      .review-hero{{
        background:
          radial-gradient(200px 140px at 12% 12%, rgba(124,58,237,.25), transparent 60%),
          linear-gradient(135deg, rgba(124,58,237,.18), rgba(15,18,34,.02));
        border:1px solid rgba(124,58,237,.28);
        border-radius:20px;
        padding:20px;
        box-shadow:0 16px 40px rgba(15,18,34,.08);
      }}
      .review-title{{margin:0; font-size:24px; font-weight:800; letter-spacing:-.2px;}}
      .review-stats{{display:grid; grid-template-columns: repeat(auto-fit, minmax(140px,1fr)); gap:12px; margin-top:12px;}}
      .review-stat{{border:1px solid var(--border); border-radius:14px; padding:12px; background:var(--surface);}}
      .review-stat .label{{font-size:11px; text-transform:uppercase; letter-spacing:.1em; color:var(--muted); font-weight:700;}}
      .review-stat .value{{font-size:18px; font-weight:800; margin-top:6px; color:var(--primary);}}
      .review-pill{{display:inline-flex; align-items:center; padding:6px 10px; border-radius:999px; font-size:12px; font-weight:700; border:1px solid transparent;}}
      .review-approved{{background:rgba(22,163,74,.14); border-color:rgba(22,163,74,.35); color:#15803d;}}
      .review-rejected{{background:rgba(220,38,38,.14); border-color:rgba(220,38,38,.35); color:#b91c1c;}}
      .review-revision{{background:rgba(245,158,11,.18); border-color:rgba(245,158,11,.35); color:#b45309;}}
      .review-pending{{background:rgba(148,163,184,.2); border-color:rgba(148,163,184,.45); color:#475569;}}
      .review-actions input{{width:100%; padding:8px 10px; border-radius:10px; border:1px solid var(--border);}}
      .q-meta{{color:var(--muted); font-size:12px; margin-top:4px;}}
    </style>

    <div class="review-hero">
      <div class="row" style="justify-content:space-between; align-items:flex-start; gap:12px;">
        <div>
          <h2 class="review-title">Review Console</h2>
          <div class="muted">Approve, reject, or send back submissions that need attention.</div>
        </div>
        <a class="btn btn-sm" href="/ui/dashboard{key_q}">Back to dashboard</a>
      </div>
      <div class="review-stats">
        <div class="review-stat"><div class="label">Completed</div><div class="value">{total_completed}</div></div>
        <div class="review-stat"><div class="label">Pending</div><div class="value">{pending_count}</div></div>
        <div class="review-stat"><div class="label">Needs attention</div><div class="value">{attention_count}</div></div>
        <div class="review-stat"><div class="label">Approved</div><div class="value">{approved_count}</div></div>
        <div class="review-stat"><div class="label">Rejected</div><div class="value">{rejected_count}</div></div>
        <div class="review-stat"><div class="label">Revision</div><div class="value">{revision_count}</div></div>
      </div>
    </div>

    <div class="card" style="margin-top:16px">
      <div class="row" style="justify-content:space-between; align-items:center; gap:12px;">
        <div class="row" style="gap:8px; flex-wrap:wrap;">
          {_view_btn("Pending", "pending")}
          {_view_btn("Needs attention", "attention")}
          {_view_btn("GPS issues", "gps")}
          {_view_btn("Missing required", "missing")}
          {_view_btn("Low confidence", "lowconf")}
          {_view_btn("Approved", "approved")}
          {_view_btn("Rejected", "rejected")}
          {_view_btn("Revision", "revision")}
          {_view_btn("All", "all")}
        </div>
        <div>
          <select id="reviewProjectSelect">
            {''.join(project_options)}
          </select>
        </div>
      </div>
      <div style="margin-top:12px; overflow:auto;">
        <table class="table">
          <thead>
            <tr>
              <th style="width:90px">Survey</th>
              <th>Facility</th>
              <th>Enumerator</th>
              <th>QA</th>
              <th>Status</th>
              <th style="width:320px">Actions</th>
            </tr>
          </thead>
          <tbody>
            {("".join(trs) if trs else "<tr><td colspan='6' class='muted' style='padding:18px'>No submissions found for this filter.</td></tr>")}
          </tbody>
        </table>
      </div>
    </div>
    <script>
      (function(){{
        const sel = document.getElementById("reviewProjectSelect");
        if(!sel) return;
        const key = "{ADMIN_KEY}" || "";
        sel.addEventListener("change", (e)=>{{
          const pid = e.target.value;
          const params = new URLSearchParams();
          if(pid) params.set("project_id", pid);
          params.set("view", "{view}");
          if(key) params.set("key", key);
          window.location.href = "/ui/review?" + params.toString();
        }});
      }})();
    </script>
    """
    return ui_shell("Review Console", html_page, show_project_switcher=False)


@app.route("/ui/review/action", methods=["POST"])
def ui_review_action():
    gate = admin_gate()
    if gate:
        return gate
    survey_id = request.form.get("survey_id") or ""
    action = (request.form.get("action") or "").strip().upper()
    reason = (request.form.get("reason") or "").strip()
    if not survey_id.isdigit():
        return ui_shell("Review", "<div class='card'><h2>Invalid survey id</h2></div>"), 400
    if action not in ("APPROVE", "REJECT", "REVISION"):
        return ui_shell("Review", "<div class='card'><h2>Invalid action</h2></div>"), 400

    status_map = {"APPROVE": "APPROVED", "REJECT": "REJECTED", "REVISION": "REVISION"}
    review_status = status_map[action]
    reviewed_by = "Supervisor"
    if ADMIN_KEY:
        reviewed_by = "Admin"

    if "review_status" not in surveys_cols():
        return ui_shell("Review", "<div class='card'><h2>Review columns missing. Run init_db().</h2></div>"), 400

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE surveys
            SET review_status=?, review_reason=?, reviewed_at=?, reviewed_by=?
            WHERE id=?
            """,
            (review_status, reason or None, now_iso(), reviewed_by, int(survey_id)),
        )
        conn.commit()

    return redirect(url_for("ui_review_console") + (f"?key={ADMIN_KEY}" if ADMIN_KEY else ""))


@app.route("/ui/errors")
def ui_errors():
    gate = admin_gate()
    if gate:
        return gate

    project_id = request.args.get("project_id") or ""
    project_id = int(project_id) if str(project_id).isdigit() else None

    where = ""
    params: list = []
    if project_id is not None:
        where = "WHERE e.project_id=?"
        params.append(int(project_id))

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
              e.id,
              e.project_id,
              p.name AS project_name,
              e.template_id,
              t.name AS template_name,
              e.survey_id,
              e.error_type,
              e.error_message,
              e.context_json,
              e.created_at
            FROM submission_errors e
            LEFT JOIN projects p ON p.id = e.project_id
            LEFT JOIN survey_templates t ON t.id = e.template_id
            {where}
            ORDER BY e.id DESC
            LIMIT 200
            """,
            tuple(params),
        )
        rows = cur.fetchall()

    trs = []
    for r in rows:
        ctx = {}
        try:
            ctx = json.loads(r["context_json"] or "{}")
        except Exception:
            ctx = {}
        meta_bits = []
        if ctx.get("facility_name"):
            meta_bits.append(f"Facility: {ctx.get('facility_name')}")
        if ctx.get("enumerator_name"):
            meta_bits.append(f"Enumerator: {ctx.get('enumerator_name')}")
        if ctx.get("enumerator_code"):
            meta_bits.append(f"Code: {ctx.get('enumerator_code')}")
        if ctx.get("coverage_node_id"):
            meta_bits.append(f"Coverage node: {ctx.get('coverage_node_id')}")
        meta = " · ".join(meta_bits) if meta_bits else "—"
        error_type = (r["error_type"] or "system").lower()
        type_class = "pill-system" if error_type != "validation" else "pill-validation"
        trs.append(
            f"""
            <tr>
              <td><span class="template-id">#{r['id']}</span></td>
              <td>
                <div class="template-name">{r['project_name'] or '—'}</div>
                <div class="template-desc">Template: {r['template_name'] or '—'}</div>
              </td>
              <td class="muted">{meta}</td>
              <td><span class="pill {type_class}">{error_type}</span></td>
              <td class="muted">{r['error_message']}</td>
              <td class="muted">{r['created_at']}</td>
            </tr>
            """
        )

    html = f"""
    <style>
      .pill{{
        display:inline-flex;
        align-items:center;
        padding:6px 10px;
        border-radius:999px;
        font-size:12px;
        font-weight:700;
        border:1px solid var(--border);
      }}
      .pill-system{{
        background:rgba(231, 76, 60, .12);
        border-color:rgba(231, 76, 60, .45);
        color:#c0392b;
      }}
      .pill-validation{{
        background:rgba(245, 158, 11, .12);
        border-color:rgba(245, 158, 11, .45);
        color:#b45309;
      }}
    </style>

    <div class="card">
      <div class="row" style="justify-content:space-between; align-items:flex-start;">
        <div>
          <h1 class="h1">Submission Errors</h1>
          <div class="muted">Track validation and system issues from share-link submissions.</div>
        </div>
        <a class="btn" href="{url_for('ui_home')}{key_qs()}">Back</a>
      </div>
    </div>

    <div class="card" style="margin-top:16px;">
      <table class="table">
        <thead>
          <tr>
            <th style="width:90px">ID</th>
            <th>Project</th>
            <th>Context</th>
            <th style="width:140px">Type</th>
            <th>Error</th>
            <th style="width:160px">When</th>
          </tr>
        </thead>
        <tbody>
          {("".join(trs) if trs else "<tr><td colspan='6' class='muted' style='padding:18px'>No submission errors logged.</td></tr>")}
        </tbody>
      </table>
    </div>
    """
    return ui_shell("Submission Errors", html)


# ---------------------------
# Exports (Supervisor)
# ---------------------------
@app.route("/ui/exports")
def ui_exports():
    gate = admin_gate()
    if gate:
        return gate
    project_id = request.args.get("project_id") or ""
    project_id = int(project_id) if str(project_id).isdigit() else None
    project_id = resolve_project_context(project_id)
    project_options = ""
    if PROJECT_REQUIRED and project_id is None:
        projects = prj.list_projects(200, organization_id=current_org_id())
        options = "".join(
            [f"<option value='{p.get('id')}'>{html.escape(p.get('name') or 'Project')}</option>" for p in projects]
        )
        html_view = f"""
        <div class="card">
          <h1 class="h1">Exports</h1>
          <div class="muted">Select a project to export data.</div>
          <form method="GET" style="margin-top:12px">
            <select name="project_id" required>
              <option value="">Choose project</option>
              {options}
            </select>
            <button class="btn btn-primary" type="submit" style="margin-left:8px">Open</button>
          </form>
        </div>
        """
        return ui_shell("Exports", html_view, show_project_switcher=False)
    try:
        projects = prj.list_projects(200, organization_id=current_org_id())
        for p in projects:
            pid = int(p.get("id"))
            status = (p.get("status") or "ACTIVE").upper()
            status_text = " [Archived]" if status == "ARCHIVED" else (" [Draft]" if status == "DRAFT" else "")
            selected = "selected" if project_id is not None and int(project_id) == pid else ""
            project_options += f"<option value='{pid}' {selected}>{p.get('name')}{status_text}</option>"
    except Exception:
        project_options = ""
    template_id = request.args.get("template_id") or ""
    enumerator = request.args.get("enumerator") or ""
    start_date = request.args.get("start_date") or ""
    end_date = request.args.get("end_date") or ""
    allow_restricted = request.args.get("allow_restricted") == "1"
    proj_q = f"?project_id={project_id}" if project_id and not ADMIN_KEY else (
        f"&project_id={project_id}" if project_id else "")
    filter_q = proj_q
    for key, val in (("template_id", template_id), ("enumerator", enumerator), ("start_date", start_date), ("end_date", end_date)):
        if val:
            filter_q += (f"&{key}={val}" if filter_q else f"?{key}={val}")
    if allow_restricted:
        filter_q += ("&allow_restricted=1" if filter_q else "?allow_restricted=1")
        proj_q += ("&allow_restricted=1" if proj_q else "?allow_restricted=1")
    html = render_template_string(
        """
        <style>
          .export-hero {
            background:
              radial-gradient(180px 140px at 10% 10%, rgba(124,58,237,.28), transparent 60%),
              linear-gradient(135deg, rgba(124,58,237,.18), rgba(15,18,34,.02));
            border:1px solid rgba(124,58,237,.28);
            border-radius:20px;
            padding:20px;
            box-shadow:0 16px 40px rgba(15,18,34,.08);
          }
          .export-title {
            margin:0;
            font-size:24px;
            font-weight:800;
            letter-spacing:-.2px;
          }
          .proj-inline-switcher{
            display:inline-flex;
            align-items:center;
            gap:8px;
            margin-top:10px;
            padding:6px 10px;
            border-radius:12px;
            border:1px solid var(--border);
            background:var(--surface);
          }
          .proj-inline-switcher label{
            font-size:11px;
            color:var(--muted);
            font-weight:700;
          }
          .proj-inline-switcher select{
            border:none;
            padding:6px 8px;
            font-size:12px;
            background:transparent;
            color:var(--text);
          }
          .export-grid {
            display:grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap:14px;
            margin-top:16px;
          }
          .export-card {
            border:1px solid var(--border);
            background:var(--surface);
            border-radius:16px;
            padding:16px;
            box-shadow:0 10px 24px rgba(15,18,34,.06);
            display:flex;
            flex-direction:column;
            gap:10px;
          }
          .export-label {
            font-weight:700;
            font-size:14px;
            color:var(--text);
          }
          .export-meta {
            color:var(--muted);
            font-size:12px;
          }
          .btn.btn-sm {
            padding: 8px 12px;
            font-size: 12px;
            border-radius: 10px;
            font-weight: 600;
          }
        </style>

        <div class="export-hero">
          <div class="row" style="justify-content:space-between; align-items:flex-start; gap:12px;">
            <div>
              <h2 class="export-title">Exports</h2>
              <div class="muted" style="margin-top:6px">Download survey data for analysis or backup.</div>
              <div class="proj-inline-switcher">
                <label>Project</label>
                <select id="projectSwitcherInline">
                  <option value="">All</option>
                  {{project_options|safe}}
                </select>
              </div>
            </div>
            <a class="btn btn-sm" style="margin-left:auto; margin-top:18px;" href="{{ url_for('ui_home') }}{{kq}}">Back to dashboard</a>
          </div>
        </div>

        <div class="card" style="margin-top:16px">
          <form method="GET" class="row" style="gap:12px; flex-wrap:wrap;">
            <input type="hidden" name="project_id" value="{{ project_id }}" />
            <div style="min-width:180px; flex:1">
              <label class="export-label">Template ID</label>
              <input name="template_id" value="{{ template_id }}" placeholder="e.g., 12" />
            </div>
            <div style="min-width:200px; flex:1">
              <label class="export-label">Enumerator</label>
              <input name="enumerator" value="{{ enumerator }}" placeholder="Name contains..." />
            </div>
            <div style="min-width:180px;">
              <label class="export-label">Start date</label>
              <input name="start_date" type="date" value="{{ start_date }}" />
            </div>
            <div style="min-width:180px;">
              <label class="export-label">End date</label>
              <input name="end_date" type="date" value="{{ end_date }}" />
            </div>
            <div style="align-self:flex-end">
              <button class="btn btn-sm" type="submit">Apply filters</button>
            </div>
            <div style="align-self:flex-end">
              <label class="row" style="gap:8px; font-size:12px;">
                <input type="checkbox" name="allow_restricted" value="1" style="width:auto" {% if allow_restricted %}checked{% endif %}/>
                <span>Include restricted data</span>
              </label>
            </div>
          </form>
        </div>

        <div class="export-grid">
          <div class="export-card">
            <div class="export-label">Facilities (CSV)</div>
            <div class="export-meta">Master list of facilities.</div>
            <a class="btn btn-sm export-link" href="{{ url_for('ui_export_facilities_csv') }}{{kq}}{{proj_q}}">Download CSV</a>
          </div>
          <div class="export-card">
            <div class="export-label">Surveys + Answers (CSV)</div>
            <div class="export-meta">SPSS-ready CSV · Stata-ready CSV · Excel-friendly CSV.</div>
            <a class="btn btn-sm export-link" href="{{ url_for('ui_export_surveys_csv') }}{{kq}}{{proj_q}}">Download CSV</a>
          </div>
          <div class="export-card">
            <div class="export-label">Surveys + Answers (JSON)</div>
            <div class="export-meta">Nested JSON for analysis.</div>
            <a class="btn btn-sm export-link" href="{{ url_for('ui_export_surveys_json') }}{{kq}}{{proj_q}}">Download JSON</a>
          </div>
          <div class="export-card">
            <div class="export-label">Research Export (CSV)</div>
            <div class="export-meta">SPSS-ready CSV · Stata-ready CSV · Excel-friendly CSV.</div>
            <a class="btn btn-sm export-link" href="{{ url_for('ui_export_surveys_flat_csv') }}{{kq}}{{filter_q}}">Download CSV</a>
          </div>
          <div class="export-card">
            <div class="export-label">Research Export (JSON)</div>
            <div class="export-meta">One row per survey, clean headers.</div>
            <a class="btn btn-sm export-link" href="{{ url_for('ui_export_surveys_flat_json') }}{{kq}}{{filter_q}}">Download JSON</a>
          </div>
          <div class="export-card">
            <div class="export-label">Metadata Sheet (CSV)</div>
            <div class="export-meta">Project info, template versions, enumerators, coverage.</div>
            <a class="btn btn-sm export-link" href="{{ url_for('ui_export_metadata_csv') }}{{kq}}{{proj_q}}">Download CSV</a>
          </div>
          <div class="export-card">
            <div class="export-label">Single Survey (JSON)</div>
            <div class="export-meta">Export one survey by ID.</div>
            <form method="GET" action="{{ url_for('ui_export_one_survey_json') }}{{kq}}" class="row" style="gap:8px">
              <input name="survey_id" placeholder="Survey ID" style="flex:1; min-width:120px" />
              {% if allow_restricted %}<input type="hidden" name="allow_restricted" value="1" />{% endif %}
              <button class="btn btn-sm" type="submit">Download</button>
            </form>
          </div>
        </div>

        <p class="muted" style="margin-top:14px">Files are generated on demand and downloaded to your device.</p>

        <script>
          (function(){
            const form = document.querySelector("form[method='GET']");
            const restricted = form ? form.querySelector("input[name='allow_restricted']") : null;
            const switcher = document.getElementById("projectSwitcherInline");
            if(switcher){
              switcher.addEventListener("change", (e)=>{
                const val = e.target.value;
                if(!val){
                  window.location.href = "/ui/exports{{kq}}";
                } else {
                  window.location.href = "/ui/exports?project_id=" + val + "{{kq}}";
                }
              });
            }
            if(form && restricted){
              form.addEventListener("submit", (e)=>{
                if(restricted.checked){
                  const ok = confirm("Include restricted data in exports? This may expose sensitive fields.");
                  if(!ok) e.preventDefault();
                }
              });
            }
            const links = document.querySelectorAll(".export-link");
            links.forEach(link=>{
              link.addEventListener("click", (e)=>{
                if(restricted && restricted.checked){
                  const ok = confirm("Include restricted data in exports? This may expose sensitive fields.");
                  if(!ok) e.preventDefault();
                }
              });
            });
          })();
        </script>
        """,
        kq=key_qs(),
        proj_q=proj_q,
        filter_q=filter_q,
        project_id=project_id,
        project_options=project_options,
        template_id=template_id,
        enumerator=enumerator,
        start_date=start_date,
        end_date=end_date,
        allow_restricted=allow_restricted,
    )
    return ui_shell("Exports", html, show_project_switcher=False)


@app.route("/ui/admin", methods=["GET", "POST"])
def ui_admin():
    gate = admin_gate(allow_supervisor=False)
    if gate:
        return gate

    key_q = f"?key={ADMIN_KEY}" if ADMIN_KEY else ""
    msg = ""
    err = ""
    if request.method == "POST":
        err = "Action not supported on this page."
    db_path = config.DB_PATH
    db_exists = os.path.exists(db_path)
    db_size = os.path.getsize(db_path) if db_exists else 0
    db_size_mb = round(db_size / (1024 * 1024), 2) if db_size else 0

    table_rows = []
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name ASC")
            tables = [r["name"] for r in cur.fetchall()]
    except Exception:
        tables = []

    for t in tables:
        cols = []
        try:
            cols = sorted(list(_table_cols(t)))
        except Exception:
            cols = []
        table_rows.append(
            f"""
            <details class="schema-row">
              <summary><b>{html.escape(t)}</b> <span class="muted">({len(cols)} cols)</span></summary>
              <div class="schema-cols">{", ".join([html.escape(c) for c in cols])}</div>
            </details>
            """
        )

    orgs = prj.list_organizations(200)

    html_page = f"""
    <style>
      .admin-stage{{background:radial-gradient(1200px 420px at 10% -10%, rgba(124,58,237,.18), transparent 60%), #eef1f7; padding:18px; border-radius:28px;}}
      .admin-board{{display:grid; grid-template-columns:64px 1fr; gap:18px; background:var(--surface); border-radius:28px; padding:18px; border:1px solid rgba(124,58,237,.12); box-shadow:0 20px 50px rgba(15,18,34,.12);}}
      .admin-rail{{display:flex; flex-direction:column; align-items:center; gap:12px; background:linear-gradient(180deg, var(--primary), #9B7BFF); border-radius:22px; padding:14px 10px; color:#fff;}}
      .admin-rail .rail-item{{width:38px; height:38px; border-radius:12px; background:rgba(255,255,255,.2); display:grid; place-items:center; font-weight:800;}}
      .admin-rail .rail-item.active{{background:#fff; color:var(--primary); box-shadow:0 8px 18px rgba(0,0,0,.2);}}
      .admin-rail a{{text-decoration:none; color:inherit;}}
      .admin-rail .rail-item:hover{{background:rgba(255,255,255,.35);}}
      .admin-main{{display:grid; gap:16px;}}
      .admin-stage .card{{border-radius:18px; border:1px solid rgba(124,58,237,.12); background:var(--surface); box-shadow:0 18px 40px rgba(15,18,34,.08);}}
      .admin-grid{{display:grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap:16px;}}
      .admin-kpi{{border-radius:18px; padding:16px; background:linear-gradient(135deg, var(--primary), #9B7BFF); color:#fff; box-shadow:0 16px 34px rgba(124,58,237,.25); border:1px solid rgba(124,58,237,.15);}}
      .admin-kpi:nth-child(2){{background:#f3f0ff; color:#1f2937; box-shadow:0 12px 28px rgba(15,18,34,.06);}}
      .admin-kpi:nth-child(3){{background:#ffffff; color:#1f2937; box-shadow:0 12px 28px rgba(15,18,34,.06);}}
      .admin-kpi .label{{font-size:11px; color:rgba(255,255,255,.85); text-transform:uppercase; letter-spacing:.08em;}}
      .admin-kpi:nth-child(2) .label, .admin-kpi:nth-child(3) .label{{color:#4b5563;}}
      .admin-kpi .muted{{color:rgba(255,255,255,.92);}}
      .admin-kpi:nth-child(2) .muted, .admin-kpi:nth-child(3) .muted{{color:#4b5563;}}
      .admin-hero .muted{{color:#4b5563;}}
      .admin-kpi .value{{font-size:18px; font-weight:800; margin-top:6px;}}
      .admin-wrap{{word-break:break-all; overflow-wrap:anywhere;}}
      .schema-row{{margin-top:10px; border:1px solid var(--border); border-radius:12px; padding:10px; background:var(--surface-2);}}
      .schema-row summary{{cursor:pointer;}}
      .schema-cols{{margin-top:6px; font-size:12px; color:var(--muted);}}
    </style>

    <div class="admin-stage">
      <div class="admin-board">
        <aside class="admin-rail">
          <a class="rail-item active" href="/ui{key_q}" title="Quicklinks">⌂</a>
          <a class="rail-item" href="#admin-actions" title="Quick actions">⚡</a>
          <a class="rail-item" href="#admin-demo" title="Demo mode">🗂</a>
          <a class="rail-item" href="#admin-schema" title="Schema status">🔒</a>
          <a class="rail-item" href="/ui{key_q}" title="Back to dashboard">⎋</a>
        </aside>
        <div class="admin-main">
    <div class="card" id="admin-overview">
      <div class="row" style="justify-content:space-between; align-items:flex-start;">
        <div>
          <h1 class="h1">Admin Panel</h1>
          <div class="muted">System status, backups, and schema information.</div>
        </div>
        <a class="btn" href="/ui{key_q}">Back to dashboard</a>
      </div>
    </div>

    {"<div class='card' style='border-color: rgba(46, 204, 113, .35)'><b>Success:</b> " + msg + "</div>" if msg else ""}
    {"<div class='card' style='border-color: rgba(231, 76, 60, .35)'><b>Error:</b> " + err + "</div>" if err else ""}

    <div class="admin-grid" style="margin-top:16px">
      <div class="admin-kpi">
        <div class="label">Environment</div>
        <div class="value">{APP_ENV.upper()}</div>
        <div class="muted" style="margin-top:6px">App version: {APP_VERSION}</div>
      </div>
      <div class="admin-kpi">
        <div class="label">Database</div>
        <div class="value admin-wrap">{html.escape(db_path)}</div>
        <div class="muted" style="margin-top:6px">{'Found' if db_exists else 'Missing'} · {db_size_mb} MB</div>
      </div>
      <div class="admin-kpi">
        <div class="label">Tables</div>
        <div class="value">{len(tables)}</div>
        <div class="muted" style="margin-top:6px">Schema overview below</div>
      </div>
    </div>

    <div class="card" id="admin-actions" style="margin-top:16px">
      <h2 class="h2">Quick actions</h2>
      <div class="row" style="margin-top:12px; flex-wrap:wrap;">
        <a class="btn" href="/ui/exports{key_q}">Open exports</a>
        <a class="btn btn-primary" href="/ui/admin/backup{key_q}">Download DB backup</a>
      </div>
    </div>

    <div class="card" id="admin-demo" style="margin-top:16px">
      <h2 class="h2">Demo mode</h2>
      <div class="muted">Generate a demo project, enumerators, assignments, and submissions for pitching or training.</div>
      <form method="POST" action="/ui/admin/demo{key_q}" style="margin-top:12px">
        <button class="btn btn-primary" type="submit">Create demo data</button>
      </form>
    </div>

    <div class="card" id="admin-schema" style="margin-top:16px">
      <h2 class="h2">Schema status</h2>
      <div class="muted">Tables and columns detected in the active database.</div>
      <div style="margin-top:12px">
        {("".join(table_rows) if table_rows else "<div class='muted'>No tables found.</div>")}
      </div>
        </div>
      </div>
    </div>
    """
    return ui_shell("Admin", html_page, show_project_switcher=False)


@app.route("/ui/admin/backup")
def ui_admin_backup():
    gate = admin_gate(allow_supervisor=False)
    if gate:
        return gate
    db_path = config.DB_PATH
    if not os.path.exists(db_path):
        return ui_shell(
            "Admin",
            "<div class='card'><h2>Backup failed</h2><div class='muted'>Database file not found.</div></div>",
            show_project_switcher=False,
        ), 404
    filename = f"openfield_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    path = os.path.join(EXPORT_DIR, filename)
    try:
        import sqlite3
        src = sqlite3.connect(db_path)
        dest = sqlite3.connect(path)
        src.backup(dest)
        dest.close()
        src.close()
    except Exception as e:
        return ui_shell(
            "Admin",
            f"<div class='card'><h2>Backup failed</h2><div class='muted'>{html.escape(str(e))}</div></div>",
            show_project_switcher=False,
        ), 500
    return send_file(
        path,
        mimetype="application/octet-stream",
        as_attachment=True,
        download_name=filename,
    )


def _create_demo_data() -> Dict[str, Any]:
    # Avoid duplicate demo project
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM projects WHERE name LIKE 'Demo Field Ops%' LIMIT 1")
        existing = cur.fetchone()
        if existing:
            return {"ok": True, "message": "Demo data already exists.", "project_id": int(existing["id"])}

    project_name = f"Demo Field Ops — {datetime.now().strftime('%b %d, %Y')}"
    project_id = prj.create_project(project_name, "Sample data for demo & training", template_id=None)

    # Create a demo template
    template_id = tpl.create_template(
        name="Demo Facility Assessment",
        description="Auto-generated demo template",
        project_id=int(project_id),
        source="demo",
        assignment_mode="OPTIONAL",
        enable_gps=1,
    )

    # Questions
    q1 = tpl.add_template_question(template_id, "Facility type", question_type="DROPDOWN", is_required=1)
    for c in ["Hospital", "Clinic", "PHC", "Other"]:
        tpl.add_choice(q1, c)
    q2 = tpl.add_template_question(template_id, "Is the facility operational today?", question_type="YESNO", is_required=1)
    q3 = tpl.add_template_question(template_id, "Number of staff on duty", question_type="NUMBER")
    q4 = tpl.add_template_question(template_id, "Key issues observed", question_type="LONGTEXT")
    q5 = tpl.add_template_question(template_id, "Date of visit", question_type="DATE")

    # Facilities
    facility_names = [
        "Alimosho PHC",
        "Mushin General Hospital",
        "Ikeja Clinic",
        "Surulere PHC",
        "Yaba Health Centre",
        "Lekki Family Clinic",
    ]
    facility_ids = [get_or_create_facility_by_name(n) for n in facility_names]

    # Coverage nodes (optional)
    coverage_nodes = []
    try:
        scheme_id = cov.create_scheme("Demo Coverage")
        for name in ["Mushin LGA", "Ikeja LGA", "Surulere LGA"]:
            coverage_nodes.append(cov.create_node(scheme_id, name))
    except Exception:
        coverage_nodes = []

    # Enumerators + assignments
    enumerator_names = ["Olamide Sobowale", "Ifeanyi Okoro", "Zainab Yusuf"]
    assignments = []
    for idx, name in enumerate(enumerator_names):
        eid = enum.create_enumerator(project_id, name, code="")
        cov_id = coverage_nodes[idx % len(coverage_nodes)] if coverage_nodes else None
        assign_id = enum.assign_enumerator(
            project_id,
            eid,
            coverage_node_id=cov_id,
            template_id=template_id,
            target_facilities_count=3,
        )
        code_info = prj.ensure_assignment_code(int(project_id), int(eid), int(assign_id))
        # Assign a subset of facilities
        subset = facility_ids[idx: idx + 3] if idx + 3 <= len(facility_ids) else facility_ids[-3:]
        for fid in subset:
            try:
                enum.add_assignment_facility(assign_id, fid)
            except Exception:
                pass
        assignments.append(
            {
                "assignment_id": assign_id,
                "enumerator_id": eid,
                "enumerator_name": name,
                "code_full": code_info.get("code_full"),
                "facilities": subset,
                "coverage_node_id": cov_id,
            }
        )

    # Create demo submissions
    questions = tpl.get_template_questions(template_id)
    for idx, a in enumerate(assignments):
        for j, fid in enumerate(a["facilities"][:2]):
            gps_ok = not (idx == 1 and j == 1)
            gps = None
            if gps_ok:
                gps = {
                    "gps_lat": 6.45 + random.random() * 0.08,
                    "gps_lng": 3.35 + random.random() * 0.08,
                    "gps_accuracy": round(5 + random.random() * 20, 1),
                    "gps_timestamp": now_iso(),
                }
            sync_source = "OFFLINE_SYNC" if (idx == 2 and j == 0) else "LIVE"
            client_uuid = f"demo-{uuid.uuid4()}" if sync_source == "OFFLINE_SYNC" else None
            sid = _insert_survey_dynamic(
                facility_id=int(fid),
                template_id=int(template_id),
                project_id=int(project_id),
                survey_type="Demo Facility Assessment",
                enumerator_name=a["enumerator_name"],
                enumerator_code=a["code_full"],
                enumerator_id=int(a["enumerator_id"]),
                assignment_id=int(a["assignment_id"]),
                gps=gps,
                coverage_node_id=int(a["coverage_node_id"]) if a.get("coverage_node_id") else None,
                qa_flags=None,
                gps_missing_flag=0 if gps_ok else 1,
                created_by="Demo",
                source="demo",
                client_uuid=client_uuid,
                client_created_at=now_iso() if client_uuid else None,
                sync_source=sync_source if sync_source == "OFFLINE_SYNC" else None,
                synced_at=now_iso() if sync_source == "OFFLINE_SYNC" else None,
            )

            for q in questions:
                qid = q[0]
                qtext = q[1]
                qtype = (q[2] or "TEXT").upper()
                answer = ""
                if qtype == "DROPDOWN":
                    answer = random.choice(["Hospital", "Clinic", "PHC"])
                elif qtype == "YESNO":
                    answer = random.choice(["YES", "NO"])
                elif qtype == "NUMBER":
                    answer = str(random.randint(4, 35))
                elif qtype == "DATE":
                    answer = datetime.now().date().isoformat()
                elif qtype == "LONGTEXT":
                    answer = "No major issues observed."
                else:
                    answer = "—"
                if idx == 0 and j == 1 and qtype == "LONGTEXT":
                    answer = ""  # force one empty answer for QA demo
                _insert_answer_dynamic(sid, qid, qtext, answer, answer_source="DEMO")

            _complete_survey(sid)

            # Assign review statuses for variety
            if idx == 0 and j == 0:
                with get_conn() as conn:
                    conn.execute(
                        "UPDATE surveys SET review_status='APPROVED', reviewed_at=?, reviewed_by=? WHERE id=?",
                        (now_iso(), "Demo", int(sid)),
                    )
                    conn.commit()
            elif idx == 1 and j == 0:
                with get_conn() as conn:
                    conn.execute(
                        "UPDATE surveys SET review_status='REVISION', review_reason=?, reviewed_at=?, reviewed_by=? WHERE id=?",
                        ("Please clarify staffing count.", now_iso(), "Demo", int(sid)),
                    )
                    conn.commit()

    return {"ok": True, "message": "Demo data created.", "project_id": project_id}


@app.route("/ui/admin/demo", methods=["POST"])
def ui_admin_demo():
    gate = admin_gate(allow_supervisor=False)
    if gate:
        return gate
    try:
        res = _create_demo_data()
        pid = res.get("project_id")
        if pid:
            return redirect(url_for("ui_project_detail", project_id=int(pid)) + (f"?key={ADMIN_KEY}" if ADMIN_KEY else ""))
        return redirect(url_for("ui_admin") + (f"?key={ADMIN_KEY}" if ADMIN_KEY else ""))
    except Exception as e:
        return ui_shell("Demo Mode", f"<div class='card'><h2>Demo setup failed</h2><div class='muted'>{html.escape(str(e))}</div></div>", show_project_switcher=False), 500


@app.route("/ui/exports/facilities.csv")
def ui_export_facilities_csv():
    gate = admin_gate()
    if gate:
        return gate
    filename = f"facilities_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    path = os.path.join(EXPORT_DIR, filename)
    exp.export_facilities_csv(path)
    return send_file(
        path,
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


@app.route("/ui/exports/surveys.csv")
def ui_export_surveys_csv():
    gate = admin_gate()
    if gate:
        return gate
    project_id = request.args.get("project_id") or None
    allow_restricted = request.args.get("allow_restricted") == "1"
    filename = f"surveys_answers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    path = os.path.join(EXPORT_DIR, filename)
    exp.export_surveys_answers_csv(
        path,
        project_id=int(project_id) if project_id else None,
        allow_restricted=allow_restricted,
    )
    return send_file(
        path,
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


@app.route("/ui/exports/surveys.json")
def ui_export_surveys_json():
    gate = admin_gate()
    if gate:
        return gate
    project_id = request.args.get("project_id") or None
    allow_restricted = request.args.get("allow_restricted") == "1"
    filename = f"surveys_answers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    path = os.path.join(EXPORT_DIR, filename)
    exp.export_surveys_answers_json(
        path,
        project_id=int(project_id) if project_id else None,
        allow_restricted=allow_restricted,
    )
    return send_file(
        path,
        mimetype="application/json",
        as_attachment=True,
        download_name=filename,
    )


@app.route("/ui/exports/research.csv")
def ui_export_surveys_flat_csv():
    gate = admin_gate()
    if gate:
        return gate
    project_id = request.args.get("project_id") or None
    template_id = request.args.get("template_id") or None
    enumerator = request.args.get("enumerator") or None
    start_date = request.args.get("start_date") or None
    end_date = request.args.get("end_date") or None
    allow_restricted = request.args.get("allow_restricted") == "1"
    filename = f"surveys_flat_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    path = os.path.join(EXPORT_DIR, filename)
    exp.export_surveys_flat_csv(
        path,
        project_id=int(project_id) if project_id else None,
        template_id=int(template_id) if template_id else None,
        enumerator_name=enumerator,
        start_date=start_date,
        end_date=end_date,
        allow_restricted=allow_restricted,
    )
    return send_file(
        path,
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


@app.route("/ui/exports/research.json")
def ui_export_surveys_flat_json():
    gate = admin_gate()
    if gate:
        return gate
    project_id = request.args.get("project_id") or None
    template_id = request.args.get("template_id") or None
    enumerator = request.args.get("enumerator") or None
    start_date = request.args.get("start_date") or None
    end_date = request.args.get("end_date") or None
    allow_restricted = request.args.get("allow_restricted") == "1"
    filename = f"surveys_flat_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    path = os.path.join(EXPORT_DIR, filename)
    exp.export_surveys_flat_json(
        path,
        project_id=int(project_id) if project_id else None,
        template_id=int(template_id) if template_id else None,
        enumerator_name=enumerator,
        start_date=start_date,
        end_date=end_date,
        allow_restricted=allow_restricted,
    )
    return send_file(
        path,
        mimetype="application/json",
        as_attachment=True,
        download_name=filename,
    )


@app.route("/ui/exports/metadata.csv")
def ui_export_metadata_csv():
    gate = admin_gate()
    if gate:
        return gate
    project_id = request.args.get("project_id") or None
    filename = f"metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    path = os.path.join(EXPORT_DIR, filename)
    exp.export_metadata_csv(path, project_id=int(project_id) if project_id else None)
    return send_file(
        path,
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


@app.route("/ui/exports/survey.json")
def ui_export_one_survey_json():
    gate = admin_gate()
    if gate:
        return gate
    survey_id = (request.args.get("survey_id") or "").strip()
    allow_restricted = request.args.get("allow_restricted") == "1"
    if not survey_id.isdigit():
        return ui_shell(
            "Export",
            "<div class='card'><h2>Invalid survey ID</h2><div class='muted'>Enter a valid numeric survey ID to export.</div></div>",
            show_project_switcher=False,
        ), 400
    filename = f"survey_{survey_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    path = os.path.join(EXPORT_DIR, filename)
    try:
        exp.export_one_survey_json(path, int(survey_id), allow_restricted=allow_restricted)
    except Exception as e:
        return ui_shell(
            "Export",
            f"<div class='card'><h2>Export failed</h2><div class='muted'>{html.escape(str(e))}</div></div>",
            show_project_switcher=False,
        ), 400
    return send_file(
        path,
        mimetype="application/json",
        as_attachment=True,
        download_name=filename,
    )


# ---------------------------
# Boot
# ---------------------------
if __name__ == "__main__":
    init_db()
    ensure_drafts_table()

    # refresh schema cache
    _SURVEYS_COLS = _table_cols("surveys")
    _ANSWERS_COLS = _table_cols("survey_answers")
    _TEMPLATES_COLS = _table_cols("survey_templates")
    _FACILITIES_COLS = _table_cols("facilities")
    _TQ_COLS = _table_cols("template_questions")
    _TQC_COLS = _table_cols("template_question_choices")

    app.run(host=config.HOST, port=config.PORT, debug=config.DEBUG)
