# HurkField (OpenField Collect)

Project-aware field data collection with assignments, QA, interviews, and exports.

## Quick start (dev)

```bash
./run_dev.sh
```

Open:
- Admin UI: `http://127.0.0.1:5000/ui`
- Public form links: from Templates → Share

## GitHub compatibility + hosting

This project is now GitHub-ready:
- `.gitignore` excludes local DB, `.env`, uploads/exports, and virtualenv files.
- `Procfile` + `wsgi.py` support production app start.
- `render.yaml` supports one-click deployment from a GitHub repo.
- `.github/workflows/ci.yml` runs syntax checks on push/PR.

Important:
- **GitHub Pages cannot host Flask backends** (it only serves static files).
- Use a Python host (Render/Railway/Fly/VM) connected to GitHub.

### Recommended (Render + GitHub)

1. Push this repo to GitHub.
2. In Render, create **New > Blueprint** and select this repo.
3. Render will use `render.yaml` automatically.
4. Set required env vars in Render:
   - `OPENFIELD_ADMIN_KEY`
   - `OPENFIELD_CODE_SECRET`
   - OAuth/SMTP/transcription keys (if needed)
5. Deploy and open your Render URL.

`render.yaml` stores data on a persistent disk:
- DB: `/var/data/hurkfield.db`
- Uploads: `/var/data/uploads`
- Exports: `/var/data/exports`

## Install manually

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

## Production run (generic)

```bash
source .venv/bin/activate
gunicorn -c gunicorn.conf.py wsgi:app
```

## Configuration

Copy `.env.example` → `.env`, then edit values.

Common settings:
- `OPENFIELD_ADMIN_KEY` — require `?key=` for `/ui` pages
- `OPENFIELD_DB_PATH` — recommended `instance/openfield.db`
- `OPENFIELD_ENV` — `development`, `demo`, `production`
- `OPENFIELD_HOST`, `OPENFIELD_PORT` — server bind
- `OPENFIELD_PLATFORM_MODE` — enable multi‑supervisor mode
- `OPENFIELD_REQUIRE_SUPERVISOR_KEY` — require supervisor access key for `/ui`
- `OPENFIELD_PROJECT_REQUIRED` — enforce project‑centric workflow

## Platform mode (orgs + supervisors)

1) Enable in `.env`:
   - `OPENFIELD_PLATFORM_MODE=1`
   - `OPENFIELD_REQUIRE_SUPERVISOR_KEY=1`
   - `OPENFIELD_PROJECT_REQUIRED=1`
2) Go to **Admin → Supervisors** and create supervisor access keys.
3) Supervisors access the UI at `/ui/access`, enter their key, and stay in their organization scope.

Notes:
- Admin key still grants platform‑wide access.
- Supervisors are scoped to their organization’s projects.

## Auth (email signup)

- `/signup` creates an organization + owner user.
- `/login` signs in with email/password.
- `/logout` clears session.

Roles stored: OWNER, SUPERVISOR, ANALYST (analyst is read‑only in `/ui`).
Password flows:
- `/resend-verification`
- `/forgot-password`
- `/reset-password?token=...`

Invites:
- Owners can invite teammates in `/ui/org/users`.
- Invite link: `/invite/accept?token=...`

Audit log:
- Actions like approvals, role updates, and invites are logged in `audit_logs`.
- View in UI: `/ui/audit`

## OAuth (all four providers)

Routes:
- `/auth/google`
- `/auth/microsoft`
- `/auth/linkedin`
- `/auth/facebook`

Set credentials in `.env`:
- `OPENFIELD_PUBLIC_BASE_URL` (e.g. `https://your-domain.com`, recommended for stable OAuth callback URLs)
- `OPENFIELD_GOOGLE_OAUTH_CLIENT_ID`
- `OPENFIELD_GOOGLE_OAUTH_CLIENT_SECRET`
- `OPENFIELD_MICROSOFT_OAUTH_CLIENT_ID`
- `OPENFIELD_MICROSOFT_OAUTH_CLIENT_SECRET`
- `OPENFIELD_LINKEDIN_OAUTH_CLIENT_ID`
- `OPENFIELD_LINKEDIN_OAUTH_CLIENT_SECRET`
- `OPENFIELD_FACEBOOK_OAUTH_CLIENT_ID`
- `OPENFIELD_FACEBOOK_OAUTH_CLIENT_SECRET`

Redirect URI for each provider:
- `https://your-domain.com/auth/<provider>/callback`

LinkedIn note:
- Redirect URI must match exactly (scheme + host + path, no trailing slash).
- For Render default domain, use `https://<service>.onrender.com/auth/linkedin/callback`.

The app reads all settings from `config.py` (env‑backed). If a `.env` file exists, it is loaded automatically.

## Run modes

- **development**: `OPENFIELD_ENV=development`, `OPENFIELD_DEBUG=1`
- **demo**: `OPENFIELD_ENV=demo`, `OPENFIELD_DEBUG=0`
- **production**: `OPENFIELD_ENV=production`, `OPENFIELD_DEBUG=0`

## Exports

Supervisor → Exports:
- Facilities CSV
- Surveys + Answers CSV
- Surveys + Answers JSON
- Research export (flat) CSV/JSON
- **Single survey JSON** (by ID)

## Admin panel

Supervisor → Admin:
- System status (env, version, DB path/size)
- Schema overview
- One‑click DB backup
- Demo data generator (projects + enumerators + submissions)

Supervisor → Review Console:
- `/ui/review` for approvals, rejections, and revision requests

Backup endpoint:
- `/ui/admin/backup`

Demo endpoint:
- `/ui/admin/demo` (POST)

## Dependencies

- Flask
- qrcode (with Pillow)
- python‑docx
- PyMuPDF (for PDF text import)
- gunicorn (production)

## Dependency locking

- `requirements.txt` lists direct dependencies (including optional import helpers).
- `requirements.lock` captures a pinned environment for repeatable installs.
  - If you need `.docx`/PDF import features, install deps from `requirements.txt` and re‑generate:
    `pip install -r requirements.txt && pip freeze > requirements.lock`

## Notes

- If `openfield.db` exists in the project root, it will be used by default.
- For portability, set `OPENFIELD_DB_PATH=instance/openfield.db` in `.env`.
- Never commit `.env` or local DB files to GitHub.

## Production deployment (Gunicorn + Nginx)

Run:

```bash
source .venv/bin/activate
gunicorn -c gunicorn.conf.py wsgi:app
```

Example configs:
- `deploy/nginx.conf.example`
- `deploy/openfield.service.example`
