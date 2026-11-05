# Super fund allocation assistant

Chat app where Claude answers questions about the strategic asset allocation (SAA) of the compared super funds.
Claude writes read-only SQL on the gold layer (the metric view first), the app runs it in Databricks, and Claude answers only from the returned rows.
Every answer shows a chart (Claude picks the type and columns) and the SQL and rows behind it.

## Run on the laptop
### First-time setup (once)
From the repo root, in PowerShell:
1. `python -m venv .venv`
2. `.venv\Scripts\Activate.ps1`
3. `pip install -r app/requirements.txt`
4. Copy `app/.streamlit/secrets.toml.example` to `app/.streamlit/secrets.toml` and fill it in (no token: you sign in through the browser).

### Start the app
Option 1, no typing: double-click `run_app.bat` in the repo root (Windows only; it expects the `.venv` above).

Option 2, in a PowerShell terminal, from the repo root:
1. `cd app`
2. `..\.venv\Scripts\Activate.ps1`
3. `streamlit run app.py`

The app opens at http://localhost:8501. That address only works while the app is running.
The first question opens a Databricks sign-in page in the browser. If the SQL warehouse is stopped, the first answer can take a few minutes.

### Stop the app
Close the black window, or press `Ctrl+C` in the terminal running the app. Restart after changing `app.py` settings or imports.

### If something goes wrong
- **Running scripts is disabled:** run `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`, then activate again.
- **Stuck on "running connect()":** usually a certificate problem or a pending sign-in. The app trusts the Windows certificates through `truststore`, so antivirus that inspects HTTPS (e.g. Norton) should not block it. Check that `truststore` is installed, finish any open sign-in tab, and restart the app.
- **The app opens on port 8502:** another copy is already running. Stop it first.

## Run on Streamlit Community Cloud
- Main file path: `app/app.py`. Paste the secrets in the app's Secrets box, including `DATABRICKS_TOKEN` and `APP_PASSWORD`.
- Limits: 20 questions per browser session, 500 rows per query.

## Safety
- The app only sends single SELECT/WITH statements. This is a basic guard; the real control is the Databricks identity's permissions.
- Better for production: a service principal with SELECT on gold only (not done: needs an account admin).

## Test questions
The 6 Genie benchmarks in the project analysis folder (`genie_benchmarks/v2`).
