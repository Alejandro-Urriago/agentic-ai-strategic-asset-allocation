# Super fund allocation assistant

Chat app where Claude answers questions about the strategic asset allocation (SAA) of the compared super funds.
Claude writes read-only SQL on the gold layer (the metric view first), the app runs it in Databricks, and Claude answers only from the returned rows.
Every answer shows the SQL and rows behind it, and says the figures are targets, not actual holdings.

## Run on the laptop
1. `pip install -r app/requirements.txt`
2. Copy `app/.streamlit/secrets.toml.example` to `app/.streamlit/secrets.toml` and fill it in (no token: you sign in through the browser).
3. From the `app` folder: `streamlit run app.py`

## Run on Streamlit Community Cloud
- Main file path: `app/app.py`. Paste the secrets in the app's Secrets box, including `DATABRICKS_TOKEN` and `APP_PASSWORD`.
- Limits: 20 questions per browser session, 500 rows per query.

## Safety
- The app only sends single SELECT/WITH statements. This is a basic guard; the real control is the Databricks identity's permissions.
- Better for production: a service principal with SELECT on gold only (not done: needs an account admin).

## Test questions
The 6 Genie benchmarks in the project analysis folder (`genie_benchmarks/v2`).
