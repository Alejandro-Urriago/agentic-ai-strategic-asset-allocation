"""
Chat app: Claude answers questions about Australian super funds' strategic asset
allocation (SAA) by running read-only SQL on the gold layer in Databricks.
Claude never invents numbers: it only sees the rows its SQL returns.

Settings come from Streamlit secrets (.streamlit/secrets.toml on the laptop,
the app's Secrets box on Streamlit Community Cloud). See README.md.
Run: streamlit run app/app.py
"""
import json
import re

import anthropic
import streamlit as st
from anthropic import beta_tool
from databricks import sql

MAX_ROWS = 500          # rows returned to Claude per query
MAX_QUESTIONS = 20      # questions per browser session, to protect API credit

CATALOG = st.secrets["DATABRICKS_CATALOG"]
SCHEMA = st.secrets.get("GOLD_SCHEMA", "gold")
GOLD = f"{CATALOG}.{SCHEMA}"
MODEL = st.secrets.get("CLAUDE_MODEL", "claude-sonnet-5-5")

SYSTEM = f"""You answer questions about the strategic asset allocation (SAA) of Australian superannuation funds.

Rules:
- Only use numbers returned by the run_sql tool. If it returns no rows or an error, say so; never guess.
- Figures are board-approved TARGET allocations reported to APRA, not actual holdings. Say this in every answer.
- A risk profile (Conservative, Balanced, Growth, High growth) is based on the share of growth assets.
  It is NOT the option's name: an option named "Balanced" can be in the Growth risk profile.
  If the user's wording could mean either, ask which one they mean.
- Compare funds within one risk profile. State the funds, risk profile, option names and dates you used.
- Derive dates from the data (e.g. latest date = MAX(date)); never assume a date.
- "Last N financial years" = the N latest financial_year values, using is_year_end_point = true.
- Be short: a few sentences or a small markdown table.

Data (Databricks SQL, catalog.schema = {GOLD}):
1. {GOLD}.metric_strategic_allocation: metric view (semantic layer). Use it first.
   Dimensions: fund, fund_type, risk_profile, investment_option_name, date, financial_year,
     is_year_end_point, sector, sub_sector, listing_type, domicile_type, economy_type.
   Measures (0-100, average target per option and date), always wrap in MEASURE():
     allocation_pct, equity_pct, listed_equity_pct, australian_listed_equity_pct,
     international_listed_equity_pct, unspecified_domicile_listed_equity_pct,
     international_share_of_listed_equity_pct, developed_listed_equity_pct,
     emerging_listed_equity_pct, unspecified_economy_listed_equity_pct, cash_pct,
     fixed_income_pct, listed_equity_change_1y_points, listed_equity_change_5y_points.
   Example: SELECT fund, financial_year, MEASURE(listed_equity_pct) AS listed_equity_pct
            FROM {GOLD}.metric_strategic_allocation
            WHERE risk_profile = 'Growth' AND is_year_end_point
            GROUP BY ALL ORDER BY fund, financial_year
   Metric views need GROUP BY on the dimensions you select; SELECT * is not allowed.
2. Star schema, for anything the metric view can't answer:
   {GOLD}.dim_fund(fund_key, fund_name, short_name, fund_type)
   {GOLD}.dim_investment_option(investment_option_key, fund_key, investment_option_name, risk_profile,
     growth_assets_pct, defensive_assets_pct, total_member_assets)  -- fractions, 0.7 = 70%
   {GOLD}.dim_asset_class(asset_class_key, sector_l1, listing_type, domicile_type, economy_type, is_overlay)
   {GOLD}.dim_date(date_key, financial_year, financial_quarter, is_quarter_end)
   {GOLD}.fact_investment_strategic_allocation(date_key, fund_key, investment_option_key, asset_class_key,
     benchmark_allocation_pct, currency_hedging_ratio_pct)  -- fractions; holds ALL options:
     INNER JOIN dim_investment_option to keep the compared ones; exclude is_overlay = true from totals.
   Unspecified domicile/economy is its own group: never merge it into australian, international or developed.
"""

READ_ONLY = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)


def connect():
    """Open a Databricks SQL connection: token if one is set (hosted app), else browser login (laptop)."""
    args = dict(server_hostname=st.secrets["DATABRICKS_HOST"], http_path=st.secrets["DATABRICKS_HTTP_PATH"])
    if st.secrets.get("DATABRICKS_TOKEN"):
        return sql.connect(**args, access_token=st.secrets["DATABRICKS_TOKEN"])
    return sql.connect(**args, auth_type="databricks-oauth")


@beta_tool
def run_sql(query: str) -> str:
    """Run one read-only SQL query (SELECT or WITH) on the gold layer and return the rows as JSON.

    Args:
        query: A single Databricks SQL SELECT statement using fully qualified table names.
    """
    statement = query.strip().rstrip(";")
    if not READ_ONLY.match(statement) or ";" in statement:
        return "Error: only a single SELECT or WITH statement is allowed."
    log = {"sql": statement}
    st.session_state.queries.append(log)
    try:
        with connect() as conn, conn.cursor() as cur:
            cur.execute(statement)
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchmany(MAX_ROWS)]
    except Exception as e:  # return the error so Claude can fix its SQL
        log["error"] = str(e)
        return f"Error: {e}"
    log["rows"] = rows
    return json.dumps(rows, default=str)


st.set_page_config(page_title="Super fund allocation assistant")
st.title("Super fund allocation assistant")
st.caption("Answers use board-approved target allocations (SAA) reported to APRA, not actual holdings.")

# Simple password, so only people with the link and password spend API credit
if st.secrets.get("APP_PASSWORD") and not st.session_state.get("signed_in"):
    if st.text_input("Password", type="password") == st.secrets["APP_PASSWORD"]:
        st.session_state.signed_in = True
        st.rerun()
    st.stop()

st.session_state.setdefault("messages", [])
for m in st.session_state.messages:
    st.chat_message(m["role"]).markdown(m["content"])

asked = sum(m["role"] == "user" for m in st.session_state.messages)
if asked >= MAX_QUESTIONS:
    st.info("Question limit reached for this session.")
    st.stop()

if prompt := st.chat_input("e.g. How has the Growth risk profile's listed equity target changed over 5 years?"):
    st.chat_message("user").markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})
    st.session_state.queries = []

    with st.chat_message("assistant"), st.spinner("Thinking and querying Databricks..."):
        # The tool runner loops: Claude writes SQL, run_sql runs it, the rows go
        # back to Claude, and it stops when Claude has its final answer.
        runner = anthropic.Anthropic(api_key=st.secrets["ANTHROPIC_API_KEY"]).beta.messages.tool_runner(
            model=MODEL,
            max_tokens=8000,
            system=SYSTEM,
            tools=[run_sql],
            messages=st.session_state.messages,
        )
        final = None
        for message in runner:
            final = message
        answer = "".join(b.text for b in final.content if b.type == "text")
        st.markdown(answer)
        # Show the SQL behind the answer, so every number can be checked
        for i, q in enumerate(st.session_state.queries, 1):
            with st.expander(f"SQL {i}" + (" (error)" if "error" in q else "")):
                st.code(q["sql"], language="sql")
                if "error" in q:
                    st.error(q["error"])
                else:
                    st.dataframe(q["rows"])
    st.session_state.messages.append({"role": "assistant", "content": answer})
