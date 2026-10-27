"""
Chat app: Claude answers questions about Australian super funds' strategic asset
allocation (SAA) by running read-only SQL on the gold layer in Databricks, and
draws a chart for each answer. Claude never invents numbers: it only sees the
rows its SQL returns.

Settings come from Streamlit secrets (.streamlit/secrets.toml on the laptop,
the app's Secrets box on Streamlit Community Cloud). See README.md.
Run from the app folder: streamlit run app.py
"""
import json
import re

import altair as alt
import anthropic
import pandas as pd
import streamlit as st
from anthropic import beta_tool
from databricks import sql

MAX_ROWS = 500          # rows returned to Claude per query
MAX_QUESTIONS = 20      # questions per browser session, to protect API credit

CATALOG = st.secrets["DATABRICKS_CATALOG"]
SCHEMA = st.secrets.get("GOLD_SCHEMA", "gold")
GOLD = f"{CATALOG}.{SCHEMA}"
MODEL = st.secrets.get("CLAUDE_MODEL", "claude-sonnet-5-5")

CHART_TYPES = ["line", "bar", "grouped_bar", "stacked_bar"]
PALETTE = ["#ff5a3c", "#2dd4bf", "#f5b942", "#60a5fa", "#a78bfa",
           "#f472b6", "#34d399", "#fb923c", "#94a3b8", "#e2e8f0"]

EXAMPLES = [  # (button label, question): the Genie benchmark questions
    ('Listed equity today · Growth',
     "What is each fund's listed equity target in the Growth risk profile at the latest date?"),
    ('Home bias · Australian vs international',
     "For the Growth risk profile, how has each fund's Australian vs international listed equity target changed over the last 5 financial years, using the last quarter-end of each year? Show every year."),
    ('Developed vs emerging markets',
     "For the Growth risk profile, how has each fund's split between developed and emerging markets in international listed equity changed over the last 5 financial years, using the last quarter-end of each year?"),
    ('Equity by risk profile',
     'How has the average listed equity target of each risk profile changed over the last 5 financial years, using the last quarter-end of each year and a simple average across funds?'),
    ('Biggest movers',
     'Which options had the biggest change in their international listed equity target over the last 1 year and 5 years, across all risk profiles?'),
    ('Ranking shift · 5 years',
     'How has the ranking of funds by international share of listed equity changed between now and 5 years ago in the Growth risk profile?'),
    ('Trend strength · international',
     'Which funds show the strongest upward or downward trend in international listed equity over the last 5 years for the Growth risk profile, using every quarter-end, and how steady is each trend?'),
]

SYSTEM = f"""You answer questions about the strategic asset allocation (SAA) of Australian superannuation funds:
each investment option's board-approved target mix, reported to APRA. Call the figures "targets".

Rules:
- Only use numbers returned by the run_sql tool. If it returns no rows or an error, fix the SQL or say so; never guess.
- A risk profile (Conservative, Balanced, Growth, High growth) is based on the share of growth assets.
  It is NOT the option's name: an option named "Balanced" can be in the Growth risk profile.
  If the user's wording could mean either, ask which one they mean.
- Compare funds within one risk profile. State the risk profile and dates you used.
- Derive dates from the data (e.g. latest date = MAX(date)); never assume a date.
- "Last N financial years" = the N latest financial_year values, using is_year_end_point = true.
- Don't speculate about causes that aren't in the data.
- Be short: 2-4 sentences of insight plus, if useful, a small markdown table.

Charts: whenever the answer has more than a few numbers, call show_chart once after your final query.
Write that query so its rows are exactly what to plot, in long format (one row per x value and series).
- line: trends over time (x = date or financial_year, color = fund or series)
- bar: ranking one number per item (x = item, y = value)
- grouped_bar: compare a few series side by side (x = item, color = series)
- stacked_bar: composition that adds up (x = item, color = part)

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

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;700&family=Inter:wght@400;500&family=JetBrains+Mono:wght@400;600&family=Instrument+Serif:ital@1&display=swap');
:root { --bg:#070b16; --panel:#0e1526; --line:#1d2740; --text:#e6e9ef; --muted:#8b93a7;
        --coral:#ff5a3c; --teal:#2dd4bf; --amber:#f5b942; }
.stApp { background: radial-gradient(1200px 600px at 70% -10%, #13203d 0%, var(--bg) 55%) fixed; color: var(--text); }
html, body, .stApp, .stMarkdown, p, li, td, th { font-family: 'Inter', sans-serif; }
#MainMenu, footer, .stDeployButton, [data-testid="stAppDeployButton"], [data-testid="stMainMenu"] { display: none; }
[data-testid="stExpandSidebarButton"], [data-testid="stSidebarCollapsedControl"] { color: var(--coral); }
header[data-testid="stHeader"] { background: transparent; }
[data-testid="stSidebar"] { background: #0a1020; border-right: 1px solid var(--line); }
.kicker { font-family: 'JetBrains Mono', monospace; font-size: .78rem; letter-spacing: .22em; color: var(--muted); margin-top: .5rem; }
.kicker .num { color: var(--amber); margin-right: .9rem; }
.title { font-family: 'Space Grotesk', sans-serif; font-weight: 700; font-size: 3rem; line-height: 1.05;
         color: #f4f6fb; margin: .6rem 0 .2rem; letter-spacing: -.01em; }
.title .accent { color: var(--coral); }
.subtitle { font-family: 'Instrument Serif', serif; font-style: italic; font-size: 1.35rem; color: var(--muted); }
.rule { height: 3px; width: 140px; background: linear-gradient(90deg, var(--coral), var(--amber)); border-radius: 2px; margin: 1rem 0 1.1rem; }
.pipeline { display: flex; flex-wrap: wrap; gap: .5rem; align-items: center; margin-bottom: 1.6rem; }
.chip { font-family: 'JetBrains Mono', monospace; font-size: .72rem; letter-spacing: .12em; color: var(--teal);
        border: 1px solid var(--line); border-radius: 999px; padding: .28rem .8rem; background: rgba(45,212,191,.06); }
.chip.hot { color: var(--coral); border-color: var(--coral); background: rgba(255,90,60,.08); }
.arrow { color: var(--muted); font-family: 'JetBrains Mono', monospace; }
.side-label { font-family: 'JetBrains Mono', monospace; font-size: .72rem; letter-spacing: .2em; color: var(--muted); margin: .4rem 0 .6rem; }
[data-testid="stChatMessage"] { background: rgba(255,255,255,.025); border: 1px solid var(--line); border-radius: 16px; padding: 1rem 1.2rem; }
[data-testid="stChatInput"] textarea { font-family: 'Inter', sans-serif; }
[data-testid="stExpander"] { border: 1px solid var(--line); border-radius: 12px; background: rgba(255,255,255,.015); }
[data-testid="stExpander"] summary p { font-family: 'JetBrains Mono', monospace; font-size: .75rem; letter-spacing: .12em; color: var(--muted); }
.stButton button { width: 100%; text-align: left; background: transparent; border: 1px solid var(--line); color: var(--text);
                   border-radius: 12px; font-size: .85rem; padding: .55rem .8rem; }
.stButton button:hover { border-color: var(--coral); color: #fff; }
.stButton button[kind="primary"] { background: var(--coral); border-color: var(--coral); color: #fff; font-weight: 600; margin-bottom: 1rem; }
</style>
"""

HEADER = """
<div class="kicker"><span class="num">01</span>AUSTRALIAN SUPER FUNDS · APRA DATA</div>
<div class="title">Strategic Asset Allocation, <span class="accent">Agentic AI</span></div>
<div class="subtitle">ask in plain English, answered from live data</div>
<div class="rule"></div>
<div class="pipeline">
  <span class="chip">APRA FILES</span><span class="arrow">›</span>
  <span class="chip">BRONZE</span><span class="arrow">›</span>
  <span class="chip">SILVER</span><span class="arrow">›</span>
  <span class="chip">GOLD + METRIC VIEW</span><span class="arrow">›</span>
  <span class="chip hot">CLAUDE AGENT</span>
</div>
"""


@st.cache_resource  # one connection for the whole app, so the browser login happens once
def connect():
    """Open a Databricks SQL connection: token if one is set (hosted app), else browser login (laptop)."""
    args = dict(server_hostname=st.secrets["DATABRICKS_HOST"], http_path=st.secrets["DATABRICKS_HTTP_PATH"])
    if st.secrets.get("DATABRICKS_TOKEN"):
        return sql.connect(**args, access_token=st.secrets["DATABRICKS_TOKEN"])
    return sql.connect(**args, auth_type="databricks-oauth")


def query_rows(statement):
    """Run a query on the shared connection; if the connection has gone stale, reconnect once."""
    for attempt in range(2):
        conn = connect()
        try:
            with conn.cursor() as cur:
                cur.execute(statement)
                cols = [c[0] for c in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchmany(MAX_ROWS)]
        except Exception as e:
            if attempt == 0 and not conn.open:
                connect.clear()  # drop the dead connection and log in again
                continue
            raise e


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
    st.session_state.turn["queries"].append(log)
    n = len(st.session_state.turn["queries"])
    try:
        rows = query_rows(statement)
    except Exception as e:  # return the error so Claude can fix its SQL
        log["error"] = str(e)
        return f"Query {n} error: {e}"
    log["rows"] = rows
    return f"Query {n} rows: " + json.dumps(rows, default=str)


@beta_tool
def show_chart(query_number: int, chart_type: str, x: str, y: str, color: str = "", title: str = "") -> str:
    """Draw a chart under the answer from the rows of one of your queries in this answer.

    Args:
        query_number: Number of the query whose rows to plot (as returned by run_sql, e.g. 2 for "Query 2").
        chart_type: One of "line", "bar", "grouped_bar", "stacked_bar".
        x: Column for the x axis (e.g. financial_year, date or fund).
        y: Numeric column for the y axis.
        color: Optional column that splits the data into series (e.g. fund or domicile). Empty for none.
        title: Short chart title.
    """
    queries = st.session_state.turn["queries"]
    if not 1 <= query_number <= len(queries) or "rows" not in queries[query_number - 1]:
        return "Error: no successful query with that number."
    rows = queries[query_number - 1]["rows"]
    missing = [c for c in (x, y, color) if c and rows and c not in rows[0]]
    if chart_type not in CHART_TYPES or missing:
        return f"Error: chart_type must be one of {CHART_TYPES}; unknown columns: {missing}"
    st.session_state.turn["charts"].append(
        {"rows": rows, "type": chart_type, "x": x, "y": y, "color": color, "title": title})
    return "Chart added."


def draw_chart(spec, key):
    """Render a chart spec with the dark theme; a dropdown lets the viewer switch chart type."""
    chart_type = st.selectbox("Chart type", CHART_TYPES, index=CHART_TYPES.index(spec["type"]),
                              key=key, label_visibility="collapsed")
    df = pd.DataFrame(spec["rows"])
    x, y, color = spec["x"], spec["y"], spec["color"]
    df[x] = df[x].astype(str)
    df[y] = pd.to_numeric(df[y], errors="coerce")
    enc = dict(x=alt.X(f"{x}:O", title=None, axis=alt.Axis(labelAngle=0 if chart_type == "line" else -30)),
               y=alt.Y(f"{y}:Q", title=y.replace("_", " ")),
               tooltip=list(df.columns))
    if color:
        enc["color"] = alt.Color(f"{color}:N", scale=alt.Scale(range=PALETTE), title=None)
    if chart_type == "line":
        chart = alt.Chart(df).mark_line(point=alt.OverlayMarkDef(size=55, filled=True), strokeWidth=2.5)
    else:
        chart = alt.Chart(df).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, opacity=.92)
        if chart_type == "grouped_bar" and color:
            enc["xOffset"] = alt.XOffset(f"{color}:N")
        elif chart_type == "bar":
            enc["x"] = alt.X(f"{x}:N", title=None, sort="-y", axis=alt.Axis(labelAngle=-30))
    chart = (chart.encode(**enc)
             .properties(height=380, background="transparent",
                         title=alt.Title(spec["title"].upper(), anchor="start", color="#8b93a7",
                                         font="JetBrains Mono", fontSize=11, fontWeight=600))
             .configure_view(stroke=None)
             .configure_axis(labelColor="#8b93a7", titleColor="#8b93a7", gridColor="#1d2740",
                             domainColor="#1d2740", tickColor="#1d2740", labelFont="JetBrains Mono",
                             titleFont="JetBrains Mono", labelFontSize=11)
             .configure_legend(labelColor="#c9cfdb", labelFont="Inter", orient="bottom", columns=5,
                               symbolType="circle"))
    st.altair_chart(chart, width="stretch")


def show_turn(turn, i):
    """Show the charts and the SQL behind one answer."""
    for j, spec in enumerate(turn.get("charts", [])):
        draw_chart(spec, key=f"chart_{i}_{j}")
    for n, q in enumerate(turn.get("queries", []), 1):
        with st.expander(f"SQL {n}" + (" · error" if "error" in q else "")):
            st.code(q["sql"], language="sql")
            if "error" in q:
                st.error(q["error"])
            else:
                st.dataframe(q["rows"], width="stretch")


st.set_page_config(page_title="Strategic Asset Allocation, Agentic AI", layout="wide")
st.markdown(CSS, unsafe_allow_html=True)
st.markdown(HEADER, unsafe_allow_html=True)

# Simple password, so only people with the link and password spend API credit
if st.secrets.get("APP_PASSWORD") and not st.session_state.get("signed_in"):
    if st.text_input("Password", type="password") == st.secrets["APP_PASSWORD"]:
        st.session_state.signed_in = True
        st.rerun()
    st.stop()

with st.sidebar:
    if st.button("＋ New chat", key="new_chat", type="primary"):
        st.session_state.history = []
        st.rerun()
    st.markdown('<div class="side-label">TRY A QUESTION</div>', unsafe_allow_html=True)
    for k, (label, q) in enumerate(EXAMPLES):
        if st.button(label, key=f"ex_{k}", help=q):
            st.session_state.pending = q

st.session_state.setdefault("history", [])
for i, h in enumerate(st.session_state.history):
    with st.chat_message(h["role"]):
        st.markdown(h["content"])
        if h["role"] == "assistant":
            show_turn(h, i)

asked = sum(h["role"] == "user" for h in st.session_state.history)
if asked >= MAX_QUESTIONS:
    st.info("Question limit reached for this session.")
    st.stop()

prompt = st.chat_input("Ask about any fund, risk profile or asset class...") or st.session_state.pop("pending", None)
if prompt:
    st.chat_message("user").markdown(prompt)
    st.session_state.history.append({"role": "user", "content": prompt})
    st.session_state.turn = {"queries": [], "charts": []}

    with st.chat_message("assistant"), st.spinner("Querying Databricks..."):
        # The tool runner loops: Claude writes SQL, run_sql runs it, the rows go back
        # to Claude, show_chart stores a chart, and it stops at Claude's final answer.
        runner = anthropic.Anthropic(api_key=st.secrets["ANTHROPIC_API_KEY"]).beta.messages.tool_runner(
            model=MODEL,
            max_tokens=8000,
            system=SYSTEM,
            tools=[run_sql, show_chart],
            messages=[{"role": h["role"], "content": h["content"]} for h in st.session_state.history],
        )
        final = None
        for message in runner:
            final = message
        answer = "".join(b.text for b in final.content if b.type == "text")
    st.session_state.history.append({"role": "assistant", "content": answer, **st.session_state.turn})
    st.rerun()  # redraw from history, so charts and dropdowns live in one place
