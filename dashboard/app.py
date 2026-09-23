"""
Retail Analytics Lakehouse - dashboard.

Every number on this page comes from the PySpark pipeline's output,
exported to dashboard/data/ by export_for_dashboard.py.

Run locally:
    pip install -r dashboard/requirements.txt
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

DATA = Path(__file__).resolve().parent / "data"
REPO_URL = "https://github.com/piyush60629/Retail-Analytics-Lakehouse"
LINKEDIN_URL = "https://www.linkedin.com/in/piyush-gupta-24204920b"

INK, MUTED = "#1B2333", "#5B6577"
BRONZE, SILVER, GOLD = "#B0703C", "#8C96A8", "#C99A2E"
GREEN, RED = "#3E7D5A", "#B8483E"
SEQ = [INK, GOLD, BRONZE, SILVER, "#4F7CA8", "#7A5C8F", GREEN, "#A0A7B4"]
SEGMENT_COLORS = {"Regular": "#A0A7B4", "Silver": SILVER, "Gold": GOLD, "Platinum": INK}

st.set_page_config(page_title="Retail Analytics Lakehouse", page_icon="🏪", layout="wide")

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&display=swap');
html, body, [class*="css"], .stMarkdown, button, input {{ font-family: 'IBM Plex Sans', system-ui, sans-serif; }}
.block-container {{ padding-top: 3.6rem; max-width: 1280px; }}
h1, h2, h3 {{ color: {INK}; letter-spacing: -0.01em; }}
div[data-testid="stMetricValue"] > div {{ font-size: 1.4rem; color: {INK}; font-weight: 700; }}
.hero-title {{ font-size: 2.4rem; font-weight: 700; color: {INK}; line-height: 1.1; }}
.hero-sub {{ color: {MUTED}; font-size: 1.05rem; max-width: 780px; margin: .5rem 0 1.4rem; line-height: 1.5; }}
.flow {{ display: flex; gap: 0; margin: .4rem 0 1.6rem; overflow-x: auto; }}
.stage {{ flex: 1 1 0; min-width: 120px; padding: .8rem .9rem; color: #fff; }}
.stage:first-child {{ border-radius: 10px 0 0 10px; }}
.stage:last-child {{ border-radius: 0 10px 10px 0; }}
.stage .t {{ font-size: .9rem; font-weight: 600; margin-bottom: .35rem; }}
.stage .n {{ font-size: 1.35rem; font-weight: 700; line-height: 1.1; }}
.stage .l {{ font-size: .82rem; opacity: .92; margin-top: .2rem; }}
.note {{ background: #EEF1F5; border-left: 3px solid {SILVER}; padding: .7rem 1rem; border-radius: 4px;
         color: {INK}; font-size: .9rem; line-height: 1.5; }}
</style>
""", unsafe_allow_html=True)


@st.cache_data
def load() -> dict[str, pd.DataFrame]:
    tables = {p.stem: pd.read_csv(p) for p in DATA.glob("*.csv")}
    tables["daily_sales_summary"]["order_date"] = pd.to_datetime(tables["daily_sales_summary"]["order_date"])
    tables["payment_summary"]["payment_date"] = pd.to_datetime(tables["payment_summary"]["payment_date"])
    tables["monthly_sales_breakdown"]["month"] = pd.to_datetime(tables["monthly_sales_breakdown"]["month"])
    return tables


def inr(v: float) -> str:
    if abs(v) >= 1e7:
        return f"₹{v / 1e7:,.2f} Cr"
    if abs(v) >= 1e5:
        return f"₹{v / 1e5:,.2f} L"
    return f"₹{v:,.0f}"


def style(fig: go.Figure, height: int = 340) -> go.Figure:
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=40, b=10),
                      font=dict(family="IBM Plex Sans, sans-serif", color=INK),
                      plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                      legend=dict(orientation="h", y=-0.2), title_font_size=15)
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="#E3E6EB")
    return fig


if not (DATA / "executive_kpis.csv").exists():
    st.error("No pipeline output found in dashboard/data/. Run the pipeline, then "
             "`python -m dashboard.export_for_dashboard --batch-date <date>`.")
    st.stop()

t = load()
batch_date = json.loads((DATA / "run_info.json").read_text())["batch_date"]
kpi = t["executive_kpis"].iloc[0]
val = t["validation_summary"]
cdc = t["cdc_summary"]
dq = t["gold_quality_results"]
scd = t["scd2_history"]
layers = t["layer_counts"]

# Product and customer KPIs are at SCD2-version grain; roll them up per business key.
products = (t["product_performance"].groupby(["product_id", "category"], as_index=False)
            .agg(product_name=("product_name", "last"), units=("total_quantity_sold", "sum"),
                 orders=("total_orders", "sum"), net_revenue=("net_revenue", "sum")))
customers = (t["customer_performance"].groupby("customer_id", as_index=False)
             .agg(customer_name=("customer_name", "last"), orders=("total_orders", "sum"),
                  total_spent=("total_spent", "sum"), latest_order=("latest_order_date", "max")))

with st.sidebar:
    st.markdown("### Retail Analytics Lakehouse")
    st.markdown("An incremental **PySpark** lakehouse on the medallion architecture, with validation, "
                "CDC, SCD Type 2, a star-schema Gold layer, data quality checks and KPIs.")
    st.link_button("View source on GitHub", REPO_URL, width="stretch")
    st.link_button("Connect on LinkedIn", LINKEDIN_URL, width="stretch")
    st.divider()
    st.caption(f"Pipeline output for batch **{batch_date}**  \n"
               "Stack: PySpark · Parquet · Python · Streamlit")
    st.caption("Built by Piyush Gupta")

# ---------------------------------------------------------------- hero
changes = int(cdc.loc[cdc["change_type"] != "UNCHANGED", "records"].sum())
new_versions = int((scd["is_current"].astype(str) == "True").sum())
dq_pass = int((dq["status"] == "PASS").sum())
fact_rows = int(layers.loc[(layers["layer"] == "Gold") & (layers["dataset"] == "fact_sales"), "rows"].iloc[0])

st.markdown('<div class="hero-title">Retail Analytics Lakehouse</div>', unsafe_allow_html=True)
st.markdown(f'<p class="hero-sub">Output of an incremental PySpark pipeline for batch {batch_date}. '
            "Raw retail files enter on the left; analytics-ready tables and KPIs come out on the right.</p>",
            unsafe_allow_html=True)
stages = [
    ("Bronze", f"{int(val['total_records'].sum()):,}", "raw rows ingested", BRONZE),
    ("Validation", f"{int(val['invalid_records'].sum()):,}", "bad rows quarantined", "#56607A"),
    ("CDC", f"{changes:,}", "inserts and updates detected", "#6F7A8F"),
    ("Silver", f"{int(val['valid_records'].sum()):,}", "clean, typed rows", SILVER),
    ("SCD Type 2", f"{new_versions:,}", "dimension rows versioned", "#8A7A4A"),
    ("Gold", f"{fact_rows:,}", "fact_sales rows", GOLD),
    ("Data quality", f"{dq_pass}/{len(dq)}", "Gold checks passed", "#9C7B22"),
]
st.markdown('<div class="flow">' + "".join(
    f'<div class="stage" style="background:{c}"><div class="t">{a}</div><div class="n">{n}</div>'
    f'<div class="l">{l}</div></div>' for a, n, l, c in stages) + "</div>", unsafe_allow_html=True)

tabs = st.tabs(["Business KPIs", "Pipeline", "Validation", "CDC & SCD Type 2", "Data quality"])

# ---------------------------------------------------------------- KPIs
with tabs[0]:
    k = st.columns(5)
    k[0].metric("Net revenue", inr(kpi["total_revenue"]))
    k[1].metric("Orders", f"{int(kpi['total_orders']):,}")
    k[2].metric("Average order value", inr(kpi["average_order_value"]))
    k[3].metric("Customers", f"{int(kpi['total_customers']):,}")
    k[4].metric("Refunds", inr(kpi["total_refunds"]))

    ms = t["monthly_sales_breakdown"]
    f1, f2 = st.columns([2, 1])
    months = sorted(ms["month"].dt.date.unique())
    with f1:
        rng = st.select_slider("Months", options=months, value=(months[0], months[-1]),
                               format_func=lambda d: d.strftime("%b %Y"))
    with f2:
        statuses = sorted(ms["order_status"].unique())
        pick = st.multiselect("Order status", statuses, default=[s for s in statuses if s.upper() != "CANCELLED"])
    sel = ms[ms["month"].dt.date.between(*rng) & ms["order_status"].isin(pick)]

    if sel.empty:
        st.info("No sales match these filters. Widen the month range or add an order status.")
    else:
        monthly = sel.groupby("month", as_index=False).agg(net_revenue=("net_revenue", "sum"),
                                                           orders=("orders", "sum"))
        fig = go.Figure()
        fig.add_bar(x=monthly["month"], y=monthly["net_revenue"], name="Net revenue", marker_color=GOLD)
        fig.add_scatter(x=monthly["month"], y=monthly["orders"], name="Orders", yaxis="y2",
                        mode="lines+markers", line=dict(color=INK, width=2.5))
        fig.update_layout(title="Monthly net revenue and orders", yaxis=dict(title="Net revenue (₹)"),
                          yaxis2=dict(title="Orders", overlaying="y", side="right", showgrid=False,
                                      rangemode="tozero"))
        st.plotly_chart(style(fig, 380), width="stretch")
        st.caption("July 2026 is a partial month (data up to the batch date).")

        c1, c2 = st.columns(2)
        with c1:
            cat = sel.groupby("category", as_index=False)["net_revenue"].sum().sort_values("net_revenue")
            fig = px.bar(cat, x="net_revenue", y="category", orientation="h", title="Net revenue by category",
                         color_discrete_sequence=[INK], labels={"net_revenue": "Net revenue (₹)", "category": ""})
            st.plotly_chart(style(fig), width="stretch")
        with c2:
            seg = sel.groupby("customer_segment", as_index=False)["net_revenue"].sum()
            seg["customer_segment"] = pd.Categorical(seg["customer_segment"], list(SEGMENT_COLORS), ordered=True)
            fig = px.bar(seg.sort_values("customer_segment"), x="customer_segment", y="net_revenue",
                         color="customer_segment", color_discrete_map=SEGMENT_COLORS,
                         title="Net revenue by customer segment (segment at time of sale)",
                         labels={"net_revenue": "Net revenue (₹)", "customer_segment": ""})
            fig.update_layout(showlegend=False)
            st.plotly_chart(style(fig), width="stretch")

    ps = t["payment_summary"]
    c3, c4 = st.columns(2)
    with c3:
        ok = ps[ps["payment_status"] == "SUCCESS"].groupby("payment_method", as_index=False)["payment_amount"].sum()
        fig = px.pie(ok, names="payment_method", values="payment_amount", hole=0.55,
                     title="Successful payments by method", color_discrete_sequence=SEQ)
        fig.update_traces(textinfo="percent+label", showlegend=False)
        st.plotly_chart(style(fig), width="stretch")
    with c4:
        stt = ps.groupby("payment_status", as_index=False).agg(amount=("payment_amount", "sum"),
                                                              payments=("payment_count", "sum"))
        fig = px.bar(stt.sort_values("amount"), x="amount", y="payment_status", orientation="h",
                     title="Payment amount by status", text=stt.sort_values("amount")["payments"].map("{:,} payments".format),
                     color="payment_status",
                     color_discrete_map={"SUCCESS": GREEN, "REFUNDED": BRONZE, "FAILED": RED, "PENDING": SILVER},
                     labels={"amount": "Amount (₹)", "payment_status": ""})
        fig.update_layout(showlegend=False)
        st.plotly_chart(style(fig), width="stretch")

    t1, t2 = st.columns(2)
    with t1:
        st.markdown("**Top 10 products by net revenue**")
        top = products.nlargest(10, "net_revenue")[["product_name", "category", "units", "net_revenue"]]
        top["net_revenue"] = top["net_revenue"].map(inr)
        st.dataframe(top, hide_index=True, width="stretch")
    with t2:
        st.markdown("**Top 10 customers by spend**")
        topc = customers.nlargest(10, "total_spent")[["customer_id", "customer_name", "orders", "total_spent"]]
        topc["total_spent"] = topc["total_spent"].map(inr)
        st.dataframe(topc, hide_index=True, width="stretch")

# ---------------------------------------------------------------- Pipeline
with tabs[1]:
    left, right = st.columns([1.1, 1])
    with left:
        st.subheader("Stages in run_pipeline.py")
        st.table(pd.DataFrame([
            ("Incremental ingestion + validation", "Detects changed source files, lands Bronze, quarantines bad rows"),
            ("Record-level CDC", "Compares with the previous batch: inserted, updated, unchanged"),
            ("Incremental Silver", "Merges the changes into clean Silver tables"),
            ("SCD Type 2", "Closes old dimension rows and opens new versions"),
            ("Fact enrichment", "Looks up the surrogate key valid on each event date"),
            ("Gold layer", "fact_sales, fact_payment, dim_customer, dim_product, dim_date"),
            ("Gold data quality", "Not-null, unique, foreign-key, reconciliation and amount checks"),
            ("Analytics & KPIs", "Daily sales, product, customer, payment and executive KPIs"),
        ], columns=["Stage", "What it does"], index=range(1, 9)))
        st.code(f"python run_pipeline.py --batch-date {batch_date}", language="bash")
    with right:
        gold = layers[layers["layer"] == "Gold"].sort_values("rows")
        fig = px.bar(gold, x="rows", y="dataset", orientation="h", title="Gold star schema: rows per table",
                     color="dataset", color_discrete_map={"fact_sales": GOLD, "fact_payment": GOLD},
                     labels={"dataset": "", "rows": "rows"})
        fig.update_traces(marker_color=[GOLD if d.startswith("fact") else SILVER for d in gold["dataset"]])
        fig.update_layout(showlegend=False)
        st.plotly_chart(style(fig, 300), width="stretch")
        st.markdown(
            '<div class="note">These numbers are produced by the PySpark pipeline and exported with '
            "<code>dashboard/export_for_dashboard.py</code>. The website only reads the exported files, "
            "so it runs without Spark.</div>", unsafe_allow_html=True)

# ---------------------------------------------------------------- Validation
with tabs[2]:
    st.subheader("Bronze validation")
    cols = st.columns(len(val))
    for col, (_, r) in zip(cols, val.iterrows()):
        col.metric(r["dataset_name"], f"{int(r['valid_records']):,} valid",
                   f"-{int(r['invalid_records'])} quarantined" if r["invalid_records"] else "0 quarantined",
                   delta_color="inverse" if r["invalid_records"] else "off")
    rf = t["validation_rule_failures"]
    if not rf.empty:
        fig = px.bar(rf.sort_values("failed_records"), x="failed_records", y="validation_rule",
                     color="dataset_name", orientation="h", title="Why rows were quarantined",
                     color_discrete_sequence=SEQ, labels={"validation_rule": "", "failed_records": "rows"})
        st.plotly_chart(style(fig, 330), width="stretch")
    st.markdown(
        '<div class="note"><code>PARENT_ORDER_QUARANTINED</code> cascades rejections: if an order fails, '
        "its items and payments are held back too, so no orphan rows reach the Gold facts.</div>",
        unsafe_allow_html=True)

# ---------------------------------------------------------------- CDC & SCD2
with tabs[3]:
    st.subheader("Change data capture")
    pivot = cdc.pivot_table(index="dataset", columns="change_type", values="records", fill_value=0)
    pivot = pivot.reindex(columns=[c for c in ["INSERTED", "UPDATED", "DELETED", "UNCHANGED"] if c in pivot.columns])
    fig = px.bar(pivot.drop(columns="UNCHANGED", errors="ignore").reset_index().melt(id_vars="dataset"),
                 x="dataset", y="value", color="change_type", barmode="group",
                 title="Rows changed in this batch", color_discrete_map={"INSERTED": GREEN, "UPDATED": BRONZE,
                                                                        "DELETED": RED},
                 labels={"value": "rows", "dataset": "", "change_type": ""})
    c1, c2 = st.columns([1.3, 1])
    c1.plotly_chart(style(fig, 320), width="stretch")
    c2.dataframe(pivot.astype(int), width="stretch")

    st.divider()
    st.subheader("SCD Type 2 history")
    dim = st.radio("Dimension", ["dim_customer", "dim_product"], horizontal=True)
    h = scd[scd["dimension"] == dim].dropna(axis=1, how="all").drop(columns="dimension")
    key = st.selectbox("Pick a record that changed", sorted(h["business_key"].unique()))
    rows = h[h["business_key"] == key]
    st.dataframe(rows.style.apply(
        lambda r: ["background-color: #F4ECD8" if str(r["is_current"]) == "True" else "color: #8C96A8"] * len(r),
        axis=1), hide_index=True, width="stretch")
    st.markdown(
        '<div class="note">When a tracked attribute changes, the current row is closed '
        "(<code>effective_to</code> set, <code>is_current = false</code>) and a new version is opened. "
        "Facts keep the surrogate key that was valid on the order date, so history is preserved.</div>",
        unsafe_allow_html=True)

# ---------------------------------------------------------------- DQ
with tabs[4]:
    a, b, c = st.columns(3)
    a.metric("Checks run", len(dq))
    b.metric("Passed", int((dq["status"] == "PASS").sum()))
    c.metric("Failed", int((dq["status"] != "PASS").sum()))
    colors = {"PASS": f"color: {GREEN}; font-weight: 600", "FAIL": f"color: {RED}; font-weight: 600"}
    show = dq[["table_name", "check_name", "status", "failed_count", "description"]]
    st.dataframe(show.style.map(lambda v: colors.get(v, ""), subset=["status"]),
                 hide_index=True, width="stretch", height=560)
