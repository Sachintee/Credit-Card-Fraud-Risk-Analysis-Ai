import hashlib
from datetime import datetime

import pandas as pd
import plotly.express as px
import requests
import streamlit as st


st.set_page_config(page_title="Fraud Risk Analysis", page_icon="💳", layout="wide")


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Barlow:wght@400;500;600;700;800&display=swap');

        :root {
            --bg: #05070c;
            --panel: #0b0f17;
            --panel-2: #101927;
            --ink: #eef3ff;
            --muted: #98a6c0;
            --accent: #2ab3ff;
            --accent-2: #ff8a3d;
            --line: #26374c;
            --good: #2dd79f;
            --bad: #ff5f7a;
        }

        html, body, [class*="css"] {
            font-family: 'Barlow', sans-serif;
        }

        .stApp {
            background:
                radial-gradient(circle at 15% 8%, rgba(42,179,255,0.16), transparent 32%),
                radial-gradient(circle at 85% 15%, rgba(255,138,61,0.11), transparent 35%),
                linear-gradient(145deg, #05070c 0%, #03050a 50%, #070b13 100%);
            color: var(--ink);
        }

        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0b1220 0%, #0b0f17 100%);
            border-right: 1px solid var(--line);
        }

        .title-wrap {
            margin-bottom: 0.4rem;
            padding: 0.3rem 0 0.8rem 0;
        }

        .title-main {
            font-size: 2.65rem;
            line-height: 1.05;
            font-weight: 800;
            letter-spacing: 0.2px;
            color: #f4f7ff;
            text-shadow: 0 2px 22px rgba(42,179,255,0.12);
        }

        .subtitle {
            color: var(--muted);
            font-size: 1rem;
            margin-top: 0.35rem;
        }

        .kpi {
            background: linear-gradient(140deg, #101927 0%, #0a111d 100%);
            border: 1px solid var(--line);
            border-radius: 18px;
            padding: 1rem 1.2rem;
            min-height: 126px;
            box-shadow: inset 0 0 0 1px rgba(255,255,255,0.02), 0 12px 26px rgba(0,0,0,0.28);
        }

        .kpi-label {
            color: var(--muted);
            font-weight: 600;
            font-size: 1rem;
            letter-spacing: 0.2px;
        }

        .kpi-value {
            font-size: 2.75rem;
            font-weight: 800;
            margin-top: 0.12rem;
            color: #f5f9ff;
        }

        .panel {
            background: linear-gradient(165deg, #0d1522 0%, #0a101a 100%);
            border: 1px solid var(--line);
            border-radius: 18px;
            padding: 0.25rem 0.45rem;
        }

        .section-h {
            color: #f0f4ff;
            font-size: 1.08rem;
            font-weight: 700;
            margin-bottom: 0.4rem;
            letter-spacing: 0.2px;
        }

        .small-muted {
            color: var(--muted);
            font-size: 0.9rem;
        }

        div[data-testid="stMetricValue"] {
            color: #f4f8ff;
        }

        .stDataFrame {
            border: 1px solid var(--line);
            border-radius: 14px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def generate_demo_data() -> pd.DataFrame:
    """Generate sample transaction data for demo mode."""
    import random
    locations = ["Delhi", "Mumbai", "Jaipur", "Bangalore", "Pune"]
    records = []
    for _ in range(150):
        records.append({
            "amount": random.randint(100, 50000),
            "location": random.choice(locations),
            "time": random.randint(1, 100000),
            "status": "FRAUD" if random.random() < 0.26 else "NORMAL",
            "processed_at": int(pd.Timestamp.now().timestamp()) - random.randint(0, 86400),
        })
    return pd.DataFrame(records)


@st.cache_data(ttl=5)
def load_data() -> pd.DataFrame:
    try:
        response = requests.get("http://127.0.0.1:8000/transactions", timeout=4)
        response.raise_for_status()
        records = response.json()
        return pd.DataFrame(records)
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, Exception):
        st.info("📊 API unreachable. Showing demo data instead. Deploy with your backend running for live data.")
        return generate_demo_data()


def map_deterministic(value: str, choices: list[str]) -> str:
    idx = int(hashlib.md5(value.encode("utf-8")).hexdigest(), 16) % len(choices)
    return choices[idx]


def enrich_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    fraud_types = ["Card Not Present", "Account Takeover", "Identity Theft", "Phishing", "Card Skimming"]
    categories = ["Apparel", "E-commerce", "Electronics", "Food Delivery", "Groceries", "Transportation"]
    states = ["Rajasthan", "Tamil Nadu", "Gujarat", "Karnataka", "Kerala", "Maharashtra", "Uttar Pradesh", "West Bengal", "Delhi"]

    if "processed_at" in df.columns:
        dt = pd.to_datetime(df["processed_at"], unit="s", errors="coerce")
    else:
        dt = pd.to_datetime(datetime.now())

    df["month"] = dt.dt.month_name().fillna("Unknown")

    base_keys = (
        df.get("location", "Unknown").astype(str)
        + "|"
        + df.get("time", 0).astype(str)
        + "|"
        + df.get("amount", 0).astype(str)
    )

    df["fraud_type"] = base_keys.apply(lambda x: map_deterministic(x + "_ft", fraud_types))
    df["category"] = base_keys.apply(lambda x: map_deterministic(x + "_cat", categories))
    df["state"] = base_keys.apply(lambda x: map_deterministic(x + "_state", states))

    amount = pd.to_numeric(df.get("amount", 0), errors="coerce").fillna(0)
    df["risk"] = pd.cut(
        amount,
        bins=[-1, 12000, 28000, 42000, float("inf")],
        labels=["Low", "Medium", "High", "Critical"],
    ).astype(str)

    return df


def render() -> None:
    inject_styles()

    st.markdown(
        """
        <div class="title-wrap">
            <div class="title-main">Credit Card Fraud Risk Analysis</div>
            <div class="subtitle">Live transaction intelligence with high-risk spotting and category trend breakdown.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    try:
        df = enrich_data(load_data())
    except Exception as exc:
        st.error(f"Unable to load API data: {exc}")
        return

    if df.empty:
        st.warning("No transactions available yet. Keep producer/consumer running and refresh in a few seconds.")
        return

    with st.sidebar:
        st.markdown("## Filter Panel")
        fraud_type_filter = st.multiselect(
            "Fraud Type", sorted(df["fraud_type"].dropna().unique().tolist()), default=sorted(df["fraud_type"].dropna().unique().tolist())
        )
        state_filter = st.multiselect("State", sorted(df["state"].dropna().unique().tolist()), default=sorted(df["state"].dropna().unique().tolist()))
        category_filter = st.multiselect(
            "Transaction Category", sorted(df["category"].dropna().unique().tolist()), default=sorted(df["category"].dropna().unique().tolist())
        )

    fdf = df[
        df["fraud_type"].isin(fraud_type_filter)
        & df["state"].isin(state_filter)
        & df["category"].isin(category_filter)
    ].copy()

    total_count = len(fdf)
    fraud_count = int((fdf["status"] == "FRAUD").sum())
    critical_count = int((fdf["risk"] == "Critical").sum())
    fraud_rate = (fraud_count / total_count * 100) if total_count else 0
    critical_rate = (critical_count / total_count * 100) if total_count else 0

    k1, k2, k3 = st.columns(3)
    with k1:
        st.markdown(f"""<div class=\"kpi\"><div class=\"kpi-label\">Fraud Rate</div><div class=\"kpi-value\">{fraud_rate:.2f}%</div></div>""", unsafe_allow_html=True)
    with k2:
        st.markdown(f"""<div class=\"kpi\"><div class=\"kpi-label\">Fraudulent Transactions</div><div class=\"kpi-value\">{fraud_count}</div></div>""", unsafe_allow_html=True)
    with k3:
        st.markdown(f"""<div class=\"kpi\"><div class=\"kpi-label\">Critical Risk Transactions</div><div class=\"kpi-value\">{critical_rate:.2f}%</div></div>""", unsafe_allow_html=True)

    row1_left, row1_right = st.columns([1.9, 1])
    with row1_left:
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        st.markdown("<div class='section-h'>Total Transaction Amount by Fraud Type and Transaction Category</div>", unsafe_allow_html=True)

        agg_stack = (
            fdf.groupby(["fraud_type", "category"], as_index=False)["amount"]
            .sum()
            .sort_values("amount", ascending=False)
        )
        fig_stack = px.bar(
            agg_stack,
            x="amount",
            y="fraud_type",
            color="category",
            orientation="h",
            barmode="stack",
            color_discrete_sequence=["#2ab3ff", "#2f4cff", "#ff8a3d", "#8e17cc", "#f15bb5", "#7d5fff"],
        )
        fig_stack.update_layout(
            height=360,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e7edf9"),
            margin=dict(l=10, r=10, t=10, b=10),
            legend_title_text="",
        )
        st.plotly_chart(fig_stack, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with row1_right:
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        st.markdown("<div class='section-h'>Total Transaction Amount by Fraud Risk</div>", unsafe_allow_html=True)

        agg_risk = fdf.groupby("risk", as_index=False)["amount"].sum()
        risk_order = ["Low", "Medium", "High", "Critical"]
        agg_risk["risk"] = pd.Categorical(agg_risk["risk"], categories=risk_order, ordered=True)
        agg_risk = agg_risk.sort_values("risk")

        fig_donut = px.pie(
            agg_risk,
            names="risk",
            values="amount",
            hole=0.58,
            color="risk",
            color_discrete_map={
                "Low": "#ff8a3d",
                "Medium": "#8e17cc",
                "High": "#2454ff",
                "Critical": "#2ab3ff",
            },
        )
        fig_donut.update_traces(textposition="outside", textinfo="value+percent")
        fig_donut.update_layout(
            height=360,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e7edf9"),
            margin=dict(l=20, r=20, t=5, b=5),
            legend_title_text="Fraud Risk",
        )
        st.plotly_chart(fig_donut, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    row2_left, row2_right = st.columns([1.9, 1])
    with row2_left:
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        st.markdown("<div class='section-h'>Fraudulent Transactions by State</div>", unsafe_allow_html=True)

        state_fraud = (
            fdf[fdf["status"] == "FRAUD"]
            .groupby("state", as_index=False)
            .size()
            .rename(columns={"size": "count"})
            .sort_values("count", ascending=False)
        )
        fig_state = px.bar(
            state_fraud,
            x="state",
            y="count",
            color_discrete_sequence=["#bf5a2a"],
            text="count",
        )
        fig_state.update_layout(
            height=330,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e7edf9"),
            margin=dict(l=10, r=10, t=10, b=10),
            xaxis_title="State",
            yaxis_title="Fraudulent Transactions",
        )
        st.plotly_chart(fig_state, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with row2_right:
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        st.markdown("<div class='section-h'>Fraudulent Transactions by Month</div>", unsafe_allow_html=True)

        month_order = [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ]
        fraud_month = (
            fdf[fdf["status"] == "FRAUD"]
            .groupby("month", as_index=False)
            .size()
            .rename(columns={"size": "count"})
        )
        fraud_month["month"] = pd.Categorical(fraud_month["month"], categories=month_order, ordered=True)
        fraud_month = fraud_month.sort_values("month")

        fig_month = px.area(
            fraud_month,
            x="month",
            y="count",
            markers=True,
            color_discrete_sequence=["#1e9cff"],
        )
        fig_month.update_layout(
            height=330,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e7edf9"),
            margin=dict(l=10, r=10, t=10, b=10),
            xaxis_title="Month",
            yaxis_title="Fraudulent Transactions",
        )
        st.plotly_chart(fig_month, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("### Live Alerts")
    alerts = fdf[fdf["status"] == "FRAUD"][
        ["amount", "location", "state", "fraud_type", "category", "risk", "month"]
    ].sort_values("amount", ascending=False)
    st.dataframe(alerts.head(25), use_container_width=True, height=300)


render()