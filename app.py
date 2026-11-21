"""Nursing-home staffing dashboard (CMS Payroll-Based Journal, Q2 2024).

Reads the CSVs in data/ that metrics/compute_metrics.py produces from the Gold tables.
"""
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

# The dashboard only reads small CSV files shipped in the repo (no AWS access needed),
# so it keeps working after the AWS pipeline is shut down.
DATA = Path(__file__).parent / "data"

st.set_page_config(page_title="Nursing Home Staffing", page_icon="🏥", layout="wide")


# cache_data: each CSV is read once, not on every click or slider move.
# ccn is read as text so leading zeros are kept (e.g. 015009).
@st.cache_data
def load(name: str) -> pd.DataFrame:
    return pd.read_csv(DATA / f"{name}.csv", dtype={"ccn": str})


# Load every table once, up front. Each one is produced by metrics/compute_metrics.py.
nat = load("national_summary").set_index("metric")["value"]
fac = load("facility").copy()
fac[["contract_share", "occupancy"]] *= 100  # fractions -> percent for the explorer table
hprd_state = load("m1_hprd_by_state")
hprd_month = load("m1_hprd_by_month")
contract = load("m2_contract_share_by_state")
occ = load("m3_occupancy_by_state")
quart = load("m4_staffing_vs_rehosp")
by_rating = load("m5_by_staffing_rating")
by_owner = load("m5_by_ownership")

st.title("Nursing home staffing, Q2 2024")
st.caption("Source: CMS Payroll-Based Journal daily nurse staffing joined to CMS Provider Data "
           "Catalog tables. Ratios are total hours / total resident-days, so larger facilities "
           "count in proportion to their size.")

# Headline tiles: national numbers computed as sum/sum over all facilities.
c1, c2, c3, c4 = st.columns(4)
c1.metric("Nursing hours per resident day", f"{nat['national_hprd']:.2f}")
c2.metric("Contract (agency) share of hours", f"{nat['national_contract_share']:.1%}")
c3.metric("Occupancy", f"{nat['national_occupancy']:.1%}")
c4.metric("Facilities", f"{int(nat['facilities']):,}")


def bar(df, x, y, title, fmt, color="#2b6cb0", top=None, horizontal=True):
    # Reusable bar chart. x = category column, y = value column, fmt = number format
    # (".2f" or "%"), top = keep only the N largest rows. Horizontal bars suit long
    # labels such as ownership types; vertical bars suit short ones.
    d = df.sort_values(y, ascending=False)
    if top:
        d = d.head(top)
    enc_y = alt.Y(f"{x}:N", sort="-x", title=None) if horizontal else alt.Y(f"{y}:Q", title=title)
    enc_x = alt.X(f"{y}:Q", title=title, axis=alt.Axis(format=fmt)) if horizontal else alt.X(f"{x}:N", sort="-y", title=None)
    # Tooltip shows the category and the value; "%" is converted to Altair's ".1%" format.
    chart = alt.Chart(d).mark_bar(color=color).encode(
        x=enc_x, y=enc_y, tooltip=[x, alt.Tooltip(y, format=fmt if fmt != "%" else ".1%")])
    # Grow the chart with the number of bars so labels never overlap.
    return chart.properties(height=max(220, 18 * len(d)) if horizontal else 300)


# One tab per metric.
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Staffing hours", "Contract share", "Occupancy", "Staffing vs rehospitalization", "Ratings & ownership"])

# Metric 1: staffing hours per resident day. The user picks how many states and highest/lowest.
with tab1:
    st.subheader("Nursing hours per resident day, by state")
    n = st.slider("States to show", 5, len(hprd_state), 20, key="n1")
    order = st.radio("Show", ["Highest", "Lowest"], horizontal=True)
    d = hprd_state.sort_values("hprd", ascending=(order == "Lowest")).head(n)
    st.altair_chart(bar(d, "state", "hprd", "Hours per resident day", ".2f"), use_container_width=True)
    st.subheader("Monthly trend (national)")
    st.altair_chart(alt.Chart(hprd_month).mark_line(point=True, color="#2b6cb0").encode(
        x=alt.X("month:N", title=None), y=alt.Y("hprd:Q", title="Hours per resident day", scale=alt.Scale(zero=False)),
        tooltip=["month", alt.Tooltip("hprd", format=".3f")]).properties(height=260), use_container_width=True)

# Metric 2: contract (agency) share of hours, highest states first.
with tab2:
    st.subheader("Share of nursing hours worked by contract (agency) staff")
    n = st.slider("States to show", 5, len(contract), 20, key="n2")
    st.altair_chart(bar(contract, "state", "contract_share", "Contract share", "%", "#c05621", top=n),
                    use_container_width=True)
    st.caption("A high share can signal reliance on temporary staff; Vermont is a clear outlier.")

# Metric 3: occupancy by state, with a switch to see the lowest instead of the highest.
with tab3:
    st.subheader("Occupancy: resident-days / (certified beds x days), by state")
    n = st.slider("States to show", 5, len(occ), 20, key="n3")
    low = st.checkbox("Show lowest instead of highest")
    d = occ.sort_values("occupancy", ascending=low).head(n)
    st.altair_chart(bar(d, "state", "occupancy", "Occupancy", "%", "#2f855a"), use_container_width=True)

# Metric 4: staffing quartiles vs average rehospitalization, plus the correlation as a plain sentence.
with tab4:
    st.subheader("Do better-staffed facilities have fewer rehospitalizations?")
    st.caption("Facilities split into four equal groups by staffing hours. Rehospitalization is the CMS "
               "short-stay measure (521) and is only reported for some facilities.")
    st.altair_chart(alt.Chart(quart).mark_bar(color="#6b46c1").encode(
        x=alt.X("hprd_quartile:N", sort=None, title="Staffing group (lowest to highest hours)"),
        y=alt.Y("rehospitalization_pct:Q", title="Average rehospitalization %", scale=alt.Scale(zero=True)),
        tooltip=["hprd_quartile", "facilities", alt.Tooltip("hprd", format=".2f"),
                 alt.Tooltip("rehospitalization_pct", format=".1f")]).properties(height=300),
        use_container_width=True)
    rho = nat["rehosp_vs_hprd_spearman"]
    st.info(f"Rank correlation between staffing hours and rehospitalization: {rho:.2f}. "
            "That is close to zero: staffing hours alone do not explain rehospitalization here.")

# Metric 5: hours by CMS star rating (left) and by ownership type (right).
with tab5:
    a, b = st.columns(2)
    with a:
        st.subheader("By CMS staffing star rating")
        st.altair_chart(alt.Chart(by_rating).mark_bar(color="#2b6cb0").encode(
            x=alt.X("staffing_rating:O", title="Staffing rating (stars)"),
            y=alt.Y("hprd:Q", title="Hours per resident day"),
            tooltip=["staffing_rating", "facilities", alt.Tooltip("hprd", format=".2f")]).properties(height=300),
            use_container_width=True)
    with b:
        st.subheader("By ownership type")
        st.altair_chart(bar(by_owner, "ownership_type", "hprd", "Hours per resident day", ".2f"),
                        use_container_width=True)

# ---- Facility explorer: filter the facility table by state, ownership or name ----
st.divider()
st.subheader("Facility explorer")
f1, f2, f3 = st.columns(3)
states = f1.multiselect("State", sorted(fac.state.dropna().unique()))
owners = f2.multiselect("Ownership", sorted(fac.ownership_type.dropna().unique()))
text = f3.text_input("Name contains")
d = fac
# Each filter only applies when the user has chosen something.
if states:
    d = d[d.state.isin(states)]
if owners:
    d = d[d.ownership_type.isin(owners)]
if text:
    d = d[d.provider_name.str.contains(text, case=False, na=False)]
st.caption(f"{len(d):,} facilities")
# Show the most useful columns, lowest staffing first. column_config only changes how numbers are displayed.
st.dataframe(
    d[["ccn", "provider_name", "city_town", "state", "ownership_type", "number_of_certified_beds",
       "staffing_rating", "hours_per_resident_day", "contract_share", "occupancy", "rehospitalization_pct"]]
    .sort_values("hours_per_resident_day"),
    use_container_width=True, hide_index=True,
    column_config={
        "hours_per_resident_day": st.column_config.NumberColumn("HPRD", format="%.2f"),
        "contract_share": st.column_config.NumberColumn("Contract share", format="%.1f%%"),
        "occupancy": st.column_config.NumberColumn("Occupancy", format="%.1f%%"),
        "rehospitalization_pct": st.column_config.NumberColumn("Rehosp. %", format="%.1f"),
    })
