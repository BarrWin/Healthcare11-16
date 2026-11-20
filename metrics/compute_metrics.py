"""Step 5: compute the dashboard metrics from the Gold marts.

Usage:  python metrics/compute_metrics.py [GOLD_BUCKET] [OUT_DIR]
Reads mart_state_month and mart_facility_quarter (parquet) and writes small CSVs
that the Streamlit dashboard reads. Ratios are always sum/sum (never an average
of ratios), so large facilities weigh in proportion to their resident-days.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

bucket = sys.argv[1] if len(sys.argv) > 1 else "healthcare-metrics-gold-926642724963"
out = Path(sys.argv[2] if len(sys.argv) > 2 else "metrics_out")
out.mkdir(parents=True, exist_ok=True)

sm = pd.read_parquet(f"s3://{bucket}/mart_state_month/")
fq = pd.read_parquet(f"s3://{bucket}/mart_facility_quarter/")


def ratio(df, num, den):
    return df[num].sum() / df[den].sum() if df[den].sum() else np.nan


# 1. Nurse staffing hours per resident day (HPRD): by state, and monthly trend
m1_state = (sm.groupby("state")[["total_hours", "resident_days"]].sum()
            .assign(hprd=lambda d: d.total_hours / d.resident_days)
            .reset_index().sort_values("hprd", ascending=False))
m1_month = (sm.groupby("month")[["total_hours", "resident_days"]].sum()
            .assign(hprd=lambda d: d.total_hours / d.resident_days).reset_index())

# 2. Contract (agency) share of nursing hours, by state
m2_state = (sm.groupby("state")[["contract_hours", "total_hours"]].sum()
            .assign(contract_share=lambda d: d.contract_hours / d.total_hours)
            .reset_index().sort_values("contract_share", ascending=False))

# 3. Occupancy: resident-days / (certified beds x days), by state
fq = fq.assign(bed_days=lambda d: d.number_of_certified_beds * d.days_valid)
m3_state = (fq.groupby("state")[["resident_days", "bed_days"]].sum()
            .assign(occupancy=lambda d: d.resident_days / d.bed_days)
            .reset_index().sort_values("occupancy", ascending=False))

# 4. Staffing level vs rehospitalization: facilities in HPRD quartiles
ok = fq[(fq.hours_per_resident_day.notna()) & (fq.rehospitalization_pct.notna())].copy()
ok["hprd_quartile"] = pd.qcut(ok.hours_per_resident_day, 4,
                              labels=["Q1 lowest", "Q2", "Q3", "Q4 highest"])
m4 = (ok.groupby("hprd_quartile", observed=True)
      .agg(facilities=("ccn", "count"),
           hprd=("hours_per_resident_day", "median"),
           rehospitalization_pct=("rehospitalization_pct", "mean"))
      .reset_index())
corr4 = ok.hours_per_resident_day.corr(ok.rehospitalization_pct, method="spearman")

# 5. HPRD and contract share by CMS star staffing rating and by ownership type
m5_rating = (fq.dropna(subset=["staffing_rating"]).groupby("staffing_rating")
             .apply(lambda d: pd.Series({"facilities": len(d),
                                         "hprd": ratio(d, "total_hours", "resident_days"),
                                         "contract_share": ratio(d, "contract_hours", "total_hours")}),
                    include_groups=False).reset_index())
m5_owner = (fq.dropna(subset=["ownership_type"]).groupby("ownership_type")
            .apply(lambda d: pd.Series({"facilities": len(d),
                                        "hprd": ratio(d, "total_hours", "resident_days"),
                                        "contract_share": ratio(d, "contract_hours", "total_hours")}),
                   include_groups=False).reset_index())

# Facility table the dashboard can filter / rank
fac_cols = ["ccn", "provider_name", "city_town", "state", "ownership_type", "number_of_certified_beds",
            "overall_rating", "staffing_rating", "total_hours", "resident_days", "contract_hours",
            "hours_per_resident_day", "contract_share", "occupancy", "rehospitalization_pct",
            "latitude", "longitude"]
facility = fq[fac_cols]

for name, df in {"m1_hprd_by_state": m1_state, "m1_hprd_by_month": m1_month,
                 "m2_contract_share_by_state": m2_state, "m3_occupancy_by_state": m3_state,
                 "m4_staffing_vs_rehosp": m4, "m5_by_staffing_rating": m5_rating,
                 "m5_by_ownership": m5_owner, "facility": facility}.items():
    df.to_csv(out / f"{name}.csv", index=False)

nat = pd.DataFrame({
    "metric": ["national_hprd", "national_contract_share", "national_occupancy",
               "facilities", "rehosp_vs_hprd_spearman"],
    "value": [ratio(sm, "total_hours", "resident_days"), ratio(sm, "contract_hours", "total_hours"),
              ratio(fq, "resident_days", "bed_days"), fq.ccn.nunique(), corr4]})
nat.to_csv(out / "national_summary.csv", index=False)

pd.set_option("display.width", 120)
print(nat.to_string(index=False))
print("\nHPRD top/bottom 3 states\n", pd.concat([m1_state.head(3), m1_state.tail(3)]).round(3).to_string(index=False))
print("\nContract share top 3\n", m2_state.head(3).round(3).to_string(index=False))
print("\nOccupancy low 3\n", m3_state.tail(3).round(3).to_string(index=False))
print("\nStaffing vs rehosp\n", m4.round(2).to_string(index=False))
print("\nBy staffing rating\n", m5_rating.round(3).to_string(index=False))
print("\nBy ownership\n", m5_owner.round(3).to_string(index=False))
print("\nwrote", sorted(p.name for p in out.glob("*.csv")))
