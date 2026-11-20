# Data dictionary

Source: CMS Payroll-Based Journal (PBJ) daily nurse staffing for nursing homes, Q2 2024,
joined to CMS Provider Data Catalog tables. Join key: `ccn` (CMS Certification Number), a
6-character text field with leading zeros (PBJ calls it `PROVNUM`). Never store it as a number.

## Metric definitions (used everywhere)

| Term | Definition |
|---|---|
| Resident-days | Sum of daily `MDScensus` (residents on the MDS census) |
| Hours per resident day (HPRD) | sum(total nursing hours) / sum(resident-days). Sum over sum, never an average of daily ratios, so large facilities weigh in proportion to their size |
| Contract share | sum(contract hours) / sum(total hours) |
| Valid row | census > 0 and total hours > 0. Only valid rows enter any metric |
| Occupancy | average daily census / certified beds. State level: resident-days / (certified beds x valid days) |
| Rehospitalization % | CMS measure 521 (short-stay residents rehospitalized after admission), adjusted score. Reported for only some facilities |

## S3 layers

| Layer | Format | Contents |
|---|---|---|
| Raw | csv, as downloaded | `pbj/` PBJ daily file, `cms/` the 19 Provider Data Catalog tables, `_manifest/` ingest record |
| Silver | parquet | Cleaned and typed: `staffing_daily/` (partitioned by `month`), `cms/<table>/`, `_dq/` quality reports, `_quarantine/` rejected rows |
| Gold | parquet | Star schema and metric marts below |

## Silver: `staffing_daily`

| Column | Type | Meaning |
|---|---|---|
| ccn | string | 6-character facility id (from PROVNUM) |
| work_date | date | Day worked (parsed from `WorkDate`, yyyyMMdd) |
| month | string | `yyyy-MM`, partition key |
| MDScensus | double | Residents on the census that day |
| total_hours | double | All nursing hours (RN director of nursing, RN, RN admin, LPN, LPN admin, CNA, NA in training, medication aide), employed plus contract |
| employed_hours | double | Hours worked by facility employees |
| contract_hours | double | Hours worked by contract (agency) staff |
| hours_per_resident_day | double | total_hours / MDScensus; null when census is 0 |
| census_zero | boolean | MDScensus = 0 |
| no_hours_reported | boolean | total_hours = 0 |

Rows with hours per resident day above 24 are impossible and are moved to `_quarantine/`.

Hard quality checks that fail the job: ccn not 6 characters, unparseable date, duplicate
(ccn, work_date), negative values. Warnings: employed + contract differs from total, facilities
missing days, fewer than 99% of PBJ facilities matched to Provider Info.

## Gold tables

### dim_facility (one row per CMS-certified nursing home, 14,814 rows)
ccn, provider_name, city_town, state, zip_code, county_parish, ownership_type, provider_type,
number_of_certified_beds, overall_rating (1-5 stars), staffing_rating (1-5 stars),
reported_total_nurse_staffing_hours_per_resident_per_day (CMS-published, for cross-checking),
total_nursing_staff_turnover, registered_nurse_turnover, number_of_substantiated_complaints,
latitude, longitude. Source: CMS `NH_ProviderInfo`.

### dim_date
work_date, year, month (number), quarter, day_of_week, is_weekend.

### fact_staffing_daily (one row per facility per day, partitioned by month)
ccn, work_date, month, state, census, total_hours, employed_hours, contract_hours,
hours_per_resident_day, census_zero, no_hours_reported.

### mart_state_month (state x month)
state, month, total_hours, resident_days, contract_hours, hours_per_resident_day, contract_share.

### mart_facility_quarter (one row per facility with valid staffing days, 14,564 rows)
| Column | Meaning |
|---|---|
| ccn, total_hours, resident_days, contract_hours | Quarter totals over valid days |
| avg_census | Average daily census over valid days |
| days_valid | Distinct valid days |
| hours_per_resident_day, contract_share | As defined above |
| provider_name ... longitude | Copied from `dim_facility` (null for 17 facilities not in Provider Info) |
| occupancy | avg_census / number_of_certified_beds |
| rehospitalization_pct | CMS measure 521 adjusted score; null when not reported |

## Dashboard files (`data/`)

Produced by `metrics/compute_metrics.py` from the two marts above.

| File | Grain | Columns |
|---|---|---|
| national_summary.csv | 5 rows | metric, value (national HPRD, contract share, occupancy, facility count, Spearman correlation of HPRD vs rehospitalization) |
| m1_hprd_by_state.csv | state | state, total_hours, resident_days, hprd |
| m1_hprd_by_month.csv | month | month, total_hours, resident_days, hprd |
| m2_contract_share_by_state.csv | state | state, contract_hours, total_hours, contract_share |
| m3_occupancy_by_state.csv | state | state, resident_days, bed_days, occupancy |
| m4_staffing_vs_rehosp.csv | HPRD quartile | hprd_quartile, facilities, hprd (median), rehospitalization_pct (mean) |
| m5_by_staffing_rating.csv | CMS staffing stars | staffing_rating, facilities, hprd, contract_share |
| m5_by_ownership.csv | ownership type | ownership_type, facilities, hprd, contract_share |
| facility.csv | facility | ccn, provider_name, city_town, state, ownership_type, number_of_certified_beds, overall_rating, staffing_rating, total_hours, resident_days, contract_hours, hours_per_resident_day, contract_share, occupancy, rehospitalization_pct, latitude, longitude |

## Known limits

- The PBJ file covers one quarter, so the monthly trend has three points.
- The file has staffing hours and census only: there are no per-resident or per-shift records,
  so a nurse-to-patient ratio per shift cannot be computed. Hours per resident day is the
  closest measure the data supports.
- Rehospitalization is missing for about 19% of facilities (2,747 of 14,564).
- 17 PBJ facilities have no Provider Info match and carry blank names and states.
