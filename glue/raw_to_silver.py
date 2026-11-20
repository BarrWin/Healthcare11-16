"""Glue Spark job: S3 Raw (csv) -> S3 Silver (parquet) with data-quality checks.

Staffing: one row per facility-day, partitioned by month, partitions overwritten
so reruns never duplicate. CMS tables: snake_case columns, CCN kept as 6-char text.
Hard failures stop the job; soft issues are flagged and written to _dq/ as JSON.
"""
import json
import re
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["RAW_BUCKET", "SILVER_BUCKET"])
RAW, SILVER = f"s3://{args['RAW_BUCKET']}", f"s3://{args['SILVER_BUCKET']}"
sc = SparkContext()
spark = GlueContext(sc).spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
s3 = boto3.client("s3")
dq = {"run_at": datetime.now(timezone.utc).isoformat(), "failed": [], "warnings": [], "counts": {}}

CATS = ["RNDON", "RNadmin", "RN", "LPNadmin", "LPN", "CNA", "NAtrn", "MedAide"]
EMP = [f"Hrs_{c}_emp" for c in CATS]
CTR = [f"Hrs_{c}_ctr" for c in CATS]
TOT = [f"Hrs_{c}" for c in CATS]


def snake(name):
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    return name


def read_csv(path):
    return (spark.read.option("header", True).option("multiLine", True)
            .option("escape", '"').option("encoding", "ISO-8859-1").csv(path))


def fail(msg):
    dq["failed"].append(msg)
    _write_dq()
    raise RuntimeError(msg)


def _write_dq():
    s3.put_object(Bucket=args["SILVER_BUCKET"],
                  Key=f"_dq/{dq['run_at'][:10]}.json", Body=json.dumps(dq, indent=2).encode())


# ---------------- staffing ----------------
raw = read_csv(f"{RAW}/pbj/")
num_cols = ["MDScensus"] + [c for c in raw.columns if c.startswith("Hrs_")]
df = raw
for c in num_cols:
    df = df.withColumn(c, F.col(c).cast("double"))
df = (df.withColumn("PROVNUM", F.col("PROVNUM").cast("string"))
        .withColumn("work_date", F.to_date(F.col("WorkDate").cast("string"), "yyyyMMdd"))
        .withColumn("month", F.date_format("work_date", "yyyy-MM")))
for c in ["PROVNAME", "CITY", "STATE", "COUNTY_NAME"]:      # trim stray whitespace/tabs
    df = df.withColumn(c, F.regexp_replace(F.trim(F.col(c)), r"\s+", " "))
df.cache()
dq["counts"]["staffing_rows"] = df.count()

# hard checks
if df.filter(F.length("PROVNUM") != 6).count():
    fail("PROVNUM is not 6 characters in some rows (was it read as a number?)")
if df.filter(F.col("work_date").isNull()).count():
    fail("WorkDate could not be parsed in some rows")
if df.groupBy("PROVNUM", "work_date").count().filter("count > 1").count():
    fail("duplicate (PROVNUM, WorkDate) rows")
neg = F.lit(False)
for c in num_cols:
    neg = neg | (F.col(c) < 0)
if df.filter(neg).count():
    fail("negative values found")

# soft checks -> flags
emp_sum = sum(F.col(c) for c in EMP)
ctr_sum = sum(F.col(c) for c in CTR)
tot = sum(F.col(c) for c in TOT)
df = (df.withColumn("total_hours", tot)
        .withColumn("employed_hours", emp_sum)
        .withColumn("contract_hours", ctr_sum))
mismatch = df.filter(F.abs(F.col("employed_hours") + F.col("contract_hours") - F.col("total_hours")) > 0.05).count()
if mismatch:
    dq["warnings"].append(f"{mismatch} rows where employed + contract != total (tolerance 0.05)")
df = (df.withColumn("census_zero", F.col("MDScensus") == 0)
        .withColumn("no_hours_reported", F.col("total_hours") == 0)
        .withColumn("hours_per_resident_day",
                    F.when(F.col("MDScensus") > 0, F.col("total_hours") / F.col("MDScensus"))))
df = df.withColumn("over_24_hprd", F.col("hours_per_resident_day") > 24)
dq["counts"].update(census_zero=df.filter("census_zero").count(),
                    no_hours=df.filter("no_hours_reported").count(),
                    over_24_hprd=df.filter("over_24_hprd").count())

days = df.select("work_date").distinct().count()
short = df.groupBy("PROVNUM").agg(F.countDistinct("work_date").alias("d")).filter(F.col("d") < days).count()
if short:
    dq["warnings"].append(f"{short} facilities are missing days")

# impossible values go to quarantine, not Silver
(df.filter("over_24_hprd")
   .write.mode("overwrite").parquet(f"{SILVER}/_quarantine/staffing_over_24_hprd/"))
clean = df.filter(~F.col("over_24_hprd"))
(clean.drop("over_24_hprd").withColumnRenamed("PROVNUM", "ccn")
      .write.mode("overwrite").partitionBy("month").parquet(f"{SILVER}/staffing_daily/"))

# ---------------- CMS reference tables ----------------
rows = s3.list_objects_v2(Bucket=args["RAW_BUCKET"], Prefix="cms/").get("Contents", [])
provider_ccns = None
for obj in rows:
    name = obj["Key"].split("/")[-1]
    if not name.lower().endswith(".csv"):
        continue
    table = snake(re.sub(r"_(Oct)?\d{4,8}\.csv$|\.csv$", "", name, flags=re.I))
    t = read_csv(f"{RAW}/{obj['Key']}")
    t = t.toDF(*[snake(c) for c in t.columns])
    for c in t.columns:                      # trim all text columns
        t = t.withColumn(c, F.trim(F.col(c)))
    ccn_col = next((c for c in t.columns if c.startswith("cms_certification_number")), None)
    if ccn_col:
        t = t.withColumn("ccn", F.lpad(F.col(ccn_col), 6, "0")).drop(ccn_col)
    t.write.mode("overwrite").parquet(f"{SILVER}/cms/{table}/")
    dq["counts"][f"cms_{table}"] = t.count()
    if table == "nh_providerinfo":
        provider_ccns = t.select("ccn").distinct()

if provider_ccns is not None:
    pbj_ccns = clean.select(F.col("PROVNUM").alias("ccn")).distinct()
    total = pbj_ccns.count()
    matched = pbj_ccns.join(provider_ccns, "ccn").count()
    dq["counts"]["pbj_facilities"], dq["counts"]["pbj_matched_provider_info"] = total, matched
    if total and matched / total < 0.99:
        dq["warnings"].append(f"only {matched}/{total} PBJ facilities match Provider Info (< 99%)")
else:
    dq["warnings"].append("NH_ProviderInfo not found in Raw, so facility match was not checked")

_write_dq()
print(json.dumps(dq, indent=2))
