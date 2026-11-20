"""Glue Spark job: S3 Silver -> S3 Gold star schema and marts (parquet).

Ratios are computed as sum(hours) / sum(resident-days) over valid rows only
(census > 0 and hours reported), never as an average of daily ratios.
"""
import sys

import boto3

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["SILVER_BUCKET", "GOLD_BUCKET"] + (["FORCE"] if "--FORCE" in sys.argv else []))
FORCE = args.get("FORCE", "false").lower() == "true"
SILVER, GOLD = f"s3://{args['SILVER_BUCKET']}", f"s3://{args['GOLD_BUCKET']}"
spark = GlueContext(SparkContext()).spark_session


def put(df, name):
    df.write.mode("overwrite").parquet(f"{GOLD}/{name}/")


def pick(df, wanted):
    """Select the columns that exist, keeping ccn first."""
    return df.select("ccn", *[c for c in wanted if c in df.columns])


s3 = boto3.client("s3")


def state(bucket, key):
    try:
        return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()
    except s3.exceptions.NoSuchKey:
        return None


def run():
    st = spark.read.parquet(f"{SILVER}/staffing_daily/")
    prov = spark.read.parquet(f"{SILVER}/cms/nh_providerinfo/")

    # ---- dimensions ----
    dim_facility = pick(prov, [
        "provider_name", "city_town", "state", "zip_code", "county_parish", "ownership_type",
        "provider_type", "number_of_certified_beds", "overall_rating", "staffing_rating",
        "reported_total_nurse_staffing_hours_per_resident_per_day",
        "total_nursing_staff_turnover", "registered_nurse_turnover",
        "number_of_substantiated_complaints", "latitude", "longitude"])
    dim_date = (st.select("work_date").distinct()
                .withColumn("year", F.year("work_date")).withColumn("month", F.month("work_date"))
                .withColumn("quarter", F.quarter("work_date"))
                .withColumn("day_of_week", F.date_format("work_date", "E"))
                .withColumn("is_weekend", F.dayofweek("work_date").isin(1, 7)))
    put(dim_facility, "dim_facility")
    put(dim_date, "dim_date")

    # ---- fact ----
    fact = st.select("ccn", "work_date", "month", F.col("STATE").alias("state"),
                     F.col("MDScensus").alias("census"), "total_hours", "employed_hours",
                     "contract_hours", "hours_per_resident_day", "census_zero", "no_hours_reported")
    fact.write.mode("overwrite").partitionBy("month").parquet(f"{GOLD}/fact_staffing_daily/")

    valid = fact.filter(~F.col("census_zero") & ~F.col("no_hours_reported"))

    # ---- metric marts ----
    agg = [F.sum("total_hours").alias("total_hours"),
           F.sum("census").alias("resident_days"),
           F.sum("contract_hours").alias("contract_hours")]
    mart_state_month = (valid.groupBy("state", "month").agg(*agg)
        .withColumn("hours_per_resident_day", F.col("total_hours") / F.col("resident_days"))
        .withColumn("contract_share", F.col("contract_hours") / F.col("total_hours")))
    put(mart_state_month, "mart_state_month")

    # rehospitalization: measure 521 = short-stay residents rehospitalized after admission
    claims = spark.read.parquet(f"{SILVER}/cms/nh_qualitymsr_claims/")
    rehosp = (claims.filter(F.col("measure_code") == "521")
              .select("ccn", F.col("adjusted_score").cast("double").alias("rehospitalization_pct")))

    fq = (valid.groupBy("ccn").agg(*agg, F.avg("census").alias("avg_census"),
                                   F.countDistinct("work_date").alias("days_valid"))
          .withColumn("hours_per_resident_day", F.col("total_hours") / F.col("resident_days"))
          .withColumn("contract_share", F.col("contract_hours") / F.col("total_hours")))
    mart_facility_quarter = (fq.join(dim_facility, "ccn", "left")
        .withColumn("occupancy", F.col("avg_census") / F.col("number_of_certified_beds").cast("double"))
        .join(rehosp, "ccn", "left"))
    put(mart_facility_quarter, "mart_facility_quarter")
    print("gold written")


silver_sig = state(args["SILVER_BUCKET"], "_state/raw_signature.txt")
if not FORCE and silver_sig and state(args["GOLD_BUCKET"], "_state/silver_signature.txt") == silver_sig:
    print("Silver unchanged since the last successful Gold build; nothing to do.")
else:
    run()
    if silver_sig:
        s3.put_object(Bucket=args["GOLD_BUCKET"], Key="_state/silver_signature.txt", Body=silver_sig.encode())
