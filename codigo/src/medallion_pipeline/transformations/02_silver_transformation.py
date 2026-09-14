"""
Silver layer transformation for Telco Churn.

Pattern aligned with professor example:
- quarantine tables per entity
- clean views for valid records
- AUTO CDC SCD2 for customer history
- stream-static enrichment for usage + monthly labels
"""

import pyspark.pipelines as dp
from pyspark.sql.functions import col, coalesce, expr, to_timestamp, when, lit

import sys

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

# Get the bundle root path injected by Databricks at runtime
bundle_source_path = spark.conf.get("bundle.sourcePath")
sys.path.append(bundle_source_path)

from src.medallion_pipeline.rules.customers import (
    get_customer_rules,
    get_usage_rules,
    get_label_rules,
    get_interaction_rules,
)
from src.medallion_pipeline.quality import build_quarantine_expression


# ---------------------------------------------------------------------------
# 1) Customers: quarantine + AUTO CDC SCD2 history
# ---------------------------------------------------------------------------

cust_quarantine_table = "silver_quarantine_customers"
cust_tmp_eval = "tmp_eval_customers"
cust_clean_view = "vw_clean_customers"
cust_quarantine_flow = "flow_quarantine_customers"
cust_history_table = "silver_customers_history"

cust_rules = get_customer_rules()
cust_expr = build_quarantine_expression(cust_rules)

dp.create_streaming_table(name=cust_quarantine_table)


@dp.table(name=cust_tmp_eval, temporary=True)
@dp.expect_all(cust_rules)
def eval_customers():
    return (
        spark.readStream
             .option("skipChangeCommits", "true")
             .table("bronze_customers")
             .withColumn(
                 "customer_sequence_at",
                 coalesce(
                     to_timestamp(col("signup_date")),      # ✅ primero fecha de negocio real
                     to_timestamp(col("customer_updated_at")),
                     lit("2023-01-01 00:00:00").cast("timestamp"),
                 )
             )
             .withColumn("is_quarantined", expr(cust_expr))
    )


@dp.append_flow(target=cust_quarantine_table, name=cust_quarantine_flow)
def quarantine_customers():
    return (
        spark.readStream
             .table(cust_tmp_eval)
             .filter("is_quarantined = true")
             .drop("is_quarantined")
    )


@dp.view(name=cust_clean_view)
def clean_customers():
    return (
        spark.readStream
             .table(cust_tmp_eval)
             .filter("is_quarantined = false")
             .drop("is_quarantined")
    )


dp.create_streaming_table(
    name=cust_history_table,
    comment="SCD2 customer history maintained through AUTO CDC.",
)


dp.create_auto_cdc_flow(
    target=cust_history_table,
    source=cust_clean_view,
    keys=["customer_id"],
    sequence_by=col("customer_sequence_at"),
    except_column_list=["ingestion_timestamp", "source_file", "customer_sequence_at"],
    stored_as_scd_type="2",
)


# ---------------------------------------------------------------------------
# 2) Usage: quarantine + clean view
# ---------------------------------------------------------------------------

usage_quarantine_table = "silver_quarantine_usage"
usage_tmp_eval = "tmp_eval_usage"
usage_clean_view = "vw_clean_usage"
usage_quarantine_flow = "flow_quarantine_usage"

usage_rules = get_usage_rules()
usage_expr = build_quarantine_expression(usage_rules)

dp.create_streaming_table(name=usage_quarantine_table)


@dp.table(name=usage_tmp_eval, temporary=True)
@dp.expect_all(usage_rules)
def eval_usage():
    return (
        spark.readStream
             .option("skipChangeCommits", "true")
             .table("bronze_usage")
             .withColumn("is_quarantined", expr(usage_expr))
    )


@dp.append_flow(target=usage_quarantine_table, name=usage_quarantine_flow)
def quarantine_usage():
    return (
        spark.readStream
             .table(usage_tmp_eval)
             .filter("is_quarantined = true")
             .drop("is_quarantined")
    )


@dp.view(name=usage_clean_view)
def clean_usage():
    return (
        spark.readStream
             .table(usage_tmp_eval)
             .filter("is_quarantined = false")
             .drop("is_quarantined")
             .withColumn("usage_event_time", to_timestamp(expr("concat(year_month, '-01 00:00:00')")))
    )


# ---------------------------------------------------------------------------
# 3) Labels: quarantine + clean view
# ---------------------------------------------------------------------------

labels_quarantine_table = "silver_quarantine_labels"
labels_tmp_eval = "tmp_eval_labels"
labels_clean_view = "vw_clean_labels"
labels_quarantine_flow = "flow_quarantine_labels"

labels_rules = get_label_rules()
labels_expr = build_quarantine_expression(labels_rules)

dp.create_streaming_table(name=labels_quarantine_table)


@dp.table(name=labels_tmp_eval, temporary=True)
@dp.expect_all(labels_rules)
def eval_labels():
    return (
        spark.readStream
             .option("skipChangeCommits", "true")
             .table("bronze_labels")
             .withColumn("label_available_date", to_timestamp(col("label_available_date")))
             .withColumn("is_quarantined", expr(labels_expr))
    )


@dp.append_flow(target=labels_quarantine_table, name=labels_quarantine_flow)
def quarantine_labels():
    return (
        spark.readStream
             .table(labels_tmp_eval)
             .filter("is_quarantined = true")
             .drop("is_quarantined")
    )


@dp.view(name=labels_clean_view)
def clean_labels():
    return (
        spark.readStream
             .table(labels_tmp_eval)
             .filter("is_quarantined = false")
             .drop("is_quarantined")
    )


# ---------------------------------------------------------------------------
# 4) Interactions: quarantine + clean table
# ---------------------------------------------------------------------------

interactions_quarantine_table = "silver_quarantine_interactions"
interactions_tmp_eval = "tmp_eval_interactions"
interactions_clean_view = "vw_clean_interactions"
interactions_quarantine_flow = "flow_quarantine_interactions"
interactions_clean_table = "silver_interactions_clean"

interactions_rules = get_interaction_rules()
interactions_expr = build_quarantine_expression(interactions_rules)

dp.create_streaming_table(name=interactions_quarantine_table)
dp.create_streaming_table(name=interactions_clean_table)


@dp.table(name=interactions_tmp_eval, temporary=True)
@dp.expect_all(interactions_rules)
def eval_interactions():
    return (
        spark.readStream
             .option("skipChangeCommits", "true")
             .table("bronze_interactions")
             .withColumn("timestamp", to_timestamp(col("timestamp")))
             .withColumn("is_quarantined", expr(interactions_expr))
    )


@dp.append_flow(target=interactions_quarantine_table, name=interactions_quarantine_flow)
def quarantine_interactions():
    return (
        spark.readStream
             .table(interactions_tmp_eval)
             .filter("is_quarantined = true")
             .drop("is_quarantined")
    )


@dp.view(name=interactions_clean_view)
def clean_interactions():
    return (
        spark.readStream
             .table(interactions_tmp_eval)
             .filter("is_quarantined = false")
             .drop("is_quarantined")
    )


@dp.append_flow(target=interactions_clean_table, name="flow_silver_interactions_clean")
def interactions_to_clean_table():
    return spark.readStream.table(interactions_clean_view)


# ---------------------------------------------------------------------------
# 5) Unified events: stream-stream join (usage + labels)
# ---------------------------------------------------------------------------

churn_events_table = "silver_churn_events"
churn_events_flow = "flow_silver_churn_events"

dp.create_streaming_table(
    name=churn_events_table,
    comment="Unified silver events: clean usage enriched with valid monthly churn labels.",
)


@dp.append_flow(target=churn_events_table, name=churn_events_flow)
def silver_churn_events():
    # Stream-static join reduces state pressure vs stream-stream join.
    df_usage = spark.readStream.table(usage_clean_view).alias("u")

    df_labels = (
        spark.read
             .table("bronze_labels")
             .filter("customer_id IS NOT NULL AND year_month IS NOT NULL")
             .withColumn("label_available_date", to_timestamp(col("label_available_date")))
             .select("customer_id", "year_month", "churn_date", "label_available_date")
             .alias("l")
    )

    return (
        df_usage.join(
            df_labels,
            on=[
                col("u.customer_id") == col("l.customer_id"),
                col("u.year_month") == col("l.year_month"),
            ],
            how="leftOuter",
        )
        .select(
            col("u.*"),
            col("l.churn_date"),
            col("l.label_available_date"),
        )
    )
