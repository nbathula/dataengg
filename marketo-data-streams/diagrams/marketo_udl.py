"""
Marketo Data Streams — Architecture Diagram
Generates marketo_udl.png using the 'diagrams' library.

Install: pip install diagrams
Run:     python marketo_udl.py
Output:  marketo_udl.png
"""

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.compute import Lambda
from diagrams.aws.integration import SQS, Eventbridge
from diagrams.aws.storage import S3
from diagrams.aws.network import APIGateway
from diagrams.onprem.queue import Kafka
from diagrams.onprem.analytics import Spark
from diagrams.azure.analytics import Databricks
from diagrams.onprem.client import Users
from diagrams.onprem.analytics import Tableau
from diagrams.saas.chat import Slack
from diagrams.custom import Custom
import os

# ---------------------------------------------------------------------------
# Fallback: use generic nodes if a specific icon isn't available
# ---------------------------------------------------------------------------
from diagrams.programming.flowchart import Database, StartEnd

graph_attr = {
    "fontsize": "13",
    "bgcolor": "white",
    "pad": "0.5",
    "splines": "ortho",
    "rankdir": "LR",
    "fontname": "Helvetica",
}

cluster_attr = {
    "fontsize": "12",
    "fontname": "Helvetica Bold",
}

with Diagram(
    "Marketo Data Streams (MDS)\nIceberg UDL — Shared Curated Core, Native Marts per Engine",
    filename="marketo_udl",
    outformat="png",
    graph_attr=graph_attr,
    show=False,
):

    # ── Sources ──────────────────────────────────────────────────────────
    with Cluster("Marketo Sources", graph_attr=cluster_attr):
        hist = S3("Historical + Batch\n(Bulk Export Files)")
        webhooks = Eventbridge("Webhooks\n(near-realtime)")

    # ── Ingestion ─────────────────────────────────────────────────────────
    with Cluster("Ingestion Layer", graph_attr=cluster_attr):
        apigw  = APIGateway("API Gateway\n(HMAC auth)")
        lam    = Lambda("Lambda\n(fan-out)")
        dlq    = SQS("SQS DLQ\n(failure replay)")
        s3land = S3("S3 Landing\nsource/date/hour")
        kafka  = Kafka("Kafka\n(per-event topics)")

    # ── Processing ────────────────────────────────────────────────────────
    with Cluster("Processing Layer (Databricks)", graph_attr=cluster_attr):
        autoloader = Spark("Auto Loader\n(batch / micro-batch)")
        sss        = Spark("Spark Structured\nStreaming")

    # ── Unity Catalog + Iceberg Core ─────────────────────────────────────
    with Cluster("Unity Catalog — Governance · Lineage · Access", graph_attr=cluster_attr):
        with Cluster("Shared Core — Apache Iceberg (Single Source of Truth)"):
            bronze  = Database("RAW / Bronze\nappend-only · source-fidelity")
            gold    = Database("CURATED / Gold\nentity-resolved · business-canonical\n★ Agent Grounding Layer")

    # ── Native Marts ──────────────────────────────────────────────────────
    with Cluster("Native Marts (per Engine)", graph_attr=cluster_attr):
        snowflake  = Database("Snowflake\nBI-Mart (native tables)\nhigh-frequency delta")
        db_ml      = Databricks("Databricks\nML Marts · Feature Store\nML-fresh data")

    # ── Consumers ─────────────────────────────────────────────────────────
    with Cluster("Consumers", graph_attr=cluster_attr):
        bi_tools = Tableau("BI Tools\nMetabase · Tableau · Domo")
        ml_tools = Users("ML Tooling\nNotebooks · Models · MLflow")
        ai_agent = Users("AI Agents\n(grounded on Gold)")

    # ── Edges ─────────────────────────────────────────────────────────────
    hist     >> Edge(label="drop")       >> apigw
    webhooks >> Edge(label="drop")       >> apigw
    apigw    >> Edge(label="validate")   >> lam
    lam      >> Edge(label="on failure") >> dlq
    lam      >> Edge(label="lands")      >> s3land
    lam      >> Edge(label="produces")   >> kafka

    s3land >> Edge(label="auto-loads")  >> autoloader
    kafka  >> Edge(label="consumes")    >> sss

    autoloader >> Edge(label="appends") >> bronze
    sss        >> Edge(label="appends") >> bronze

    bronze >> Edge(label="dbt transforms") >> gold

    gold >> Edge(label="materializes\nBI-ready data")  >> snowflake
    gold >> Edge(label="materializes\nML-rich data")   >> db_ml

    snowflake >> Edge(label="dashboards") >> bi_tools
    db_ml     >> Edge(label="train/score") >> ml_tools
    gold      >> Edge(label="REST API\nquery/ground") >> ai_agent


print("Generated: marketo_udl.png")
