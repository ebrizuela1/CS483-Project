# Databricks notebook source
# runs in the workspace only, downloads need the /Volumes mount
import os
import shutil
import urllib.request
import zipfile

from pyspark.sql import functions as F

from config import DB, RAW_DIR, get_spark

spark = get_spark()
HF = "https://huggingface.co/datasets/Zihan1004/FNSPID/resolve/main"
FILES = [
    "Stock_price/full_history.zip",
    "Stock_news/All_external.csv",
    "Stock_news/nasdaq_exteral_data.csv",
]

# ingestion
def download(path):
    dst = f"{RAW_DIR}/{os.path.basename(path)}"
    with urllib.request.urlopen(f"{HF}/{path}") as r:
        if os.path.exists(dst) and os.path.getsize(dst) == int(r.headers["Content-Length"]):
            return
        with open(dst, "wb") as f:
            shutil.copyfileobj(r, f, 64 << 20)      # stream response to disk in 64 MB chunks


for path in FILES:
    download(path)

prices_dir = f"{RAW_DIR}/prices"
# marker written last so a crashed extract reruns instead of leaving partial tickers
done = f"{prices_dir}/_SUCCESS"
if not os.path.exists(done):
    with zipfile.ZipFile(f"{RAW_DIR}/full_history.zip") as z:
        z.extractall(prices_dir)
    open(done, "w").close()

# COMMAND ----------

# Prices Table
# lowercase, spaces to underscores, delta rejects spaces
def clean_cols(df):
    return df.toDF(*[c.strip().lower().replace(" ", "_") for c in df.columns])


prices = (
    spark.read.options(header=True, pathGlobFilter="*.csv")
    .csv(f"{prices_dir}/full_history")
    .withColumn("ticker", F.regexp_replace(F.col("_metadata.file_name"), r"\.csv$", ""))
)
clean_cols(prices).write.mode("overwrite").saveAsTable(f"{DB}.prices_raw")

# COMMAND ----------

# News Table
# multiLine because article text has quoted newlines
opts = dict(header=True, multiLine=True, escape='"')
external = spark.read.options(**opts).csv(f"{RAW_DIR}/All_external.csv").withColumn("source", F.lit("external"))
nasdaq = (
    spark.read.options(**opts)
    .csv(f"{RAW_DIR}/nasdaq_exteral_data.csv")
    .drop("Unnamed: 0")
    .withColumn("source", F.lit("nasdaq"))
)
clean_cols(external.unionByName(nasdaq)).write.mode("overwrite").saveAsTable(f"{DB}.news_raw")
