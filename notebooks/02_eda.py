# Databricks notebook source
import os

import matplotlib.pyplot as plt
from pyspark.sql import functions as F

from config import DB, RAW_DIR, get_spark

spark = get_spark()
FIG = f"{RAW_DIR}/figures"
os.makedirs(FIG, exist_ok=True)
prices = spark.table(f"{DB}.prices_raw").withColumn("d", F.to_date("date"))
raw = spark.table(f"{DB}.news_raw").withColumn("ts", F.to_timestamp(F.regexp_replace("date", " UTC$", "")))
# rows without a ticker are untagged Reuters, Bloomberg, and lenta.ru news, copied into both files
news = raw.filter(F.col("stock_symbol").isNotNull())
untagged = raw.filter(F.col("stock_symbol").isNull())


def null_rates(df, cols):
    return df.select([F.round(F.avg(F.col(c).isNull().cast("int")), 3).alias(c) for c in cols])


# COMMAND ----------

for t in ["prices_raw", "news_raw"]:
    d = spark.sql(f"DESCRIBE DETAIL {DB}.{t}").first()
    print(f"{t} delta size {d.sizeInBytes / 1e9:.2f} GB, {d.numFiles} files")

# COMMAND ----------

prices.agg(
    F.count("*").alias("rows"),
    F.countDistinct("ticker").alias("tickers"),
    F.min("d").alias("first"),
    F.max("d").alias("last"),
    F.sum(F.col("d").isNull().cast("int")).alias("bad_dates"),
).show()
null_rates(prices, ["volume", "open", "high", "low", "close", "adj_close"]).show()
print("duplicate ticker days", prices.groupBy("ticker", "d").count().filter("count > 1").count())

# COMMAND ----------

untagged.groupBy("source").agg(
    F.count("*").alias("rows"),
    F.min("ts").alias("first"),
    F.max("ts").alias("last"),
).show()
untagged.filter(F.col("source") == "nasdaq").groupBy(
    F.regexp_extract("url", "^https?://([^/]+)", 1).alias("host")
).count().orderBy(F.desc("count")).show(5)
untagged_urls = {s: untagged.filter(F.col("source") == s).select("url") for s in ["external", "nasdaq"]}
print("untagged urls shared by both files", untagged_urls["external"].intersect(untagged_urls["nasdaq"]).count())

# COMMAND ----------

news.groupBy("source").agg(
    F.count("*").alias("rows"),
    F.countDistinct("stock_symbol").alias("tickers"),
    F.min("ts").alias("first"),
    F.max("ts").alias("last"),
    F.round(F.avg(F.col("ts").isNull().cast("int")), 4).alias("bad_dates"),
    F.countDistinct("url").alias("distinct_urls"),
).show()
text_cols = ["article_title", "url", "publisher", "article", "lsa_summary"]
for s in ["external", "nasdaq"]:
    print(s)
    null_rates(news.filter(F.col("source") == s), text_cols).show()

# COMMAND ----------

news.agg(
    F.count("*").alias("rows"),
    F.countDistinct("url").alias("distinct_urls"),
    F.countDistinct("stock_symbol", "url").alias("distinct_ticker_url"),
).show()
urls = {s: news.filter(F.col("source") == s).select("url").distinct() for s in ["external", "nasdaq"]}
print("urls in both files", urls["external"].intersect(urls["nasdaq"]).count())
print("tickers per url")
news.groupBy("url").agg(F.countDistinct("stock_symbol").alias("tickers")).groupBy("tickers").count().orderBy(
    "tickers"
).show(10)

# COMMAND ----------

per_year = news.groupBy(F.year("ts").alias("year"), "source").count().toPandas()
per_year.pivot(index="year", columns="source", values="count").fillna(0).plot.bar(stacked=True, figsize=(10, 4))
plt.ylabel("articles")
plt.title("FNSPID articles with a ticker per year")
plt.tight_layout()
plt.savefig(f"{FIG}/articles_per_year.png", dpi=150)

# COMMAND ----------

# 2010 is the first full year of news, tickers that never get news would only dilute the share
news_weeks = news.select(F.col("stock_symbol").alias("ticker"), F.date_trunc("week", "ts").alias("week")).distinct()
news_tickers = news_weeks.select("ticker").distinct()
price_weeks = (
    prices.filter(F.year("d") >= 2010)
    .select("ticker", F.date_trunc("week", "d").alias("week"))
    .distinct()
    .join(news_tickers, "ticker")
)
coverage = (
    price_weeks.join(news_weeks.withColumn("has_news", F.lit(1)), ["ticker", "week"], "left")
    .groupBy(F.year("week").alias("year"))
    .agg(F.round(F.avg(F.coalesce("has_news", F.lit(0))), 3).alias("coverage"))
    .orderBy("year")
    .toPandas()
)
print(coverage.to_string(index=False))
coverage.plot(x="year", y="coverage", legend=False, figsize=(10, 4))
plt.ylabel("share of ticker weeks with news")
plt.title("weekly news coverage, tickers with news, 2010 on")
plt.tight_layout()
plt.savefig(f"{FIG}/weekly_coverage.png", dpi=150)

print("news tickers", news_tickers.count())
print("news tickers with prices", news_tickers.join(prices.select("ticker").distinct(), "ticker").count())
