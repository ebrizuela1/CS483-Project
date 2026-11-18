# Databricks notebook source
import os

import matplotlib.pyplot as plt
from pyspark.sql import functions as F

from src.config import DB, RAW_DIR, get_spark

spark = get_spark()
FIG = f"{RAW_DIR}/figures"
os.makedirs(FIG, exist_ok=True)
prices = spark.table(f"{DB}.prices_raw").withColumn("d", F.to_date("date"))
news = spark.table(f"{DB}.news_raw").withColumn("ts", F.to_timestamp(F.regexp_replace("date", " UTC$", "")))


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

news.groupBy("source").agg(
    F.count("*").alias("rows"),
    F.countDistinct("stock_symbol").alias("tickers"),
    F.min("ts").alias("first"),
    F.max("ts").alias("last"),
    F.round(F.avg(F.col("ts").isNull().cast("int")), 4).alias("bad_dates"),
    F.countDistinct("url").alias("distinct_urls"),
).show()
text_cols = ["article_title", "stock_symbol", "url", "publisher", "article", "lsa_summary"]
for s in ["external", "nasdaq"]:
    print(s)
    null_rates(news.filter(F.col("source") == s), text_cols).show()

key = ["stock_symbol", "article_title", "ts"]
print("exact duplicate rows on ticker, title, time", news.count() - news.dropDuplicates(key).count())

# COMMAND ----------

per_year = news.groupBy(F.year("ts").alias("year"), "source").count().toPandas()
per_year.pivot(index="year", columns="source", values="count").fillna(0).plot.bar(stacked=True, figsize=(10, 4))
plt.ylabel("articles")
plt.title("FNSPID articles per year")
plt.tight_layout()
plt.savefig(f"{FIG}/articles_per_year.png", dpi=150)

# COMMAND ----------

# share of ticker weeks that have at least one article, among tickers with prices that week
price_weeks = prices.select("ticker", F.date_trunc("week", "d").alias("week")).distinct()
news_weeks = news.select(F.col("stock_symbol").alias("ticker"), F.date_trunc("week", "ts").alias("week")).distinct()
coverage = (
    price_weeks.join(news_weeks.withColumn("has_news", F.lit(1)), ["ticker", "week"], "left")
    .groupBy(F.year("week").alias("year"))
    .agg(F.avg(F.coalesce("has_news", F.lit(0))).alias("coverage"))
    .orderBy("year")
    .toPandas()
)
print(coverage.to_string(index=False))
coverage.plot(x="year", y="coverage", legend=False, figsize=(10, 4))
plt.ylabel("share of ticker weeks with news")
plt.title("weekly news coverage")
plt.tight_layout()
plt.savefig(f"{FIG}/weekly_coverage.png", dpi=150)

news_tickers = news.select(F.col("stock_symbol").alias("ticker")).distinct()
print("news tickers with prices", news_tickers.join(prices.select("ticker").distinct(), "ticker").count())
