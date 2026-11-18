# Databricks notebook source
from src.config import DB, VOLUME, get_spark

spark = get_spark()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DB}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {DB}.{VOLUME}")

# COMMAND ----------

spark.sql(f"SHOW VOLUMES IN {DB}").show()
spark.range(5).show()
