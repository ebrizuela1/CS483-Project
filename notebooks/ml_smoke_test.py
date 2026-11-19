# Databricks notebook source
# toy fits to check pyspark.ml estimators run on serverless through spark connect #8
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.sql import functions as F

from config import get_spark

spark = get_spark()

# y is linear in x1 and x2 plus noise, x3 is pure noise, so a working fit beats chance clearly
df = (
    spark.range(2000)
    .select(*[F.rand(seed=i).alias(f"x{i}") for i in (1, 2, 3)], F.randn(seed=4).alias("noise"))
    .withColumn("y", 3 * F.col("x1") - 2 * F.col("x2") + 0.3 * F.col("noise"))
    .withColumn("label", (F.col("y") > 0.5).cast("double"))
)
train, test = df.randomSplit([0.8, 0.2], seed=42)
assembler = VectorAssembler(inputCols=["x1", "x2", "x3"], outputCol="features")


def fit_and_score(model, evaluator):
    preds = Pipeline(stages=[assembler, model]).fit(train).transform(test)
    return evaluator.evaluate(preds)


# COMMAND ----------

# classifiers, label is y above 0.5
auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
for model in [LogisticRegression(labelCol="label"), GBTClassifier(labelCol="label", maxIter=20, seed=42)]:
    print(f"{type(model).__name__} test auc {fit_and_score(model, auc):.3f}")

# COMMAND ----------

# regressors, target is y itself
rmse = RegressionEvaluator(labelCol="y", metricName="rmse")
for model in [LinearRegression(labelCol="y"), GBTRegressor(labelCol="y", maxIter=20, seed=42)]:
    print(f"{type(model).__name__} test rmse {fit_and_score(model, rmse):.3f}")
