CATALOG = "workspace"
SCHEMA = "profitable"
VOLUME = "raw"
DB = f"{CATALOG}.{SCHEMA}"
RAW_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

TRAIN_END = 2018
VAL_END = 2020
TEST_END = 2023
COST_BPS = 10


def get_spark():
    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.getOrCreate()
