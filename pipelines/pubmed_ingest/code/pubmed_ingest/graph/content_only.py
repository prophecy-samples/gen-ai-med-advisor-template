from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_ingest.config.ConfigStore import *
from pubmed_ingest.udfs.UDFs import *

def content_only(spark: SparkSession, pubmed_bronze_baseline_content: DataFrame) -> DataFrame:
    return pubmed_bronze_baseline_content.select(col("content").alias("text"))
