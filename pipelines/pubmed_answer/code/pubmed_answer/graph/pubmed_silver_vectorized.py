from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_answer.config.ConfigStore import *
from pubmed_answer.udfs.UDFs import *

def pubmed_silver_vectorized(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"prophecy_demos.pubmed_silver.abstracts_vectorized")
