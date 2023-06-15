from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_vectorize.config.ConfigStore import *
from pubmed_vectorize.udfs.UDFs import *

def parallelize(spark: SparkSession, pubmed_silver_vectorized_1: DataFrame) -> DataFrame:
    return pubmed_silver_vectorized_1.repartition(10)
