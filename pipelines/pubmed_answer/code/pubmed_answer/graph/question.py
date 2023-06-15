from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_answer.config.ConfigStore import *
from pubmed_answer.udfs.UDFs import *

def question(spark: SparkSession) -> DataFrame:
    df1 = spark.range(1)

    return df1.select(lit("What are common diseases associated with migraines?").alias("question"))
