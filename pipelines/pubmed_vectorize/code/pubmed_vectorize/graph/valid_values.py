from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_vectorize.config.ConfigStore import *
from pubmed_vectorize.udfs.UDFs import *

def valid_values(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((length(col("abstract")) > lit(0)) & (length(col("title")) > lit(0))))
