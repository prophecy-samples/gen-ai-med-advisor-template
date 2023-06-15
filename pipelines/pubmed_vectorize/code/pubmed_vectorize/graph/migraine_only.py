from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_vectorize.config.ConfigStore import *
from pubmed_vectorize.udfs.UDFs import *

def migraine_only(spark: SparkSession, valid_values: DataFrame) -> DataFrame:
    return valid_values.filter(
        ((instr(col("title"), "migraine") > lit(0)) | (instr(col("abstract"), "migraine") > lit(0)))
    )
