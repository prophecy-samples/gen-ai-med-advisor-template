from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_ingest.config.ConfigStore import *
from pubmed_ingest.udfs.UDFs import *

def with_baseline(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        concat(lit("https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"), col("_href")).alias("url"), 
        col("_href").alias("file_name")
    )
