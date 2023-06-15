from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_ingest.config.ConfigStore import *
from pubmed_ingest.udfs.UDFs import *

def Reformat_1(spark: SparkSession, parallelize: DataFrame) -> DataFrame:
    return parallelize.select(download(col("url"), col("file_name")).alias("content"), col("file_name"))
