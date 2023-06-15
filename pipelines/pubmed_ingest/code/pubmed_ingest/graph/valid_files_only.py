from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_ingest.config.ConfigStore import *
from pubmed_ingest.udfs.UDFs import *

def valid_files_only(spark: SparkSession, pubmed_bronze_baseline_xml: DataFrame) -> DataFrame:
    return pubmed_bronze_baseline_xml.filter((regexp_extract(col("_href"), "(.xml.gz$)", 1) == lit(".xml.gz")))
