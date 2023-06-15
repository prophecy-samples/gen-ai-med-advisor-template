from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_ingest.config.ConfigStore import *
from pubmed_ingest.udfs.UDFs import *

def pubmed_bronze_baseline_content(spark: SparkSession) -> DataFrame:
    from spark_ai.webapps import WebUtils
    WebUtils().register_udfs(spark)
    df1 = spark.range(1)

    return df1\
        .withColumn("url", lit("https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"))\
        .withColumn("content", expr("web_scrape(url)"))
