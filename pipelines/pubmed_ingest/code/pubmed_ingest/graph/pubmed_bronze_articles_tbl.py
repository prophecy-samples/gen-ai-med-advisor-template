from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_ingest.config.ConfigStore import *
from pubmed_ingest.udfs.UDFs import *

def pubmed_bronze_articles_tbl(spark: SparkSession, pubmed_bronze_articles: DataFrame):
    pubmed_bronze_articles.write\
        .format("delta")\
        .option("mergeSchema", True)\
        .option("overwriteSchema", True)\
        .mode("overwrite")\
        .saveAsTable(f"prophecy_demos.pubmed_bronze.articles")
