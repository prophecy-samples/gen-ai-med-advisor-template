from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_vectorize.config.ConfigStore import *
from pubmed_vectorize.udfs.UDFs import *

def clean_values(spark: SparkSession, pubmed_bronze_articles_tbl: DataFrame) -> DataFrame:
    return pubmed_bronze_articles_tbl.select(
        array_join(col("Article.Abstract.AbstractText._VALUE"), "; ").alias("abstract"), 
        col("Article.ArticleTitle").alias("title"), 
        col("PMID._VALUE").cast(StringType()).alias("pmid")
    )
