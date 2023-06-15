from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_answer.config.ConfigStore import *
from pubmed_answer.udfs.UDFs import *

def Aggregate_1(spark: SparkSession, Join_1: DataFrame) -> DataFrame:
    return Join_1.agg(
        array_join(
            collect_list(
              concat(
                lit("PMID: "), 
                col("match_pmid"), 
                lit(";\n Title: "), 
                col("title"), 
                lit(";\n Abstract: "), 
                col("abstract")
              )
            ), 
            "; "
          )\
          .alias("context"), 
        first(col("question")).alias("question")
    )
