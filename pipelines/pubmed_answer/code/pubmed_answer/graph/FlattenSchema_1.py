from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_answer.config.ConfigStore import *
from pubmed_answer.udfs.UDFs import *

def FlattenSchema_1(spark: SparkSession, PineconeLookup_1: DataFrame) -> DataFrame:
    return PineconeLookup_1\
        .withColumn("pinecone_matches", explode_outer("pinecone_matches"))\
        .select(col("pinecone_matches.id").alias("match_pmid"), col("question"))
