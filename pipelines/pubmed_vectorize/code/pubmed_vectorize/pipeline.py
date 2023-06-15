from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pubmed_vectorize.config.ConfigStore import *
from pubmed_vectorize.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_vectorize.graph import *

def pipeline(spark: SparkSession) -> None:
    df_pubmed_bronze_articles_tbl = pubmed_bronze_articles_tbl(spark)
    df_clean_values = clean_values(spark, df_pubmed_bronze_articles_tbl)
    df_valid_values = valid_values(spark, df_clean_values)
    df_migraine_only = migraine_only(spark, df_valid_values)
    df_latest_first = latest_first(spark, df_migraine_only)
    df_first_5000 = first_5000(spark, df_latest_first)
    df_OpenAI_1 = OpenAI_1(spark, df_first_5000)
    pubmed_silver_vectorized(spark, df_OpenAI_1)
    df_pubmed_silver_vectorized_1 = pubmed_silver_vectorized_1(spark)
    df_parallelize = parallelize(spark, df_pubmed_silver_vectorized_1)
    pubmed_silver_vectors_pinecone(spark, df_parallelize)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pubmed_vectorize")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/pubmed_vectorize")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
