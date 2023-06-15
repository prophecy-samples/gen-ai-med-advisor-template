from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pubmed_ingest.config.ConfigStore import *
from pubmed_ingest.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_ingest.graph import *

def pipeline(spark: SparkSession) -> None:
    df_pubmed_bronze_baseline_xml = pubmed_bronze_baseline_xml(spark)
    df_valid_files_only = valid_files_only(spark, df_pubmed_bronze_baseline_xml)
    df_with_baseline = with_baseline(spark, df_valid_files_only)
    df_parallelize = parallelize(spark, df_with_baseline)
    df_Reformat_1 = Reformat_1(spark, df_parallelize)
    Script_1(spark, df_Reformat_1)
    df_pubmed_bronze_baseline_content = pubmed_bronze_baseline_content(spark)
    df_pubmed_bronze_articles = pubmed_bronze_articles(spark)
    df_content_only = content_only(spark, df_pubmed_bronze_baseline_content)
    pubmed_bronze_baseline_text(spark, df_content_only)
    pubmed_bronze_articles_tbl(spark, df_pubmed_bronze_articles)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pubmed_ingest")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/pubmed_ingest")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
