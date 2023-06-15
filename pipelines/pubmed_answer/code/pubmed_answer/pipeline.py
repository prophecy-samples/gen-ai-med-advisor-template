from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pubmed_answer.config.ConfigStore import *
from pubmed_answer.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_answer.graph import *

def pipeline(spark: SparkSession) -> None:
    df_question = question(spark)
    df_vectorize = vectorize(spark, df_question)
    df_PineconeLookup_1 = PineconeLookup_1(spark, df_vectorize)
    df_FlattenSchema_1 = FlattenSchema_1(spark, df_PineconeLookup_1)
    df_pubmed_silver_vectorized = pubmed_silver_vectorized(spark)
    df_Join_1 = Join_1(spark, df_FlattenSchema_1, df_pubmed_silver_vectorized)
    df_Aggregate_1 = Aggregate_1(spark, df_Join_1)
    df_OpenAI_1 = OpenAI_1(spark, df_Aggregate_1)
    df_Reformat_1 = Reformat_1(spark, df_OpenAI_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pubmed_answer")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/pubmed_answer")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
