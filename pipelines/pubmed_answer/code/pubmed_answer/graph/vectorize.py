from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_answer.config.ConfigStore import *
from pubmed_answer.udfs.UDFs import *

def vectorize(spark: SparkSession, question: DataFrame) -> DataFrame:
    from spark_ai.llms.openai import OpenAiLLM
    from pyspark.dbutils import DBUtils
    OpenAiLLM(api_key = DBUtils(spark).secrets.get(scope = "open_ai", key = "token")).register_udfs(spark = spark)

    return question\
        .withColumn("_row_num", row_number().over(Window.partitionBy().orderBy(col("question"))))\
        .withColumn("_group_num", ceil(col("_row_num") / 20))\
        .withColumn("_data", struct(col("*")))\
        .groupBy(col("_group_num"))\
        .agg(collect_list(col("_data")).alias("_data"), collect_list(col("question")).alias("_texts"))\
        .withColumn("_embedded", expr(f"openai_embed_texts(_texts)"))\
        .select(
          col("_texts"),
          col("_embedded.embeddings").alias("_embeddings"),
          col("_embedded.error").alias("openai_error"),
          col("_data")
        )\
        .select(expr("explode_outer(arrays_zip(_embeddings, _data))").alias("_content"), col("openai_error"))\
        .select(col("_content._embeddings").alias("openai_embedding"), col("openai_error"), col("_content._data.*"))\
        .drop("_row_num")\
        .drop("_group_num")
