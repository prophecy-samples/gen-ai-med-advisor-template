from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_answer.config.ConfigStore import *
from pubmed_answer.udfs.UDFs import *

def OpenAI_1(spark: SparkSession, Aggregate_1: DataFrame) -> DataFrame:
    from spark_ai.llms.openai import OpenAiLLM
    from pyspark.dbutils import DBUtils
    OpenAiLLM(api_key = DBUtils(spark).secrets.get(scope = "open_ai", key = "token")).register_udfs(spark = spark)

    return Aggregate_1\
        .withColumn("_context", col("context"))\
        .withColumn("_query", col("question"))\
        .withColumn(
          "openai_answer",
          expr(
            "openai_answer_question(_context, _query, \" Answer the question to the best of your ability using your knowledge and based on the context below. Make sure to provide references at the end of your answer with PMID.\n\nContext:\n```\n{context}\n```\n\nQuestion: \n```\n{query}\n```\n\nAnswer: \")"
          )
        )\
        .drop("_context", "_query")
