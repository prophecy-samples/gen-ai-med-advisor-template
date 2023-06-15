from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_answer.config.ConfigStore import *
from pubmed_answer.udfs.UDFs import *

def Reformat_1(spark: SparkSession, OpenAI_1: DataFrame) -> DataFrame:
    return OpenAI_1.select(col("question"), col("openai_answer.choices")[0].alias("answer"))
