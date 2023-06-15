from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_ingest.config.ConfigStore import *
from pubmed_ingest.udfs.UDFs import *

def pubmed_bronze_baseline_text(spark: SparkSession, in0: DataFrame):
    in0.write.format("text").text("dbfs:/prophecy-samples/med-advisor/pubmed_links", compression = None, lineSep = None)
