from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.transpiler import ABIUtil, BigDecimal, ScalaUtil, getContentAsStream, call_spark_fcn, substring_scala
from prophecy.lookups import (
    createLookup,
    createRangeLookup,
    lookup,
    lookup_last,
    lookup_match,
    lookup_count,
    lookup_row,
    lookup_row_reverse,
    lookup_nth
)

def registerUDFs(spark: SparkSession):
    spark.udf.register("download", download)
    ScalaUtil.initializeUDFs(spark)

@udf(returnType = BinaryType())
def download(url: str):
    import requests

    return requests.get(url).content
