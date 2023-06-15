from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from pubmed_ingest.config.ConfigStore import *
from pubmed_ingest.udfs.UDFs import *

def Script_1(spark: SparkSession, in0: DataFrame):

    def save(file_name, content):

        with open(f"/dbfs/prophecy-samples/med-advisor/pubmed/{file_name}", "wb") as _out:
            _out.write(content)

        return len(content)

    import os
    os.makedirs("/dbfs/prophecy-samples/med-advisor/pubmed/", exist_ok = True)
    save_udf = udf(save, IntegerType())
    in0.select(save_udf(in0.file_name, in0.content).alias('size')).agg(expr('sum(size)')).show()

    return 
