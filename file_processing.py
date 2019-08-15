# from pyspark.sql import SparkSession
#
# spark = SparkSession \
#     .builder \
#     .appName("File_Processing") \
#     .master("local[*]") \
#     .getOrCreate()
#
#
# df = spark.read.csv("/home/orienit/work/spark_inputs/OP_DTL_OWNRSHP_PGYR2013_P06292018.csv", inferSchema=True)
#
# df.show()

from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import