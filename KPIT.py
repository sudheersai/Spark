from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as fn
from pyspark.sql import Row

spark = SparkSession \
    .builder \
    .appName('testing') \
    .master("local[*]") \
    .getOrCreate()


# row = Row(name="Alice", age=11)
# row.show()

# df1 = spark.createDataFrame([Row])


df2 = spark.createDataFrame([Row(id=1, value=float('NaN')),
     Row(id=2, value=42.0),
     Row(id=3, value=None)
 ]).show()
