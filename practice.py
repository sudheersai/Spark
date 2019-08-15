from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import *

Spark = SparkSession \
    .builder \
    .appName ("next entni") \
    .master("local[*]") \
    .getOrCreate()

schema = StructType([StructField("order_id", IntegerType(), True),
                    StructField("order_date", StringType(), True),
                    StructField("order_customer_id", IntegerType(), True),
                    StructField("order_status", StringType(), True)])

df = Spark.read.format('csv').load("/home/orienit/Desktop/Itversity/Input/orders.txt", schema=schema)
df.show()

df.filter(df.order_status != 'CLOSED').select(df.order_id, df.order_customer_id).show()

df= df.withColumn('new_status', fn.regexp_replace(df.order_status, 'PENDING_', ' '))

df= df.withColumn('CNT', fn.length(df.new_status))
df.show()
df.printSchema()

#df=df.withColumn('CNT', df.CNT.cast(StringType()))

df.printSchema()

df.show()

df.groupBy('order_status').count().show()


##df.where(df.order_customer_id > 100) | (df.order_customer_id <= 250).show()

df_output= df.where('order_customer_id >=100 and order_customer_id <= 250')

df.write.mode('overwrite').format('csv').save('/home/orienit/Desktop/Itversity/Input/orders.txt1')

df.select(fn.split(df.order_date,' ')[0]).alias('test').show()

df=df.withColumn('order_date', fn.split(df.order_date,' ')[0]).alias('date')

df=df.withColumn('year', fn.split(df.order_date,'-')[0])
df=df.withColumn('month', fn.split(df.order_date,'-')[1])
df=df.withColumn('date', fn.split(df.order_date,'-')[2])

df= df.withColumn('new_status', fn.regexp_replace(df.order_status,  ('PAYMENT_','SS'), ' '))
#df=df.withColumn('new_status', fn.split(df.order_status,"_")[1])

df.show()



