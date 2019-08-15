from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType,StringType

spark = SparkSession.builder.appName('Continues Streaming').master("local").getOrCreate()

streamingDataFrame = spark.readStream. \
    format("text") .\
    load("/opt/gen_logs/logs/access.log")
   # load("/home/orienit/Documents/Web_Log/web_log.txt")

streamingDataFrame.printSchema()

print(input_file_name)

def getDate(path):
    return '-'.join(path.split("/")[3:6])

def getHr(path):
    return int(path.split("/")[6])

## Registerning the above UDF's

getDate_udf = udf(getDate, StringType())
getHr_udf = udf(getHr, IntegerType())


### In streaming the syntax is not like pythone we should write like sql syntax"

streamingDataFrame = streamingDataFrame.\
    filter(split(split(streamingDataFrame['value'],' ')[6], "/") [1] == 'department'). \
    withColumn('filePath', input_file_name().cast('string'))

### >>> logmessage = '122.172.200.100 - - [17/Oct/2014:00:04:36 -0400] "GET /tag/hbase-sink/ HTTP/1.1" 200 15997 "https://www.google.co.in/" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.117 Safari/537.36"'
##>>> logmessage.split(" ")
# ['122.172.200.100', '-', '-', '[17/Oct/2014:00:04:36', '-0400]', '"GET', '/tag/hbase-sink/', 'HTTP/1.1"', '200', '15997', '"https://www.google.co.in/"', '"Mozilla/5.0', '(X11;', 'Linux', 'x86_64)', 'AppleWebKit/537.36', '(KHTML,', 'like', 'Gecko)', 'Chrome/33.0.1750.117', 'Safari/537.36"']
# >>> logmessage.split(" ")[6].split("/")[1]
# 'tag'

print (input_file_name) ## to access the actual file name


streamingDataFrame.printSchema()


streamingDataFrame = streamingDataFrame.\
    withColumn('department_name', split(split(streamingDataFrame['value'],' ')[6], "/") [2]) .\
    withColumn('dt', getDate_udf(streamingDataFrame['filePath']).cast('date')) . \
    withColumn('hr', getHr_udf(streamingDataFrame['filePath']))


streamingDataFrame.printSchema()
#
# root
#  |-- value: string (nullable = true)
#  |-- filePath: string (nullable = false)
#  |-- department_name: string (nullable = true)
#  |-- dt: date (nullable = true)
#  |-- hr: integer (nullable = true)

streamingDataFrame = streamingDataFrame.drop('filePath')

streamingDataFrame.printSchema()


dataStreamWriter = streamingDataFrame. \
    writeStream.format("console"). \
    format('console') .\
    outputMode('complete') .\
    trigger(processingTime='2 seconds') ## for continues trigger(continues='1 second')

streamingQuery = dataStreamWriter.start()


#
# dataStreamWriter = streamingDataFrame. \
#     writeStream.format("console"). \
#     option("checkpointLocation", "/home/orienit/Documents/Web_Log/") .\
#     outputMode('append') .\
#     trigger(processingTime='2 seconds'). \
#     partitionBy('dt' ,'hr')

## for continues trigger(continues='1 second')

# streamingQuery = dataStreamWriter.start("/home/orienit/Documents/Web_Log/kk")

# streamingQuery.awaitTermination()

