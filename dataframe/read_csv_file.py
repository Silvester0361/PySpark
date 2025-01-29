from pyspark.sql import  SparkSession
from pyspark. sql.types import *
schema = StructType([StructField("Song Number",IntegerType(),True),
                     StructField("Singer",StringType(),True),
                     StructField("Date",StringType(),True),
                     StructField("Minutes",StringType(),True)])
spark = SparkSession.builder.appName("sample").getOrCreate()
df = spark.read.csv(path="D:\Python\Sample.csv" ,schema=schema, header= True)
df.show()
df.printSchema()

