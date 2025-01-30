from pyspark.sql import  SparkSession
spark = SparkSession.builder.appName("sample").getOrCreate()
df = spark.read.csv(path="D:\Python\Sample.csv", header= True)
df.show(truncate=3)

spark = SparkSession.builder.appName("sample").getOrCreate()
df = spark.read.csv(path="D:\Python\Sample.csv", header= True)
df.show(vertical= True)

spark = SparkSession.builder.appName("sample").getOrCreate()
df = spark.read.csv(path="D:\Python\Sample.csv", header= True)
df.show(n= 25)




