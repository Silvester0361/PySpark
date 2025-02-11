from pyspark.sql import SparkSession

from pyspark.sql.functions import *

data = [("Silvester",23),("Roshan",20),("Susan",19)]
column =["Name","Age"]
spark = SparkSession.builder.appName("sample").getOrCreate()
df = spark.createDataFrame(data,column)

df.filter(col("Age")>=20).select(col("Age")).show()
df.where(col("Name").like("%r")).select(col("Age")).show()