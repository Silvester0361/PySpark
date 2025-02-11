from pyspark.sql import SparkSession
from pyspark.sql.functions import*

data = [("Silvester",23),("Roshan",20),("Susan",19)]
column =["Name","Age"]

spark =SparkSession.builder.appName("sample").getOrCreate()
df = spark.createDataFrame(data,column)
df1=df.count()
df1.show()