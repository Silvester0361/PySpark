from pyspark.sql import SparkSession
from pyspark.sql.functions import col

df = [("Silvester",23),("Roshan Mathew",20),("Susan Mary",19)]
column =["Name","Age"]
spark = SparkSession.builder.appName("sample").getOrCreate()
df = spark.createDataFrame(df,column)

df =df.withColumnRenamed("Name","Full_Name")

df.show()
