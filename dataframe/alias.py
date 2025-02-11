from pyspark.sql import SparkSession

data = [("Silvester",23),("Roshan Mathew",20),("Susan Mary",19)]
column =["Name","Age"]

spark = SparkSession.builder.appName("sample").getOrCreate()

df =spark.createDataFrame(data,column)

df.select(df.Name.alias("Full_name")).show()