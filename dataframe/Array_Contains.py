from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains,col

data=[("Silvester",23,["Python","Pyspark"]),("Roshan",20,["Java","C++"]),("Susan",19,["JavaScript","C"])]
column=["Name","Age","Languages"]

spark =SparkSession.builder.appName("sample").getOrCreate()
df = spark.createDataFrame(data,column)
df.withColumn("Language",array_contains(col("Languages"),"C++")).show()