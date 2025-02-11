

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


data = [("Silvester",23),("Roshan",20),("Susan",19)]
column =["Name","Age"]

spark = SparkSession.builder.appName("Sample").getOrCreate()

df = spark.createDataFrame(data,column)

df.sort(col("Age").asc()).show()
df.sort(col("Age").desc()).show()

