from pyspark.sql import SparkSession
from pyspark.sql.functions import array,col

data = [("Silvester","Python","Pyspark"),("Roshan","Java","C++"),("Susan","JavaScript","C")]
column = ["Name","Language1","Language2"]

spark = SparkSession.builder.appName("sample").getOrCreate()
df = spark.createDataFrame(data,column)

df.withColumn("Languages",array(col("Language1"),col("Language2"))).show()