from pyspark.sql.types import  StructType,StructField,StringType,IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col

data = [("Silvester","Python,Pyspark"),("Roshan","Java,C++"),("Susan","JavaScript,C")]

schema = StructType([StructField("Name",StringType()),
                     StructField("Languages",StringType())])

spark = SparkSession.builder.appName("sample").getOrCreate()
df = spark.createDataFrame(data, schema)
df.withColumn("Languages",split(col("Languages")," ")).show()
