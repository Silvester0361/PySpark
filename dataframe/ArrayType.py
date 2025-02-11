from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType
from pyspark.sql.functions import col


df = [("Silvester",[1,3,4,5]),("Roshan",[2,6,7,8])]
column = StructType([StructField("Name",StringType()),
                     StructField("Numbers",ArrayType(IntegerType()))])

spark = SparkSession.builder.appName("sample").getOrCreate()
df = spark.createDataFrame(df,column)
df.withColumn("Numbers",col("Numbers")[1]).show()

df.show()
df.printSchema()