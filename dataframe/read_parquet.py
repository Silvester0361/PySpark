from pyspark.sql import SparkSession
from pyspark.sql.types import *

schema = StructType([StructField("model",StringType(),True),
                     StructField("mpg",FloatType(),True),
                     StructField("cyl",IntegerType(),True),
                     StructField("disp",FloatType(),True),
                     StructField("hp",IntegerType(),True),
                     StructField("drat",DoubleType(),True),
                     StructField("wt",DoubleType(),True),
                     StructField("qsec",FloatType(),True),
                     StructField("vs",IntegerType(),True),
                     StructField("am",IntegerType(),True),
                     StructField("gear",IntegerType(),True),
                     StructField("carb",IntegerType(),True)])

spark =SparkSession.builder.appName("sample").getOrCreate()
df = spark.read.parquet("D:\pyspark_programs\dataframe\mtcars.parquet")
df.show()
df.printSchema()
print("Total rows ",df.count())
