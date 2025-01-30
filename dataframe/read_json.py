from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import VarcharType, StringType

schema = StructType([StructField("address", StringType(), True),
                     StructField("email",StringType(),True),
                     StructField("name",StringType(),True),
                     StructField("phone",IntegerType(),True),
                     StructField("website",StringType(),True)])
spark = SparkSession.builder.appName("sample").getOrCreate()
df = spark.read.json(path="D:\pyspark_programs\dataframe\sample.json",multiLine=True,schema=schema)
df.show()
df.printSchema()