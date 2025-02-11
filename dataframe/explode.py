from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode
from pyspark.sql.types import StringType, ArrayType, IntegerType, StructType,StructField

schema = StructType([StructField("Name",StringType()),
                     StructField("Numbers",ArrayType(IntegerType()))])

data =[("Silvester",[1,2,3,4,5]),("Roshan",[6,7,8,9]),("Susan",[10,11,12,13])]

spark = SparkSession.builder.appName("sample").getOrCreate()
df = spark.createDataFrame(data,schema)

df1 =df.withColumn("Numbers",explode(col("Numbers")))
df1.show()
df.printSchema()