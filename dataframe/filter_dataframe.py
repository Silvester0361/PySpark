from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("sample").getOrCreate()

data1=[(12,"silvester"),
      (13,"leo das")]
schema1=["age","name"]
df=spark.createDataFrame(data1,schema1)
df.filter(col("age")==12).show()