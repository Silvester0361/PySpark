from pyspark.sql import SparkSession
from pyspark.sql.functions import  col,lit

data =[("Silvester",23,"M"),("Roshan",20,"M"),("Susan",19,"F")]
column =["Name","Age","Gender"]
spark=SparkSession.builder.appName("sample").getOrCreate()
df = spark.createDataFrame(data,column)
df = df.withColumn(colName="Age",col=col("Age").cast("Integer"))
df1 = df.withColumn("Age",col("Age") + 2)
df2 = df.withColumn("Country",lit("India"))
# df.show()
# df1.show()
df2.show()
df2.printSchema()