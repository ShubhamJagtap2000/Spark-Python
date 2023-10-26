#Defining a schema in Spark to define data is correct format 
from pyspark.sql.types import *

myschema = StructType([
  StructField('id', IntegerType()),
  StructField('first_name', StringType()),
  StructField('last_name', StringType()),
  StructField('gender', StringType()),
  StructField('city', StringType()),
  StructField('job_title', StringType()),
  StructField('Salary', IntegerType()),
  StructField('latitude', floatType()),
  StructField('longitude', floatType())
])

df = spark.read.csv("original.csv", header = True, schema = myschema)
df.show()

#Validate if the schema has been implemented correctly
df.dtypes
