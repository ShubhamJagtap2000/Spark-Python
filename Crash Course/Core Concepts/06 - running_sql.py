# Defining a schema in Spark to define data is correct format 
from pyspark.sql.types import *

myschema = StructType([
  StructField('id', IntegerType()),
  StructField('first_name', StringType()),
  StructField('last_name', StringType()),
  StructField('gender', StringType()),
  StructField('city', StringType()),
  StructField('job_title', StringType()),
  StructField('Salary', IntegerType()),
  StructField('latitude', FloatType()),
  StructField('longitude', FloatType())
])

df = spark.read.csv("original.csv", header = True, schema = myschema)
df.show()

# Create a temporary table form a dataframe
df.registerTempTable("original")

# Run a query on the created temp table
query1 = spark.sql('select * from original')
query1.show()

# Filter data with SQL
query2 = spark.sql('select concat(firsT_name, " ", last_name) as full_name from original where gender = "Female"')
query2.sql()
