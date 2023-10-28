# Defining a schema in Spark to define data is correct format 
from pyspark.sql.types import *

myschema = StructType([
  StructField('id', IntegerType()),
  StructField('first_name', StringType()),
  StructField('last_name', StringType()),
  StructField('gender', StringType()),
  StructField('city', StringType()),
  StructField('job_title', StringType()),
  StructField('Salary', StringType()),
  StructField('latitude', FloatType()),
  StructField('longitude', FloatType())
])

df = spark.read.csv("original.csv", header = True, schema = myschema)
df.show()

# Cleanup the data, remove the '$' from Salary column 
from pyspark.sql.functions import *
df = df.withColumn('clean_salary', df.Salary.substr(2, 100).cast('float'))
df.show()

# Group by gender, and find what the total salary is for each gender
from pyspark.sql.functions as sqlfunc
df1 = df.groupBy('gender').agg(sqlfunc.sum('clean_salary'))
df1.show()

# Add other aggregations
df1 = df.groupBy('gender').agg(sqlfunc.avg('clean_salary').alias('average'),
                               sqlfunc.min('clean_salary').alias('min'),
                               sqlfunc.max('clean_salary').alias('max'))
df1.show()


# Add other group by
df1 = df.groupBy('gender', 'city').agg(sqlfunc.sum('clean_salary').alias('total'),
                                       sqlfunc.avg('clean_salary').alias('average'),
                                       sqlfunc.min('clean_salary').alias('min'),
                                       sqlfunc.max('clean_salary').alias('max'))
df1.show()
