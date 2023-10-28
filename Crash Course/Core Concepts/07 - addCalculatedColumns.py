#Defining a schema in Spark to define data is correct format 
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

# Our dataset as 'Salary' in String Type with a '$' sign at first index so we remove it and make it a Float Type
from pyspark.sql.functions import *
df = df.withColumn('clean_salary', df.Salary.substr(2, 100).cast('float'))
df.show()

# Calculate 'monthly_salary' for all users
df = df.withColumn('monthly_salary', df.clean_salary/12)
df.show()

# Create a column to show that a user is a Male/Female
df = df.withColumn('are_they_female', when(df.gender == 'Female', 'Yes').otherwise('no'))
df.show()



