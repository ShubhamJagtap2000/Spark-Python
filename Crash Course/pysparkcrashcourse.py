# -*- coding: utf-8 -*-
"""PySparkCrashCourse.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/github/ShubhamJagtap2000/Spark-Python/blob/main/PySparkCrashCourse.ipynb
"""

!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
!tar xf spark-3.1.1-bin-hadoop3.2.tgz
!pip install -q findspark
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.1-bin-hadoop3.2"
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark

mydata = spark.read.format("csv").option("header", "true").load("original.csv")

mydata.show()

"""# New Section"""

from pyspark.sql.functions import *

mydata2 = mydata.withColumn("clean_city", when(mydata.City.isNull(), "Unknown").otherwise(mydata.City))

mydata2.show()

#Overwrite
mydata2 = mydata2.filter(mydata2.JobTitle.isNotNull())

mydata2.show()

mydata2 = mydata2.withColumn("clean_salary", mydata2.Salary.substr(2, 100).cast('float'))

mydata2.show()

mean = mydata2.groupby().avg('clean_salary')

mean.show()

mean = mydata2.groupBy().avg('clean_salary').take(1)[0][0]

print(mean)

from pyspark.sql.functions import lit
mydata2 = mydata2.withColumn('new_salary', when(mydata2.clean_salary.isNull(), lit(mean)).otherwise(mydata2.clean_salary))

mydata2.show()

#Median values for latitude and longitude
import numpy as np

latitude = mydata2.select('Latitude')

latitude.show()

#filter null values
latitude = latitude.filter(latitude.Latitude.isNotNull())

latitude.show()

#dataframe.field('...').select('new_field')
latitude = latitude.withColumn("latitude2", latitude.Latitude.cast('float')).select('latitude2')

latitude.show()

median = np.median(latitude.collect())

print(median)

mydata2 = mydata2.withColumn("lat", when(mydata2.Latitude.isNull(), lit(median)).otherwise(mydata2.Latitude))

#filtered all null values
mydata2.show()

mydata2.show()

import pyspark.sql.functions as sqlfunc
genders = mydata2.groupBy('gender').agg(sqlfunc.avg('new_salary').alias('AvgSalary'))

genders.show()

#Print male and female salary in particular column
df = mydata2.withColumn('female_salary', when(mydata2.gender == 'Female', mydata2.new_salary). otherwise(lit(0)))

df.show()

#Overwrite 'df' for Male salary
df = df.withColumn('male_salary', when(df.gender == 'Male', df.new_salary).otherwise(lit(0)))

df.show()

df = df.groupBy('JobTitle').agg(sqlfunc.avg('female_salary').alias('final_female_salary'), sqlfunc.avg('male_salary').alias('final_male_salary'))

df.show()

df = df.withColumn('delta', df.final_female_salary - df.final_male_salary)

df.show()

cityavg = mydata2.groupBy('city').agg(sqlfunc.avg('new_salary').alias('avgsalary'))

#Sort the dataframe
cityavg = cityavg.sort(col('avgsalary').desc())

cityavg.show()



