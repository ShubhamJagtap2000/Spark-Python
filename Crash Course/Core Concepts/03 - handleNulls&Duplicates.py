# Drop rows involving 'null' value/s
df_dropped = df.na.drop()                  
df_dropped.show()

# Keep only those rows where job_title is not null, as job_title is important field
df_null_jobs = df.filter(df.job_title.isNotNull())
df_null_jobs.show()

# If 'city' is 'null' return the new column 'Clean city' as 'Unknown', otherwise give the name of city as it is
from pyspark.sql.functions import *
df_handled = df.withColumn("Clean city", when(df.city.isNull(), 'Unknown').otherwise(df.city))
df_handled.show()

# Dropping duplicates if two rows are identical
df_no_duplicates = df.dropDuplicates()
df_no_duplicates.show()


