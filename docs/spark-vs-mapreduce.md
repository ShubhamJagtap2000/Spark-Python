# Spark vs. MapReduce

- MapReduce needs files to be stored on HDFS, Spark does not

- Spark performs operations 100 times faster than MapReduce

- MapReduce writes most data into disk(HDD) after each map and reduce operation

- Spark keeps most data in memory after each transformation

- Spark can spill over to disk if memory(RAM) is filled
