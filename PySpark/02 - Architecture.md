# PySpark Architecture

- Apache Spark works on a master-slave architecture.
- Operations are executed on workers, and the Cluster Manages resources.

![image](https://github.com/ShubhamJagtap2000/Python-Spark/assets/63872951/d5823743-26f4-4706-a092-b37443ab76c1)

- So when a spark application runs, Spark Driver creates a context that is an entry point for the application and all associated operations are executed on the workers nodes.
- Finally, the cluster manager administrates those resources.
