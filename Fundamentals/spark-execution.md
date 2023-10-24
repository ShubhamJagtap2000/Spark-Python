# How Spark works in a cluster

- In a cluster we will have 2 different elements
- On one hand there will be the controller or resource manager, on the other hand there will be workers/nodes
- Each worker will have different tasks and associated disks to save data in memory
- When a Spark application is executes, it runs as independent process, coordinated by the SparkSession object in the driver program
- Then the resource manager/Spark master assigns the tasks to the workers, one task per partition
- Resource manager or controller is responsible for assigning the tasks
- A task applies its unit of work to the dataset in its partition and outputs a new partition dataset
- Because iterative algorithms apply operations repeatedly to data, they benefit from caching datasets across iterations
- It means that in each iteration, they can store data and benefit from it
- By caching, you create a checkpoint in your spark application, and if any of the tasks fail, your application will be able to recompute the lost partition from the cache
- Finally, the resultsare sent back to the driver application or can be saved to disk
- When we send data back to the controller, it joins each worker's reesults, creating complete solution for all initial work
- In this way, Spark distributes data and nodes process them
