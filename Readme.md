Hadoop
==============

	•Scalable to Petabytes or more easily (Volume) 
	•Offering parallel data processing (Velocity) 提供并行数据处理
	•Storing all kinds of data (Variety)

Core of Hadoop is HDFS,MapReduce and YARN.

HDFS
---------------
HDFS is a file system that 
• follows master-slave architecture
• allows us to store data over multiple nodes (machines)
• allows multiple users to access data.
• just like file systems in your PC

HDFS supports
• distributed storage
• distributed computation
• horizontal scalability




### NameNode


### DataNode


### Block
Each block has 3 replications by default, replications will be stored on different DataNodes
Each block is replicated 3 times and stored on different DataNodes

### Read and Write in HDFS



MapReduce
-----------------
MapReduce is a programming framework that 
* allows us to perform distributed and parallel 
processing on large data sets in a distributed 
environment
* no need to bother about the issues like reliability, 
fault tolerance etc
* offers the flexibility to write code logic without 
caring about the design issues of the system

MapReduce consists of Map and Reduce

Map
•Reads a block of data
•Produces key-value pairs as intermediate outputs

•Reduce
•Receive key-value pairs from multiple map jobs
• aggregates the intermediate data tuples to the final 
output







YARN
------------------