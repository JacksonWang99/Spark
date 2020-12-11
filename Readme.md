Big Data Management
============================

Hadoop
-----------------

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
* allows us to perform distributed and parallelprocessing on large data sets in a distributed environment
* no need to bother about the issues like reliability, fault tolerance etc
* offers the flexibility to write code logic without caring about the design issues of the system


MapReduce consists of Map and Reduce

Map
* Reads a block of data
* Produces key-value pairs as intermediate outputs

Reduce
* Receive key-value pairs from multiple map jobs
* aggregates the intermediate data tuples to the final output


## Data Structure
* Key-value pairs: Keys and values can be: integers, float, strings, raw bytes

## Shuffle
Shuffling is the process of data redistribution 洗牌是数据重新分配的过程
* To make sure each reducer obtains all values associated with the same key.
* It is needed for all of the operations which require grouping
* Hash shuffle，Sort shuffle

combineByKey， reduceByKey，groupByKey
Merge
	* Union
	* Zip
	* Join



YARN
------------------


Spark
==================

## What is Spark
* Apache Spark is an open-source cluster computing framework for real-time processing. 
* Spark provides an interface for programming entire clusters with 
	* implicit data parallelism 
	* fault-tolerance
* Built on top of Hadoop MapReduce, extends the MapReduce model to efficiently use more types of computations


## Spark Architecture
* Master Node:   takes care of the job execution within the cluster
* Cluster Manager(集群管理器): allocates resources across applications
* Worker Node: executes the tasks 执行任务


## RDD Resilient Distributed Dataset(弹性分布式数据集)
初始阶段可以理解为RDD就是python中的list

* RDD is where the data stays
* RDD is the fundamental data structure of Apache Spark (Spark 的基本数据结构)

## RDD Operations

* Narrow transformation（involves no data shuffling） map, flatMap, filter, sample
* Wide transformation(involves data shuffling), sortByKey, reduceByKey, groupByKey,join

## High Dimensional Similarity Search


## Locality Sensitive Hashing
* Jaccard similarity, Angular distance, Euclidean distance


## C2LSH
* Counting the Collisions 计算碰撞
 

* Virtual Rehashing


Spark SQL
-----------------------------
Table is one of the most commonly used ways to present data.Table has (relatively) stable data structure
Transform RDDs using SQL
Spark SQL is not about SQL：Aims to Create and Run Spark Programs Faster


# DataFrame
SparkSession是使用Dataset和DataFrameAPI编程Spark的入口点
SparkSession is the entry point to programming Spark with the Dataset and DataFrame API.

## Creating DataFrames
举例
spark = SparkSession.builder.config(conf=conf).getOrCreate()
df = spark.read.format(”json”).load(”example.json")

## DataFrame Operations
	1. registerTempTable()  change table name
	2. show(), cache(), printSchema(), select(), alias()重命名
	3. filter() 过滤， count()计数, orderBy() 排序, groupBy() 分组, join() 
	4. Writing/save DataFrames

Transformation examples: filter,select,drop,intersect,join
Action examples: count,collect ,show ,head ,take
DataFrames use columnar storage(柱状存储),Transpose of row-based storage,Data in each column (with the same type) are packed together







Classification and PySpark MLlib  分类
================================
MLlib is Spark’s scalable machine learning library consisting of common learning algorithms and utilities
• Basic Statistics
• Classification
• Regression               回归分析
• Clustering               聚类，集群
• Recommendation System
• Dimensionality Reduction 降维
• Feature Extraction       特征提取
• Optimization             最优化
Supervised Learning


## Classification

* Process 1: Preprocessing and Feature Engineering 预处理和特征工程
* Process 2: Train a classifier
* Process 3: Evaluate the Classifier

### How to Judge a Model
* k-fold cross-validation

### Naïve Bayes Classifier
* Bayes’ Rule
* Naïve Bayes: Intuition: Simple (“naïve”) classification method based on Bayes rule
* Multinomial Naïve Bayes Classifier

## Pipeline
• In machine learning, it is common to run a sequence of algorithms to process and learn from data
• A Pipeline is specified as a sequence of stages
• each stage is either a Transformer or an Estimator 
• stages are run in order
• the input DataFrame is transformed as it passes through each stage
• Transformer stages
• the transform() method is called on the DataFrame
• Estimator stages
• the fit() method is called to produce a Transformer (which becomes part of the PipelineModel, or fitted Pipeline)
• then Transformer’s transform() method is called on the DataFrame


Step 1: prepare training data for base models
Step 2: learn base classifiers ( naïve bayes and SVM )
Step 3: generate features for meta model
Step 4: Learn Meta Classifier
Step 5: generate meta features for prediction
Step 6: prediction using meta classifier





