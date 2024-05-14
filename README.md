# Distributed Data Processing using Apache Spark

1. [About the Course](#about-the-course)
2. [Presnetation slides](./presentation_slides/presentation_slides.md)
3. [Confuguration of Databricks Compute Cluster](./databricks_installation/databricks_compute_cluster_installation.md)
4. [Scala Notebooks](./scala_notebooks/scala_notebooks.md)
5. [Spark Code](./SparkD05/)
6. [Resources](./Resources/resources.md) 
7. [Assignemtns](./assignments/assignments.md)
<br>

## About the Course

Distributed processing architecture, such as Apache Spark, is a software infrastructures designed to process large amounts of data across a cluster of interconnected machines. They enable the distribution of computational tasks across multiple processing nodes in parallel, which provides `greater processing` capacity, better `scalability`, and enhanced `performance`.

Spark is one of the most popular and powerful distributed processing architectures. Here are some key features of Spark:

### Programming Model

Spark offers an abstract programming model called `Resilient Distributed Datasets (RDD)`, which allows data to be processed transparently across the cluster. RDDs are `immutable` and `fault-tolerant` collections, meaning they can be distributed across multiple computing nodes and retrieved in case of failure.

### In-Memory Processing

* Spark uses `RAM` to store intermediate data and computation results, which allows for rapid access to data without having to read from disk. This enables faster response times and more efficient task execution.

### Support for Various Data Sources

* Spark provides connectors to process different types of data from various sources, such as local files, distributed file systems, relational databases, streaming data, etc. It also allows integration with other tools and frameworks like Hadoop, Hive, and Cassandra.

### Batch and Real-Time Processing

* Spark supports both `batch data processing` and `real-time (streaming) processing`. It enables continuous analysis on real-time data streams, as well as iterative processing for machine learning algorithms and graph processing.

### Extensive Ecosystem

* Spark has a rich ecosystem with a comprehensive library of components, including `Spark SQL` for SQL processing, `Spark Streaming` for real-time processing, `MLlib` for machine learning, `GraphX` for graph processing, and many more. This facilitates the development of complex applications using a coherent set of tools.

<br>

Distributed processing architectures like Spark are used for various applications, such as big data analysis, real-time stream processing, distributed machine learning, personalized recommendation, predictive analytics, etc. They leverage the parallel computing power of distributed clusters for fast and efficient processing of large volumes of data.

<br>



   
