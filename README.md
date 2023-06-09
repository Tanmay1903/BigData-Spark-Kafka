# BigData-Spark-Kafka
- To implement spark code to determine the top 100 most Frequently occurring words and top 100 Most Frequently occurring words having more than 6 characters in a given dataset of 16GB.
- Gain proficiency in log analytics and implement the log analytics techniques described in the article.
- Implement a data processing and analysis pipeline using Big Data technologies, including Kafka (producer and consumer), Spark Streaming, Parquet files, and HDFS File System. The objective is to process and analyze log files by establishing a flow that begins with a Kafka producer, followed by a Kafka consumer that performs transformations using Spark. 

## Problem Statement
- Problem 1: Develop Spark code to determine the top 100 most frequently occurring words and the top 100 most frequently occurring words with more than 6 characters in a given dataset of 16GB.

- Problem 2: Gain proficiency in log analytics. 
  - Create a summary report that identifies the endpoint with the highest number of invocations on a specific day of the week, along with the corresponding count of invocations, for the entire dataset.
  - Find the top 10 years with the least occurrences of 404 status codes in the dataset and provide the corresponding count of 404 status codes for each year.

- Problem 3: Implement a data processing and analysis pipeline using Big Data technologies, including Kafka (producer and consumer), Spark Streaming, Parquet files, and HDFS File System. The pipeline should process and analyze log files, starting with a Kafka producer, followed by a Kafka consumer performing transformations using Spark. The transformed data should be converted into Parquet file format and stored in the HDFS. 

- Bonus Problem: The bonus task requires the development of a clustering algorithm capable of grouping requests based on several factors, including the host that invoked it, the time at which the endpoint was accessed, the status code received, and the data size of the returned information.

## System Configurations
- Apple Macbook Pro
  -	Processor: Apple M1 chip.
  -	Cores: 10 number of cores.
  -	Memory: 8 GB RAM available on the system.
  -	Storage Type: SSd storage with 256 GB storage space
  -	Storage size available: Available disk space.
  -	Operating System: macOS Ventura
  -	Programming Language: Python3
- Samsung Galaxy Notebook 7
  - Processor: Intel i7 11th Gen.
  - Cores: 10 number of cores.
  - Memory: 16 GB RAM available on the system.
  - Storage Type: SSd storage with 512 GB storage space
  - Storage size available: Available disk space.
  - Operating System: Windows 11
  - Programming Language: Python3

## Files in the Project
- Big_data_Assn_3_RDD.ipynb: This file contains the script that determines K Most Frequent Words using Spark RDD.
- Big_data_Assn_3_DataFrame.ipynb: This file contains the script that determines K Most Frequent Words using Spark DataFrame.
- LogAnalytics-Article.ipynb: This file contains the log analysis as per the article mentioned in the assignment.
- LogAnalytics-SectionA.ipynb: This file contains the log analysis on the NASA dataset to get the desired results.
- BigDataAssn3-Kafka.py: This file contains the implemented code for running Producer and Consumer Sequentially.
- BigDataAssn3-Kafka-thread.py: This file contains the implemented code for running Producer and Consumer Parallely in Threads.
- producer.py: In this file, Kafka Producer is implemented as a seperate application.
- consumer.py: In this file, Structured Streaming is implemented as a seperate application to consume data from Kafka topic.
- consumer_with_clustering.py: In this file, Structured Streaming is implemented as a seperate application to consume data from Kafka topic and K-means clustering algorithm is implemented using Spark's MLlib library.
- parquet_17GB - This folder contains the output of the analysis in parquet file format on 17GB file(100 Batches only).
- parquet_42MB - This folder contains the output of the analysis in parquet file format on 42MB file.
 
## How to run the code
- .ipynb files can be executed directly using Jupyter notebook.
- To run producer.py:- 
  - ```python producer.py ```  
- To run consumer.py:-
  - ```spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 {PATH_TO_YOUR_FILE}```
  - Example in my case:
    ```spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 Desktop/BigData_Assn3/consumer.py```
- To run consumer_with_clustering.py:-
  - ```spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 {PATH_TO_YOUR_FILE}```
  - Example in my case:
    ```spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 Desktop/BigData_Assn3/consumer_with_clustering.py```
    
## How to Install Spark on Windows 
- Check Prerequisites.
  - Check for Java Installation
  - check for Python Installation 
- Install Apache Spark from "https://spark.apache.org/downloads.html"
- Extract Spark in C:\ folder
- Set Environment Variables: 
  User Variable
  - HADOOP_HOME : C:\hadoop
  - SPARK_HOME: C:\spark\spark-3.4.0-bin-hadoop3
  - JAVA_HOME: C:\java\jdk1.8.0_202
  - PYSPARK_PYTHON: C:\Users\"XYZ"\AppData\Local\Programs\Python\Python311\python.exe
  System Variable 
  - %HADOOP_HOME%\bin
  - %SPARK_HOME%\bin
  - %JAVA_HOME%\bin
  - C:\Users\"XYZ"\AppData\Local\Programs\Python\Python311\Lib\site-packages
 - For Spark to run on windows we need winutils 
  - Download the winutils.exe from https://github.com/steveloughran/winutils 
    based on your spark version
  - extract that into c:\hadoop\bin
 - run "spark-shell" on command Prompt
  
