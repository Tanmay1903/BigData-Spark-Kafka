from kafka import KafkaProducer
from pyspark.sql import SparkSession
import time
import logging
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.functions import regexp_extract
from pyspark.conf import SparkConf
import threading

start_time = time.time()
# Set the log level for Spark
spark_log_level = "WARN"

conf = SparkConf() \
    .setMaster("local[*]") \
    .setAppName("LogAnalytics") \
    .setExecutorEnv("spark.executor.memory", "4g") \
    .setExecutorEnv("spark.driver.memory", "4g")
    
# Initialize Spark session
spark = SparkSession.builder.config(conf = conf).getOrCreate()
spark.sparkContext.setLogLevel(spark_log_level)

# Set the log level for KafkaProducer
kafka_log_level = "ERROR"
logging.getLogger("kafka").setLevel(kafka_log_level)

# Set Kafka broker(s) and topic
bootstrap_servers = 'localhost:9092'
topic = 'log_topic_17_1'
group_id = 'consumer_group'

host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})\]'
method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
status_pattern = r'\s(\d{3})\s'
content_size_pattern = r'\s(\d+)$'

month_map = {
  'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
  'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12
}

def parse_clf_time(text):
    """ Convert Common Log time format into a Python datetime object
    Args:
        text (str): date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
    Returns:
        a string suitable for passing to CAST('timestamp')
    """
    # NOTE: We're ignoring the time zones here, might need to be handled depending on the problem you are solving
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
      int(text[7:11]),
      month_map[text[3:6]],
      int(text[0:2]),
      int(text[12:14]),
      int(text[15:17]),
      int(text[18:20])
    )
    
week_map = {
    1: "Sunday",
    2: "Monday",
    3: "Tuesday",
    4: "Wednesday",
    5: "Thursday",
    6: "Friday",
    7: "Saturday"
}

def parse_day_of_week(dayOfWeek):
    return week_map[dayOfWeek]

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def producer_job():
    # Read log file
    log_file_path = '/Users/tanmaysingla/Downloads/17GBBigServerLog.log' # '/Users/tanmaysingla/Jupyter Files/42MBSmallServerLog.log' # '/Users/tanmaysingla/Downloads/17GBBigServerLog.log'
    f = open(log_file_path, 'r')
    count = 0
    for line in f:
        producer.send(topic, value=line.strip().encode('utf-8'))
        
    producer.send(topic, value="end_of_file".encode('utf-8'))
    producer.close()


#while True:
# Create Kafka consumer DataFrame
def consumer_job():
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("kafkaConsumer.groupId", group_id) \
        .option("startingOffsets", "earliest") \
        .load()

    # Process received DataFrame
    def process_dataframe(df, batch_identifier):
        if not df.isEmpty():
            should_not_continue = False
            # Process the DataFrame using Spark
            df = df.withColumn("value", df["value"].cast("string"))
            if df.filter(df["value"] == "end_of_file").count() > 0:
                should_not_continue = True
                df = df.filter(~(df["value"] == "end_of_file"))
                
            logs_df = df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                            regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                            regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                            regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                            regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                            regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                            regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
            
            
            udf_parse_time = udf(parse_clf_time)

            logs_df_with_time = (logs_df.select('*', udf_parse_time(logs_df['timestamp']).cast('timestamp').alias('time')).drop('timestamp'))
            # logs_df_with_time.show(10, truncate=True)
            
            logs_df_with_time.cache()
            
            udf_parse_day = udf(parse_day_of_week)
            enpoint_day_of_week_df = logs_df_with_time.select(logs_df_with_time.endpoint, udf_parse_day(F.dayofweek('time')).alias("dayOfWeek"))
            
            highest_invocations_df = (enpoint_day_of_week_df
                            .groupBy("endpoint", "dayOfWeek")
                            .count()
                            .sort("count", ascending=False)
                            .select(enpoint_day_of_week_df.dayOfWeek.alias("Day in a Week"), enpoint_day_of_week_df.endpoint, "count"))
            highest_invocations_df.show(1, truncate=False)
            # Write DataFrame to Parquet format in HDFS
            # highest_invocations_df.write.parquet("/Users/tanmaysingla/Desktop/BigData_Assn3/parquet_17GB/highest_invocations")
            
            not_found_df = logs_df_with_time.filter(logs_df["status"] == 404)
            yearly_404_sorted_df = (not_found_df
                            .select(F.year("time").alias("Year"))
                            .groupBy("Year")
                            .count()
                            .sort("count", ascending=True).limit(10))
            yearly_404_sorted_df.show(10, truncate=False)
            # yearly_404_sorted_df.write.parquet("/Users/tanmaysingla/Desktop/BigData_Assn3/parquet_17GB/yearly_404")
            # Here, we are simply printing the contents of the DataFrame for demonstration purposes
            if should_not_continue:
                end_time = time.time() - start_time
                print("Time taken: ", end_time)
                query.stop()
            

    # Apply processing function to the streaming DataFrame
    query = df.writeStream.foreachBatch(process_dataframe).trigger(processingTime='10 seconds').start()

    # Wait for the termination of the query or 10 seconds
    query.awaitTermination()

# Create producer and consumer threads
producer_thread = threading.Thread(target=producer_job)
consumer_thread = threading.Thread(target=consumer_job)

# Start the threads
producer_thread.start()
consumer_thread.start()

# Wait for both threads to finish
producer_thread.join()
consumer_thread.join()
    
# Sleep for 10 seconds before checking for available data again
#time.sleep(10)
