from pyspark.sql import SparkSession
import logging
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.functions import regexp_extract
from pyspark.conf import SparkConf

# Set the log level for Spark
spark_log_level = "WARN"

conf = SparkConf() \
    .setMaster("local[*]") \
    .setAppName("LogProcessing")

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

# Regular expression patterns for log parsing
host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})\]'
method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
status_pattern = r'\s(\d{3})\s'
content_size_pattern = r'\s(\d+)$'

# Month and day mapping for log parsing
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

# Read data from Kafka as a streaming DataFrame
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", topic) \
    .option("kafkaConsumer.groupId", group_id) \
    .option("startingOffsets", "earliest") \
    .load()

# Process received DataFrame in each batch
def process_dataframe(df, batch_identifier):
    if not df.isEmpty():
        should_not_continue = False

        # Convert the value column to string type
        df = df.withColumn("value", df["value"].cast("string"))

        # Check if the "end_of_file" message exists in the DataFrame
        if df.filter(df["value"] == "end_of_file").count() > 0:
            should_not_continue = True
            # Remove the "end_of_file" message from the DataFrame
            df = df.filter(~(df["value"] == "end_of_file"))
            
        # Extract relevant fields from the log lines using regular expressions
        logs_df = df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                        regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                        regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                        regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                        regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                        regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                        regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
        
        # Define a user-defined function (UDF) to parse the timestamp into a timestamp data type
        udf_parse_time = udf(parse_clf_time)

        # Apply the UDF to convert the timestamp column into a timestamp data type and rename the column
        logs_df_with_time = (logs_df.select('*', udf_parse_time(logs_df['timestamp']).cast('timestamp').alias('time')).drop('timestamp'))
        
        # Define a UDF to parse the day of the week from the timestamp
        udf_parse_day = udf(parse_day_of_week)
        enpoint_day_of_week_df = logs_df_with_time.select(logs_df_with_time.endpoint, udf_parse_day(F.dayofweek('time')).alias("dayOfWeek"))
        
        # Apply the UDF to extract the endpoint and day of the week columns
        highest_invocations_df = (enpoint_day_of_week_df
                        .groupBy("endpoint", "dayOfWeek")
                        .count()
                        .sort("count", ascending=False)
                        .select(enpoint_day_of_week_df.dayOfWeek.alias("Day in a Week"), enpoint_day_of_week_df.endpoint, "count"))
        highest_invocations_df.show(1, truncate=False)
        
        not_found_df = logs_df_with_time.filter(logs_df["status"] == 404)
        yearly_404_sorted_df = (not_found_df
                        .select(F.year("time").alias("Year"))
                        .groupBy("Year")
                        .count()
                        .sort("count", ascending=True).limit(10))
        yearly_404_sorted_df.show(10, truncate=False)

        # Write DataFrame to Parquet format in Local
        # highest_invocations_df.write.parquet("/Users/tanmaysingla/Desktop/BigData_Assn3/parquet_17GB/highest_invocations/" + str(batch_identifier))
        # yearly_404_sorted_df.write.parquet("/Users/tanmaysingla/Desktop/BigData_Assn3/parquet_17GB/yearly_404/" + str(batch_identifier))

        # Write DataFrame to Parquet format in HDFS
        highest_invocations_df.write.parquet("hdfs://localhost:9000/data/parquet_17GB/highest_invocations/" + str(batch_identifier))
        yearly_404_sorted_df.write.parquet("hdfs://localhost:9000/data/parquet_17GB/yearly_404/" + str(batch_identifier))

        if should_not_continue:
            query.stop()


# Apply processing function to the streaming DataFrame
query = df.writeStream.foreachBatch(process_dataframe).trigger(processingTime='10 seconds').start()

# Wait for the termination of the query or 10 seconds
query.awaitTermination()