from kafka import KafkaProducer
import logging

# Set the log level for KafkaProducer
kafka_log_level = "ERROR"
logging.getLogger("kafka").setLevel(kafka_log_level)

# Set Kafka broker(s) and topic
bootstrap_servers = 'localhost:9092'
topic = 'log_topic_17_1'

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# log_file_path = '/Users/tanmaysingla/Jupyter Files/42MBSmallServerLog.log' 
log_file_path = '/Users/tanmaysingla/Downloads/17GBBigServerLog.log'

f = open(log_file_path, 'r')
for line in f:
    # Send each line of the log file as a message to the Kafka topic
    # Strip any leading or trailing whitespaces and encode the line as UTF-8
    producer.send(topic, value=line.strip().encode('utf-8'))

# Send a special "end_of_file" message to indicate the completion of log file processing
producer.send(topic, value="end_of_file".encode('utf-8'))

# Close the Kafka producer
producer.close()

# batch_size = 1000  # Number of log lines to batch
# index = 0
# batch = []
# for line in f:
#     batch.append(line.strip().encode('utf-8'))
#     if len(batch) >= batch_size:
#         producer.send(topic, value=b'\n'.join(batch))
#         producer.flush()
#         print(index)
#         index += 1
#         batch = []

# # Send any remaining log lines as the last batch
# if batch:
#     producer.send(topic, value=b'\n'.join(batch))
#     producer.flush()