### KAFA PRODUCER SCRIPT ###
# This script subscribes to a Kafka topic "bearingdata", reads messages, and saves the data to a PostgreSQL database.
# The script uses the KafkaConsumer class from the kafka package to read messages from Kafka.
# The KafkaConsumer class is initialized with the Kafka broker address, the topic name, and a value deserializer that converts the data from JSON format.


from kafka import KafkaConsumer
import json
from common.database_insert import save_to_db

# Kafka consumer configuration
consumer = KafkaConsumer('bearingdata',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='my-group',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))


# Main loop to consume messages from Kafka
for message in consumer:
    data = message.value

    # Extract timestamp and averages from received data
    timestamp = data['timestamp']
    int_avgs = data['averages']

    # Save data to database
    save_to_db(timestamp, int_avgs)
    print("Data saved to database:", data)

    # Optionally, write to file for logging purposes

# Close the Kafka consumer after finishing reading (not necessary in an infinite loop)
consumer.close()
