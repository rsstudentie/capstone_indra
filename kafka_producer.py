### KAFA PRODUCER SCRIPT ###
# This script reads data from files in a folder, computes averages, and sends the data to a Kafka topic.
# The data is read from the files in the 'data' folder, and the averages are computed for each column.
# The computed averages are then sent to a Kafka topic named 'bearingdata'.
# The script uses the KafkaProducer class from the kafka package to send data to Kafka.
# The KafkaProducer class is initialized with the Kafka broker address and a value serializer that converts the data to JSON format.


from kafka import KafkaProducer
from json import dumps
import os
from datetime import datetime, timedelta

# Kafka producer setup
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

# Path to the folder containing input files
folder_path = 'data/set1'


# Function to read columns from a file
def read_columns(file_path):
    with open(file_path, "r") as file:
        for line in file:
            columns = line.split()  # Split the line into columns
            yield columns


# Function to process file, aggregate data, and produce Kafka streams
def process_file(file_path):
    filename = os.path.basename(file_path)
    timestamp_str = filename.split('.')[:6]  # Split and take the first 6 parts
    timestamp = datetime.strptime('.'.join(timestamp_str), '%Y.%m.%d.%H.%M.%S')

    # Initialize column sums and count
    col_sums = [0] * 8
    row_count = 0

    # Read the file and compute column sums and count the number of rows
    with open(file_path, 'r') as file:
        for line in file:
            values = line.strip().split('\t')
            for i, value in enumerate(values):
                col_sums[i] += float(value) * 1000  # Scale to integer
            row_count += 1

    # Compute the averages
    col_avgs = [sum_value / row_count for sum_value in col_sums]

    # Convert averages to integers
    int_avgs = [int(avg_value) for avg_value in col_avgs]

    # Prepare data to send to Kafka
    data_to_send = {
        'timestamp': timestamp.isoformat(),
        'averages': int_avgs
    }

    # Produce message to Kafka
    producer.send('bearingdata', value=data_to_send)
    print(f"{filename} data sent to Kafka:", data_to_send)

    # Flush Kafka producer buffer
    producer.flush()


# Main script to process files in the folder
if not os.path.exists(folder_path):
    print("The folder does not exist.")
else:
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)

        if os.path.isfile(file_path):
            print("Processing file:", filename)
            process_file(file_path)

# Close Kafka producer
producer.close()
