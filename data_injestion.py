### DATA INJESTION ###
# This module is responsible for reading the data files, processing them, and saving the data to the database.
# The process_file function reads the data from a file, computes the averages for each column, and aggregates the data.
# The aggregated data is then saved to the database using the save_to_db function.


import os
from datetime import datetime, timedelta
from common.database_insert import save_to_db


def process_file(file_path, aggregation_data):
    """
    Process a file to extract timestamp and channel data,
    aggregate it, and save it to the database.
    """
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

    # Check if this is the first file
    if aggregation_data['start_time'] is None:
        aggregation_data['start_time'] = timestamp

    # Add current file's data to the aggregation
    aggregation_data['count'] += 1
    for i in range(8):
        aggregation_data['sums'][i] += int_avgs[i]

    # Calculate the time difference
    time_diff = timestamp - aggregation_data['start_time']
    if time_diff >= timedelta(minutes=10):
        # Calculate the final averages
        final_avgs = [sum_value // aggregation_data['count'] for sum_value in aggregation_data['sums']]
        print(f"Time difference before insert: {time_diff}")
        save_to_db(aggregation_data['start_time'], final_avgs)

        # Reset aggregation data
        aggregation_data['start_time'] = None
        aggregation_data['sums'] = [0] * 8
        aggregation_data['count'] = 0

        # Start new aggregation with the current file
        aggregation_data['start_time'] = timestamp
        aggregation_data['count'] = 1
        aggregation_data['sums'] = int_avgs
