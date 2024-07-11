### CALCULATE CONTROL LIMITS ###
# This script calculates the control limits for each bearings and channel in the dataset
# The control limits are calculated as the mean plus or minus three times the standard deviation
# The control limits are used to detect outliers in the data
# The script reads the data from the database, calculates the control limits, and saves them to the database
# The control limits are saved in a table named 'control_limits' in the database

import pandas as pd
import psycopg2
import json

from common.load_data import load_data


# Calculate UCL and LCL for each parameter
def calculate_control_limits(data, column):
    mean = data[column].mean()
    std_dev = data[column].std()
    ucl = mean + 3 * std_dev
    lcl = mean - 3 * std_dev



def save_control_limits(control_limits):
    """
    Save control limits to the database.
    """
    # Database connection details. Load these from environment variables or a env.json
    configdetails = json.loads(open('env.json').read())
    DB_HOST = configdetails['DB_HOST']
    DB_NAME = configdetails['DB_NAME']
    DB_USER = configdetails['DB_USER']
    DB_PASSWORD = configdetails['DB_PASSWORD']
    DB_PORT = configdetails['DB_PORT']

    # Connect to the database
    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT)
    cur = conn.cursor()

    # Prepare the SQL insert statement
    sql = """
            INSERT INTO control_limits (bearing_name , mean, ucl, lcl) 
            VALUES (%s, %s, %s, %s)
        """

    # Save control limits to the database
    for key, value in control_limits.items():
        bearing, channel, parameter = key
        ucl, lcl = value
        cur.execute(sql, (bearing, channel, parameter, ucl, lcl))

    # Commit the transaction and close the connection
    conn.commit()
    cur.close()
    conn.close()

def main():
    # Load data from database
    data = load_data()

    # Calculate control limits for each bearing and channel
    control_limits = {}
    for bearing in data['bearing'].unique():
        bearing_data = data[data['bearing'] == bearing]
        for channel in ['c1', 'c2']:
            for parameter in ['bearing1', 'bearing2', 'bearing3', 'bearing4']:
                column = f'{parameter}_{channel}'
                control_limits[(bearing, channel, parameter)] = calculate_control_limits(bearing_data, column)


    # Save control limits to the database
    save_control_limits(control_limits)


if __name__ == '__main__':
    main()
