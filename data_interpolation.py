### DATA INTERPOLATION ###
# This module is responsible for loading data from the database, interpolating missing values, and saving the interpolated data back to the database.
# The load_data function reads data from the database table 'bearings'.
# The interpolate_and_fill function interpolates missing values in the data at a specified time interval using linear interpolation.
# The main function loads the data, interpolates it, and saves the interpolated data to the database table 'bearings_filled'.


import pandas as pd
import psycopg2
import json

from common.load_data import load_data


def interpolate_and_fill(df, interval='10T'):
    """
    Interpolate missing values in a DataFrame at a specified time interval.
    """
    # Sort dataframe by eventtime
    df = df.sort_values(by='eventtime').reset_index(drop=True)

    # Set eventtime as the index
    df = df.set_index('eventtime')

    # Resample to specified interval and interpolate
    df_resampled = df.resample(interval).mean().interpolate(method='linear')

    # Reset index and fill any remaining missing values with the mean of each column
    df_filled = df_resampled.reset_index().fillna(df.mean())

    return df_filled


def main():
    # Load data from database
    data = load_data()

    # Interpolate missing values and fill remaining missing values
    interpolated_data = interpolate_and_fill(data)

    # Save the interpolated data to the database table bearings_filled
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
            INSERT INTO bearings_filled (eventtime, bearing1_c1, bearing1_c2, bearing2_c1, bearing2_c2, bearing3_c1, bearing3_c2, bearing4_c1, bearing4_c2) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

    # Iterate over the rows of the DataFrame and insert each row into the database
    for index, row in interpolated_data.iterrows():
        cur.execute(sql, row)

    # Commit the transaction and close the connection
    conn.commit()
    cur.close()
    conn.close()

    print("Interpolated data saved to database.")


if __name__ == "__main__":
    main()
