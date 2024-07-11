from deepecho import PARModel
import pandas as pd
from datetime import timedelta
import psycopg2
import json

def generate_synthetic_data(data):
    # Define data types for all the columns
    data_types = {
        'eventtime': 'timestamp',
        'bearing1_c1': 'continuous',
        'bearing1_c2': 'continuous',
        'bearing2_c1': 'continuous',
        'bearing2_c2': 'continuous',
        'bearing3_c1': 'continuous',
        'bearing3_c2': 'continuous',
        'bearing4_c1': 'continuous',
        'bearing4_c2': 'continuous',
    }

    # Instantiate PARModel and generate synthetic data
    # Instantiate PARModel
    model = PARModel(cuda=False)  # Set cuda=True if GPU is available and beneficial for your dataset

    # Learn a model from the data
    model.fit(
        data=data,
        entity_columns=[],  # Specify entity columns if applicable
        context_columns=[],  # Specify context columns if applicable
        data_types=data_types,
        sequence_index='eventtime',  # Specify the column that represents the sequence index
        segment_size=1  # Provide segment_size when entity_columns are empty
    )

    # Calculate the start and end date for generating synthetic data (5 months before and including the max eventtime)
    end_date = min(data['eventtime'])
    start_date = end_date - timedelta(days=5 * 30)  # Assuming 30 days per month

    # Generate timestamps at 10-minute intervals from start_date to end_date
    timestamps = pd.date_range(start=start_date, end=end_date, freq='10T')

    # Sample new data at 10-minute intervals for the defined period
    synthetic_data = model.sample(
        num_entities=len(timestamps),  # Number of timestamps
        sequence_length=1,  # Single point for each timestamp
        segment_size=1  # Provide segment_size when entity_columns are empty
    )

    return synthetic_data


def load_data():
    """
    Load data from database and return as a pandas DataFrame.
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

    # Query to select data from the database
    sql = "SELECT * FROM bearings_filled"

    # Execute the query and fetch the results
    cur.execute(sql)
    data = cur.fetchall()

    # Get the column names from the cursor description
    columns = [desc[0] for desc in cur.description]

    # Create a DataFrame from the fetched data and column names
    df = pd.DataFrame(data, columns=columns)

    # Close the cursor and connection
    cur.close()
    conn.close()

    return df


def main():
    # Load data from database
    data = load_data()

    # Generate synthetic data
    synthetic_data = generate_synthetic_data(data)

    # Save the synthetic data to the database table bearings_synthetic
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
            INSERT INTO bearings_synthetic (eventtime, bearing1_c1, bearing1_c2, bearing2_c1, bearing2_c2, bearing3_c1, bearing3_c2, bearing4_c1, bearing4_c2) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

    # Insert synthetic data into the database
    for index, row in synthetic_data.iterrows():
        cur.execute(sql, [index] + row.tolist())

    # Commit the transaction and close the connection
    conn.commit()
    cur.close()
    conn.close()

    print("Synthetic data saved to database.")

if __name__ == "__main__":
    main()