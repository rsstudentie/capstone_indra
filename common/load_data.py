import json
import psycopg2
import pandas as pd


def load_data(tablename = 'bearings_filled'):
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
    sql = "SELECT * FROM bearings"

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
