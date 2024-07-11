import psycopg2
import json

# Function to save received data to database
def save_to_db(timestamp, int_avgs):
    # Database connection details. Load these from environment variables or a env.json
    configdetails = json.loads(open('../env.json').read())
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
            INSERT INTO bearings (eventtime, bearing1_c1, bearing1_c2, bearing2_c1, bearing2_c2, bearing3_c1, bearing3_c2, bearing4_c1, bearing4_c2) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    cur.execute(sql, [timestamp] + int_avgs)

    # Commit the transaction and close the connection
    conn.commit()
    cur.close()
    conn.close()
