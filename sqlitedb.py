import sqlite3
import os
from sqlite3 import Error

# Create directory where SQLite DB will be stored if not already created
# Create connection to DB
def create_connection():
    db_file = "cs505relational.db"
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print("Connected to SQLite DB")
    except Error as e:
        print(e)
    
    return conn

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS patient_data " 
              "(testing_id integer, patient_mrn integer, patient_name text, patient_zipcode integer, patient_status integer);")
    conn.commit()
    
def insert_into_sqlite(entry):
    conn = create_connection()
    cursor = conn.cursor()
    insert_query = "INSERT INTO patient_data (testing_id, patient_mrn, patient_name, patient_zipcode, patient_status) VALUES (?, ?, ?, ?, ?);"
    values = (entry["testing_id"], entry["patient_mrn"], entry["patient_name"], entry["patient_zipcode"], entry["patient_status"])
    cursor.execute(insert_query, values)
    conn.commit()
    conn.close()

def reset_sqlite_db():
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute("DROP TABLE patient_data;")
    conn.commit()
    print("Table Dropped")
    create_table(conn)
    conn.close()
