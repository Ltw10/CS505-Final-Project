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
    except Error as e:
        print(e)
    
    return conn

def create_patient_data_table():
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS patient_data " 
              "(testing_id integer, patient_name text, patient_mrn text, patient_zipcode integer, patient_status integer);")
    conn.commit()
    conn.close()

def create_hospital_data_table():
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS hospital_data " 
              "(hospital_id integer, patient_mrn text, patient_name text, patient_status integer);")
    conn.commit()
    conn.close()

def create_vax_data_table():
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS vax_data " 
              "(vaccination_id integer, patient_mrn text, patient_name text);")
    conn.commit()
    conn.close()
    
def insert_into_patient_data_sqlite(entry):
    conn = create_connection()
    cursor = conn.cursor()
    insert_query = "INSERT INTO patient_data (testing_id, patient_name, patient_mrn, patient_zipcode, patient_status) VALUES (?, ?, ?, ?, ?);"
    values = (entry["testing_id"], entry["patient_name"], entry["patient_mrn"], entry["patient_zipcode"], entry["patient_status"])
    cursor.execute(insert_query, values)
    conn.commit()
    conn.close()

def insert_into_hospital_data_sqlite(entry):
    conn = create_connection()
    cursor = conn.cursor()
    insert_query = "INSERT INTO hospital_data (hospital_id, patient_mrn, patient_name, patient_status) VALUES (?, ?, ?, ?);"
    values = (entry["hospital_id"], entry["patient_mrn"], entry["patient_name"], entry["patient_status"])
    cursor.execute(insert_query, values)
    conn.commit()
    conn.close()

def insert_into_vax_data_sqlite(entry):
    conn = create_connection()
    cursor = conn.cursor()
    insert_query = "INSERT INTO vax_data (vaccination_id, patient_mrn, patient_name) VALUES (?, ?, ?);"
    values = (entry["vaccination_id"], entry["patient_mrn"], entry["patient_name"])
    cursor.execute(insert_query, values)
    conn.commit()
    conn.close()

def reset_sqlite_db():
    conn = create_connection()
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS patient_data;")
    cursor.execute("DROP TABLE IF EXISTS hospital_data;")
    cursor.execute("DROP TABLE IF EXISTS vax_data;")
    conn.commit()
    conn.close()
    print("Tables Dropped")
    create_patient_data_table()
    create_hospital_data_table()
    create_vax_data_table()
