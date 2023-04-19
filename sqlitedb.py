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

# Create the patient data table in sqlite
def create_patient_data_table():
    conn = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS patient_data " 
              "(testing_id integer, patient_name text, patient_mrn text, patient_zipcode integer, patient_status integer);")
        conn.commit()
    except Error as e:
        print("Error creating patient data table in sqlite db \n" + e)
    conn.close()

# Create the hospital data table in sqlite
def create_hospital_data_table():
    conn = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS hospital_data " 
              "(hospital_id integer, patient_mrn text, patient_name text, patient_status integer);")
        conn.commit()
    except Error as e:
        print("Error cerating the hospital data table in sqlite db \n" + e)
    conn.close()

# Create the vaccination data table in sqlite
def create_vax_data_table():
    conn = create_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS vax_data " 
              "(vaccination_id integer, patient_mrn text, patient_name text);")
        conn.commit()
    except Error as e:
        print("Error creating vaccination data table in sqlite db \n" + e)
    conn.close()

# Insert data entry into the patient data table in sqlite    
def insert_into_patient_data_sqlite(entry):
    conn = create_connection()
    cursor = conn.cursor()
    try:
        insert_query = "INSERT INTO patient_data (testing_id, patient_name, patient_mrn, patient_zipcode, patient_status) VALUES (?, ?, ?, ?, ?);"
        values = (entry["testing_id"], entry["patient_name"], entry["patient_mrn"], entry["patient_zipcode"], entry["patient_status"])
        cursor.execute(insert_query, values)
        conn.commit()
    except Error as e:
        print("Error inserting into patient data table in sqlite db \n" + e)
    conn.close()

# Insert data entry into the hospital data table in sqlite  
def insert_into_hospital_data_sqlite(entry):
    conn = create_connection()
    cursor = conn.cursor()
    try:
        insert_query = "INSERT INTO hospital_data (hospital_id, patient_mrn, patient_name, patient_status) VALUES (?, ?, ?, ?);"
        values = (entry["hospital_id"], entry["patient_mrn"], entry["patient_name"], entry["patient_status"])
        cursor.execute(insert_query, values)
        conn.commit()
    except Error as e:
        print("Error inserting into hospital data table in sqlite db \n" + e)
    conn.close()

# Insert data entry into the vaccination data table in sqlite  
def insert_into_vax_data_sqlite(entry):
    conn = create_connection()
    cursor = conn.cursor()
    try:
        insert_query = "INSERT INTO vax_data (vaccination_id, patient_mrn, patient_name) VALUES (?, ?, ?);"
        values = (entry["vaccination_id"], entry["patient_mrn"], entry["patient_name"])
        cursor.execute(insert_query, values)
        conn.commit()
    except Error as e:
        print("Error inserting into vaccination data table in sqlite db \n" + e)
    conn.close()

# Drops all sqlite db tables and recreates them
def reset_sqlite_db():
    try:
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
    except Error as e:
        print("Error resetting the sqlite database \n" + e)
        return 0
    return 1