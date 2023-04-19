from sqlitedb import create_connection, reset_sqlite_db

zip_positive_map = {} # Used to store positive covid cases per zip code {key = zipcode, value = number of positive patients}
zip_alert_list = [] # Used to store the zip codes that have doubled their positive cases between incoming batches

# Queries the sqlite db to update both the zip positive map and the zip alert list
def update_zip_positive_map():
    conn = create_connection()
    cursor = conn.cursor()
    sql = "SELECT patient_zipcode, count(*) FROM patient_data WHERE patient_status = 1 GROUP BY patient_zipcode"
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    zip_alert_list.clear()
    for row in rows:
        if row[0] in zip_positive_map:
            if (row[1] / 2) >= zip_positive_map[row[0]]:
                zip_alert_list.append(row[0])
        zip_positive_map[row[0]] = row[1]

# Returns the zip alert list
def retrieve_zip_alert_list():
    return zip_alert_list

# Resets both the sqlite db and pyorient db
def reset_dbs():
    reset_status = reset_sqlite_db()
    return reset_status


# Generates the complete hospital report for the get patient status endpoint given a hospital id
def generate_hospital_report(hospital_id):
    
    # Connect to the sqlite database
    conn = create_connection()
    
    # Create a cursor object
    c = conn.cursor()
    
    # Execute queries to get number of in-patients, icu patients, and ventilator patients  
    c.execute(f"SELECT COUNT(*) FROM hospital_data WHERE hospital_id='{hospital_id}' AND patient_status=1;")
    in_patients = c.fetchone()[0]
    
    c.execute(f"SELECT COUNT(*) FROM hospital_data WHERE hospital_id='{hospital_id}' AND patient_status=2;")
    icu_patients = c.fetchone()[0]
    
    c.execute(f"SELECT COUNT(*) FROM hospital_data WHERE hospital_id='{hospital_id}' AND patient_status=3;")
    ventilator_patients = c.fetchone()[0]
    
    # Query to get the list of patient_mrns that are in-patients at this hospital
    c.execute(f"SELECT patient_mrn FROM hospital_data WHERE hospital_id='{hospital_id}' AND patient_status=1;")
    in_patient_list = c.fetchall()

    # Turning results of query into a nice tuple
    clean_in_patient_list = []
    for patient_mrn in in_patient_list:
        clean_in_patient_list.append(patient_mrn[0])

    clean_in_patient_list = tuple(clean_in_patient_list)

    # Query to get number of vaccinated patients from this hospital in-patient list
    # If only one patient in list the query string has to be formatted differently
    if len(clean_in_patient_list) == 1:
        query = f"SELECT COUNT(*) FROM vax_data WHERE patient_mrn='{clean_in_patient_list[0]}';"
    # Insert tuple into query string
    else:
        query = "select COUNT(*) FROM vax_data WHERE patient_mrn IN {}".format(clean_in_patient_list)
    c.execute(query)
    vaccinated_in_patients = c.fetchone()[0]

    # Query to get the list of patient_mrns that are icu patients at this hospital
    c.execute(f"SELECT patient_mrn FROM hospital_data WHERE hospital_id='{hospital_id}' AND patient_status=2")
    icu_patient_list = c.fetchall()

    # Turning results of query into a nice tuple
    clean_icu_patient_list = []
    for patient_mrn in icu_patient_list:
        clean_icu_patient_list.append(patient_mrn[0])

    clean_icu_patient_list = tuple(clean_icu_patient_list)

    # Query to get number of vaccinated patients from this hospital icu patient list
    # If only one patient in list the query string has to be formatted differently
    if len(clean_icu_patient_list) == 1:
        query = f"SELECT COUNT(*) FROM vax_data WHERE patient_mrn='{clean_icu_patient_list[0]}';"
    # Insert tuple into query string
    else:
        query = "select COUNT(*) FROM vax_data WHERE patient_mrn IN {}".format(clean_icu_patient_list)
    c.execute(query)
    vaccinated_icu_patients = c.fetchone()[0]

    # Query to get the list of patient_mrns that are ventilator patients at this hospital
    c.execute(f"SELECT patient_mrn FROM hospital_data WHERE hospital_id='{hospital_id}' AND patient_status=3")
    ventilator_patient_list = c.fetchall()

    # Turning results of query into a nice tuple
    clean_ventilator_patient_list = []
    for patient_mrn in ventilator_patient_list:
        clean_ventilator_patient_list.append(patient_mrn[0])

    clean_ventilator_patient_list = tuple(clean_ventilator_patient_list)

    # Query to get number of vaccinated patients from this hospital ventialtor patient list
    # If only one patient in list the query string has to be formatted differently
    if len(clean_ventilator_patient_list) == 1:
        query = f"SELECT COUNT(*) FROM vax_data WHERE patient_mrn='{clean_ventilator_patient_list[0]}';"
    # Insert tuple into query string
    else:
        query = "select COUNT(*) FROM vax_data WHERE patient_mrn IN {}".format(clean_ventilator_patient_list)
    c.execute(query)
    vaccinated_ventilator_patients = c.fetchone()[0]

    # Close the database connection
    conn.close()
    
    # Calculate the percentage of vaccinated patients
    if in_patients != 0:
        percent_vaccinated_in = (vaccinated_in_patients / in_patients)
    else:
        percent_vaccinated_in = "NA"
    if icu_patients != 0:
        percent_vaccinated_icu = (vaccinated_icu_patients / icu_patients)
    else:
        percent_vaccinated_icu = "NA"
    if ventilator_patients != 0:
        percent_vaccinated_ventilator = (vaccinated_ventilator_patients / ventilator_patients)
    else:
        percent_vaccinated_ventilator = "NA"
    
    # Create a dictionary with the report data
    report_data = {
        "in-patient_count": in_patients,
        "in-patient_vax": percent_vaccinated_in,
        "icu-patient_count": icu_patients,
        "icu_patient_vax": percent_vaccinated_icu,
        "patient_vent_count": ventilator_patients,
        "patient_vent_vax": percent_vaccinated_ventilator
    }

    # Return data
    return(report_data)

# Generates the complete hospital report for the get patient status endpoint for all hospitals
def generate_overall_report():
    
    # Connect to the sqlite database
    conn = create_connection()
    
    # Create a cursor object
    c = conn.cursor()
    
    # Execute queries to get number of in-patients, icu patients, and ventilator patients  
    c.execute(f"SELECT COUNT(*) FROM hospital_data WHERE patient_status=1;")
    in_patients = c.fetchone()[0]
    
    c.execute(f"SELECT COUNT(*) FROM hospital_data WHERE patient_status=2;")
    icu_patients = c.fetchone()[0]
    
    c.execute(f"SELECT COUNT(*) FROM hospital_data WHERE patient_status=3;")
    ventilator_patients = c.fetchone()[0]
    
    # Query to get the list of patient_mrns that are in-patients at this hospital
    c.execute(f"SELECT patient_mrn FROM hospital_data WHERE patient_status=1;")
    in_patient_list = c.fetchall()

    # Turning results of query into a nice tuple
    clean_in_patient_list = []
    for patient_mrn in in_patient_list:
        clean_in_patient_list.append(patient_mrn[0])

    clean_in_patient_list = tuple(clean_in_patient_list)

    # Query to get number of vaccinated patients from this hospital in-patient list
    # If only one patient in list the query string has to be formatted differently
    if len(clean_in_patient_list) == 1:
        query = f"SELECT COUNT(*) FROM vax_data WHERE patient_mrn='{clean_in_patient_list[0]}';"
    # Insert tuple into query string
    else:
        query = "select COUNT(*) FROM vax_data WHERE patient_mrn IN {}".format(clean_in_patient_list)
    c.execute(query)
    vaccinated_in_patients = c.fetchone()[0]

    # Query to get the list of patient_mrns that are icu patients at this hospital
    c.execute(f"SELECT patient_mrn FROM hospital_data WHERE patient_status=2")
    icu_patient_list = c.fetchall()

    # Turning results of query into a nice tuple
    clean_icu_patient_list = []
    for patient_mrn in icu_patient_list:
        clean_icu_patient_list.append(patient_mrn[0])

    clean_icu_patient_list = tuple(clean_icu_patient_list)

    # Query to get number of vaccinated patients from this hospital icu patient list
    # If only one patient in list the query string has to be formatted differently
    if len(clean_icu_patient_list) == 1:
        query = f"SELECT COUNT(*) FROM vax_data WHERE patient_mrn='{clean_icu_patient_list[0]}';"
    # Insert tuple into query string
    else:
        query = "select COUNT(*) FROM vax_data WHERE patient_mrn IN {}".format(clean_icu_patient_list)
    c.execute(query)
    vaccinated_icu_patients = c.fetchone()[0]

    # Query to get the list of patient_mrns that are ventilator patients at this hospital
    c.execute(f"SELECT patient_mrn FROM hospital_data WHERE patient_status=3")
    ventilator_patient_list = c.fetchall()

    # Turning results of query into a nice tuple
    clean_ventilator_patient_list = []
    for patient_mrn in ventilator_patient_list:
        clean_ventilator_patient_list.append(patient_mrn[0])

    clean_ventilator_patient_list = tuple(clean_ventilator_patient_list)

    # Query to get number of vaccinated patients from this hospital ventialtor patient list
    # If only one patient in list the query string has to be formatted differently
    if len(clean_ventilator_patient_list) == 1:
        query = f"SELECT COUNT(*) FROM vax_data WHERE patient_mrn='{clean_ventilator_patient_list[0]}';"
    # Insert tuple into query string
    else:
        query = "select COUNT(*) FROM vax_data WHERE patient_mrn IN {}".format(clean_ventilator_patient_list)
    c.execute(query)
    vaccinated_ventilator_patients = c.fetchone()[0]

    # Close the database connection
    conn.close()
    
    # Calculate the percentage of vaccinated patients
    if in_patients != 0:
        percent_vaccinated_in = (vaccinated_in_patients / in_patients)
    else:
        percent_vaccinated_in = "NA"
    if icu_patients != 0:
        percent_vaccinated_icu = (vaccinated_icu_patients / icu_patients)
    else:
        percent_vaccinated_icu = "NA"
    if ventilator_patients != 0:
        percent_vaccinated_ventilator = (vaccinated_ventilator_patients / ventilator_patients)
    else:
        percent_vaccinated_ventilator = "NA"
    
    # Create a dictionary with the report data
    report_data = {
        "in-patient_count": in_patients,
        "in-patient_vax": percent_vaccinated_in,
        "icu-patient_count": icu_patients,
        "icu_patient_vax": percent_vaccinated_icu,
        "patient_vent_count": ventilator_patients,
        "patient_vent_vax": percent_vaccinated_ventilator
    }

    # Return data
    return(report_data)


