from sqlitedb import create_connection
import json

def generate_hospital_report(hospital_id):
    # Connect to the sqlite database
    conn = create_connection()
    
    # Create a cursor object
    c = conn.cursor()
    
    # Execute a query to retrieve the patient status for the given hospital (still working on finishing queries)
    c.execute(f"SELECT COUNT(*) FROM hospital_data WHERE hospital_id='{hospital_id}' AND patient_status=1")
    in_patients = c.fetchone()[0]
    
    c.execute(f"SELECT COUNT(*) FROM hospital_data WHERE hospital_id='{hospital_id}' AND patient_status=2")
    icu_patients = c.fetchone()[0]
    
    c.execute(f"SELECT COUNT(*) FROM hospital_data WHERE hospital_id='{hospital_id}' AND patient_status=3")
    ventilator_patients = c.fetchone()[0]

    c.execute(f"SELECT patient_mrn FROM hospital_data WHERE hospital_id='{hospital_id}' AND patient_status=1")
    in_patient_list = c.fetchall()

    query = "select COUNT(*) FROM vax_data WHERE patient_mrn IN {}".format(in_patient_list)
    c.execute(query)
    vaccinated_in_patients = c.fetchone()[0]

    c.execute(f"SELECT patient_mrn FROM hospital_data WHERE hospital_id='{hospital_id}' AND patient_status=2")
    icu_patient_list = c.fetchall()

    query = "select COUNT(*) FROM vax_data WHERE patient_mrn IN {}".format(icu_patient_list)
    c.execute(query)
    vaccinated_icu_patients = c.fetchone()[0]

    c.execute(f"SELECT patient_mrn FROM hospital_data WHERE hospital_id='{hospital_id}' AND patient_status=3")
    ventilator_patient_list = c.fetchall()

    query = "select COUNT(*) FROM vax_data WHERE patient_mrn IN {}".format(ventilator_patient_list)
    c.execute(query)
    vaccinated_vent_patients = c.fetchone()[0]
    
    # Calculate the percentage of vaccinated patients
    percent_vaccinated_in = (vaccinated_in_patients / in_patients)
    percent_vaccinated_icu = (vaccinated_icu_patients / icu_patients)
    percent_vaccinated_ventilator = (vaccinated_vent_patients / ventilator_patients)
    
    # Create a dictionary with the report data
    report_data = {
        "in-patient_count": in_patients,
        "in-patient_vax": percent_vaccinated_in,
        "icu-patient_count": icu_patients,
        "icu_patient_vax": percent_vaccinated_icu,
        "patient_vent_count": ventilator_patients,
        "patient_vent_vax": percent_vaccinated_ventilator
    }
    
    # Close the connection
    conn.close()

    # Return data
    return(report_data)

def generate_overall_report():
    # Connect to the sqlite database
    conn = create_connection()
    
    # Create a cursor object
    c = conn.cursor()
    
    # Execute a query to retrieve the patient status for the given hospital (still working on finishing queries)
    c.execute(f"SELECT COUNT(*) FROM hospital_data WHERE patient_status=1")
    in_patients = c.fetchone()[0]
    
    c.execute(f"SELECT COUNT(*) FROM hospital_data WHERE patient_status=2")
    icu_patients = c.fetchone()[0]
    
    c.execute(f"SELECT COUNT(*) FROM hospital_data WHERE patient_status=3")
    ventilator_patients = c.fetchone()[0]

    c.execute(f"SELECT patient_mrn FROM hospital_data WHERE patient_status=1")
    in_patient_list = c.fetchall()

    query = "select COUNT(*) FROM vax_data WHERE patient_mrn IN {}".format(in_patient_list)
    c.execute(query)
    vaccinated_in_patients = c.fetchone()[0]

    c.execute(f"SELECT patient_mrn FROM hospital_data WHERE patient_status=2")
    icu_patient_list = c.fetchall()

    query = "select COUNT(*) FROM vax_data WHERE patient_mrn IN {}".format(icu_patient_list)
    c.execute(query)
    vaccinated_icu_patients = c.fetchone()[0]

    c.execute(f"SELECT patient_mrn FROM hospital_data WHERE patient_status=3")
    ventilator_patient_list = c.fetchall()

    query = "select COUNT(*) FROM vax_data WHERE patient_mrn IN {}".format(ventilator_patient_list)
    c.execute(query)
    vaccinated_vent_patients = c.fetchone()[0]
    
    # Calculate the percentage of vaccinated patients
    percent_vaccinated_in = (vaccinated_in_patients / in_patients)
    percent_vaccinated_icu = (vaccinated_icu_patients / icu_patients)
    percent_vaccinated_ventilator = (vaccinated_vent_patients / ventilator_patients)
    
    # Create a dictionary with the report data
    report_data = {
        "in-patient_count": in_patients,
        "in-patient_vax": percent_vaccinated_in,
        "icu-patient_count": icu_patients,
        "icu_patient_vax": percent_vaccinated_icu,
        "patient_vent_count": ventilator_patients,
        "patient_vent_vax": percent_vaccinated_ventilator
    }
    
    # Close the connection
    conn.close()

    # Return data
    return(report_data)