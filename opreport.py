# Importing subscriber code has not been used yet 
import sqlitedb
import subscriber 
import json

def generate_hospital_report(hospital_name):
    # Connect to the sqlite database
    conn = sqlitedb.connect('cs505relational.db')
    
    # Create a cursor object
    c = conn.cursor()
    
    # Execute a query to retrieve the patient status for the given hospital (still working on finishing queries)
    c.execute(f"SELECT COUNT(*) FROM patients WHERE hospital='{hospital_name}' AND status='in-patient'")
    in_patients = c.fetchone()[0]
    
    c.execute(f"SELECT COUNT(*) FROM patients WHERE hospital='{hospital_name}' AND status='icu'")
    icu_patients = c.fetchone()[0]
    
    c.execute(f"SELECT COUNT(*) FROM patients WHERE hospital='{hospital_name}' AND status='ventilator'")
    ventilator_patients = c.fetchone()[0]
    
    c.execute(f"SELECT COUNT(*) FROM patients WHERE hospital='{hospital_name}' AND vaccinated='yes' AND status='in-patient'")
    vaccinated_in_patients = c.fetchone()[0]

    c.execute(f"SELECT COUNT(*) FROM patients WHERE hospital='{hospital_name}' AND vaccinated='yes' AND status='icu'")
    vaccinated_icu_patients = c.fetchone()[0]

    c.execute(f"SELECT COUNT(*) FROM patients WHERE hospital='{hospital_name}' AND vaccinated='yes' AND status='ventilator'")
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
    
    # Convert the dictionary to a JSON string
    json_data = json.dumps(report_data)
    
    # Print the JSON string to the console
    print(json_data)
    
    # Close the connection
    conn.close()

