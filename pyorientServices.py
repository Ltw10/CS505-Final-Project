import pyorient
from pyorient import OrientDB
from pyorient.ogm import Graph
import re

dbname = "cs505graph"
login = "root"
password = "rootpwd"

def reset_graphDB(client, name):
    if client.db_exists(name):
        client.db_drop(name)

    # Create New Database
    client.db_create(name,
        pyorient.DB_TYPE_GRAPH,
        pyorient.STORAGE_TYPE_PLOCAL)

def create_patient(db, patient_mrn):
    patient = db.create_vertex("patient")
    patient.patient_mrn = patient_mrn
    patient.save()
    return patient

def get_contacts(db, patient_mrn):
    query = "TRAVERSE in(), out(), inV(), outV() FROM (SELECT FROM patient WHERE patient_mrn = '{patient_mrn}') WHILE $depth <= 2"
    results = db.query(query)
    
    for item in results:
        print("Contact: " + item.patient_mrn)

def createGraphDB(entries):
    # Connect to OrientDB server
    client = pyorient.OrientDB("localhost", 2424)
    session = client.connect(login, password)

    reset_graphDB(client, dbname)

    # Open the database
    db = client.db_open(dbname, login, password, pyorient.DB_TYPE_GRAPH)

    client.command("CREATE CLASS Patient EXTENDS V")
    client.command("CREATE PROPERTY Patient.MRN String")
    client.command("CREATE PROPERTY Patient.contactList EMBEDDEDLIST String")
    client.command("CREATE PROPERTY Patient.eventList EMBEDDEDLIST String")

    for entry in entries:
        patient_mrn = entry['patient_mrn']
        contact_list = entry['contact_list']
        event_list = entry['event_list']
        
        query = "CREATE VERTEX Patient SET MRN = '" + str(patient_mrn) + "', contactList = " + str(contact_list) + ", eventList = " + str(event_list)
        result = client.command(query)
        current_patient_rid = result[0]._rid

        for mrnOfContact in contact_list:
            # Check if contact MRN already exists as a Patient
            query = "SELECT FROM Patient WHERE MRN = '" + str(mrnOfContact) + "'"
            result = client.command(query)
            
            if len(result) > 0:
                # Create an edge between the current patient and the contact patient
                contact_patient_rid = result[0]._rid
                query = "CREATE EDGE Contact FROM " + current_patient_rid + " TO " + contact_patient_rid
                client.command(query)
                print("EDGE CREATED ")

    for entry in entries:
        patient_mrn = entry['patient_mrn']
        query = "SELECT FROM Patient WHERE MRN = '" + str(patient_mrn) + "'"
        result = client.command(query)

        if len(result) > 0:
            requested_patient_rid = result[0]._rid

            # Use TRAVERSE to find all vertices connected by the "Contact" edge to the requested MRN
            query = "TRAVERSE out('Contact') FROM " + requested_patient_rid
            result = client.command(query)

            # Extract MRNs from the query result
            mrns = [r.MRN for r in result if 'MRN' in r.oRecordData]
            print(str(patient_mrn) + "has the mrns:", mrns)
        else:
            print("Requested MRN not found.")

    

    # # Define the query to retrieve all patients
    # query = "SELECT FROM Patient"
    # # Execute the query
    # result = client.query(query)
    # # Iterate through the result and print the property values for each patient
    # for record in result:
    #     mrn = record.oRecordData['MRN']
    #     contact_list = record.oRecordData['contactList']
    #     event_list = record.oRecordData['eventList']
    #     print("MRN: {}".format(mrn))
    #     print("contactList: {}".format(contact_list))
    #     print("eventList: {}".format(event_list))

    

    client.db_close()