import pyorient
from pyorient import OrientDB
from pyorient.ogm import Graph
import re

dbname = "cs505graph"
login = "root"
password = "rootpwd"

def create_graphDB(client):
    # Create New Database
    client.db_create(dbname,
        pyorient.DB_TYPE_GRAPH,
        pyorient.STORAGE_TYPE_PLOCAL)

    db = client.db_open(dbname, login, password, pyorient.DB_TYPE_GRAPH)

    client.command("CREATE CLASS Patient EXTENDS V")
    client.command("CREATE PROPERTY Patient.MRN String")
    client.command("CREATE PROPERTY Patient.contactList EMBEDDEDLIST String")
    client.command("CREATE PROPERTY Patient.eventList EMBEDDEDLIST String")

def reset_graphDB():
    client = pyorient.OrientDB("localhost", 2424)
    session = client.connect(login, password)
    
    if client.db_exists(dbname):
        client.db_drop(dbname)
        
    # Create New Database
    create_graphDB(client)
    
    client.db_close()

def get_graph_contacts(client, patient_mrn):
    #client = pyorient.OrientDB("localhost", 2424)
    #session = client.connect(login, password)
    
    #db = client.db_open(dbname, login, password, pyorient.DB_TYPE_GRAPH)

    query = "SELECT expand(out('Contact').MRN) FROM Patient WHERE MRN = '" + str(patient_mrn) + "'"
    result = client.command(query)
    graph_contacts = [res.MRN for res in result]
    print(graph_contacts)

    #client.db_close()


def insert_into_graph(entries):
    client = pyorient.OrientDB("localhost", 2424)
    session = client.connect(login, password)

    if not client.db_exists(dbname):
        create_graphDB

    else: 
        db = client.db_open(dbname, login, password, pyorient.DB_TYPE_GRAPH)

    for index, entry in enumerate(entries):
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
               # Check if the MRN in the result is not equal to the MRN of the current patient
                if str(result[0].MRN) != str(patient_mrn):
                    # Create an edge between the current patient and the contact patient
                    contact_patient_rid = result[0]._rid
                    query = "CREATE EDGE Contact FROM " + current_patient_rid + " TO " + contact_patient_rid
                    client.command(query)
                    #print("EDGE CREATED")
        
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
    

    client.db_close()


#  # Define the query to retrieve all patients
#     query = "SELECT FROM Patient"
#     # Execute the query
#     result = client.query(query)
#     # Iterate through the result and print the property values for each patient
#     for record in result:
#         mrn = record.oRecordData['MRN']
#         contact_list = record.oRecordData['contactList']
#         event_list = record.oRecordData['eventList']
#         print("MRN: {}".format(mrn))
#         print("contactList: {}".format(contact_list))
#         print("eventList: {}".format(event_list))

reset_graphDB()