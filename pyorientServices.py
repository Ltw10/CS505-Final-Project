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
    client.command("CREATE CLASS Contact EXTENDS E")
    client.command("CREATE CLASS Event EXTENDS V")
    client.command("CREATE PROPERTY Event.eventId String")
    client.command("CREATE CLASS Attended EXTENDS E")

def reset_graphDB():
    client = pyorient.OrientDB("localhost", 2424)
    session = client.connect(login, password)
    
    if client.db_exists(dbname):
        client.db_drop(dbname)
        
    # Create New Database
    create_graphDB(client)
    
    client.db_close()

def get_graph_contacts(patient_mrn):
    client = pyorient.OrientDB("localhost", 2424)
    session = client.connect(login, password)
    
    db = client.db_open(dbname, login, password, pyorient.DB_TYPE_GRAPH)

    query = "SELECT expand(both(`Contact`).MRN) FROM Patient WHERE MRN = '" + str(patient_mrn) + "'"
    result = client.command(query)
    graph_contacts = [res.value for res in result]
    return graph_contacts

    client.db_close()

def get_graph_event_contacts(patient_mrn):
    client = pyorient.OrientDB("localhost", 2424)
    session = client.connect(login, password)
    
    db = client.db_open(dbname, login, password, pyorient.DB_TYPE_GRAPH)

    possible_contacts = {}

    query = "SELECT expand(out(`Attended`).eventId) FROM Patient WHERE MRN = '" + patient_mrn + "'"
    result = client.command(query)
    event_ids = [res.value for res in result]
    for event_id in event_ids:
        query = "SELECT expand(in(`Attended`).MRN) FROM Event WHERE eventId = '" + event_id + "'"
        result = client.command(query)
        possible_contact_mrns = [res.value for res in result]
        possible_contact_mrns.remove(patient_mrn)
        possible_contacts[event_id] = possible_contact_mrns
    print(possible_contacts)

    client.db_close()

    return possible_contacts

def insert_into_graph(entries):
    client = pyorient.OrientDB("localhost", 2424)
    session = client.connect(login, password)

    if not client.db_exists(dbname):
        create_graphDB(client)

    else: 
        db = client.db_open(dbname, login, password, pyorient.DB_TYPE_GRAPH)

    # Create patient verticies
    for entry in entries:
        patient_mrn = entry['patient_mrn']
        contact_list = entry['contact_list']
        event_list = entry['event_list']
        
        query = "CREATE VERTEX Patient SET MRN = '" + patient_mrn + "', contactList = " + str(contact_list) + ", eventList = " + str(event_list)
        result = client.command(query)
        current_patient_rid = result[0]._rid

    # Creating contact list edges and event list edges
    for entry in entries:
        
        patient_mrn = entry['patient_mrn']
        contact_list = entry['contact_list']
        event_list = entry['event_list']
        
        query = "SELECT FROM Patient WHERE MRN = '" + patient_mrn + "'"
        result = client.command(query)
        current_patient_rid = result[0]._rid
        
        # Loops through all contacts from contact list to create an edge
        for mrnOfContact in contact_list:
            query = "SELECT FROM Patient WHERE MRN = '" + mrnOfContact + "'"
            result = client.command(query)
            # Check if patient already exists
            if len(result) > 0:
                contact_patient_rid = result[0]._rid 
            # If patient with mrn doesn't exist create them
            else:
                query = "CREATE VERTEX Patient SET MRN = '" + mrnOfContact + "'"
                result = client.command(query)
                contact_patient_rid = result[0]._rid
            # Check if this is connection to itself
            if current_patient_rid != contact_patient_rid:
                query = "SELECT * FROM Contact WHERE (in.MRN = '" + patient_mrn + "' or out.MRN = '" + patient_mrn + "') and (in.MRN = '" + mrnOfContact + "' or out.MRN = '" + mrnOfContact + "')"
                result = client.command(query)
                # Check for already existing connection
                if len(result) == 0:
                    query = "CREATE EDGE Contact FROM " + current_patient_rid + " TO " + contact_patient_rid
                    client.command(query)

        for event in event_list:
            query = "SELECT FROM Event WHERE eventId = '" + event + "'"
            result = client.command(query)
            # Event does not yet exist
            if len(result) == 0:
                query = "CREATE VERTEX Event SET eventId = '" + event + "'"
                result = client.command(query)
                event_rid = result[0]._rid
            # Event exists
            else:
                query = "SELECT * FROM Event WHERE eventId = '" + event + "'"
                result = client.command(query)
                event_rid = result[0]._rid
            query = "CREATE EDGE Attended FROM " + current_patient_rid + " TO " + event_rid
            client.command(query)

    print("GOOD TO GO")
    client.db_close()

# reset_graphDB()

entries = [
    {
        "patient_mrn": "1234sagsdgdsfg-sadasd",
        "contact_list": ["2432532ghdfddfd", "dsadsfgewrw"],
        "event_list": ["1243fdgdfg"]
    },
    {
        "patient_mrn": "1234sagsdgdsfg-sadasa",
        "contact_list": ["1234sagsdgdsfg-sadasd"],
        "event_list": ["1243fdgdfg"]
    }
]
# insert_into_graph(entries)
# get_graph_event_contacts("1234sagsdgdsfg-sadasd")