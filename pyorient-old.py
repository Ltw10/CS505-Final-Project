
import json
import re

def create_db(client, name):

   # Remove Old Database
    if client.db_exists(name):
        print("db exists")
    else:
        client.db_create(name,
            pyorient.DB_TYPE_GRAPH,
            pyorient.STORAGE_TYPE_PLOCAL)
        client.command("CREATE CLASS mrn EXTENDS V")
        client.command("CREATE PROPERTY mrn.contacts String")
        client.command("CREATE PROPERTY Person.events String")

def getrid(client,mrn):
    nodeId = client.query("SELECT FROM V WHERE mrn = '" + str(mrn) + "'")
    return str(nodeId[0]._rid)

def createDB():
    #database name
    dbname = "cs505graph.db"
    #database login is root by default
    login = "root"
    #database password, set by docker param
    password = "rootpwd"

    #create client to connect to local orientdb docker container
    client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect(login, password)

    #create database
    create_db(client,dbname)

    #open the database we are interested in
    client.db_open(dbname, login, password)

    return client

def insert_into_graphDB(client, data):
    #loop through each key in the json database and create a new vertex, V with the mrn in the database
    for key in data:
        mrn = re.sub("'", "", data.get(key).get("patient_mrn"))
        contacts = re.sub("'", "", data.get(key).get("contact_list"))
        events = re.sub("'", "", data.get(key).get("event_list"))

        #print("CREATE VERTEX Person SET patient_mrn = '" + mrn + "', contact_list = '" + contacts + "', event_list = '" + events + "'")
        client.command("CREATE VERTEX Person SET patient_mrn = '" + mrn + "', contact_list = '" + contacts + "', event_list = '" + events + "'")

    #loop through each key creating edges from contacts to contacted and events both attended
    for key in data:
        patientNodeMRN = str(getrid(client,key))
        for mrnID in data.get(key)["patient_mrn"]:
            attended_event = str(getrid(client,mrnID))
            client.command("CREATE EDGE FROM " + patientNodeMRN + " TO " + attended_event)

    client.close()

def get_confirmed_contacts(mrn):

    #personNodeMRN is the V id, which matches the patient_mrn in the JSON file
    #personNodeId[A/B] is the internal OrientDB node ids (rid), which are used for functions like shortestPath

    dbname = "cs505graph.db"
    login = "root"
    password = "rootpwd"

    client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect(login, password)

    client.db_open(dbname, login, password)

    #get the RID of the person
    personNodeMRN = getrid(client,mrn)

    pathlist = client.command("SELECT " + personNodeMRN + "")

    client.close()
    return distance

def get_possible_contacts(mrn):
    #personId[A/B] is the V id, which matches the id in the JSON file
    #personNodeId[A/B] is the internal OrientDB node ids (rid), which are used for functions like shortestPath

    dbname = "cs505graph.db"
    login = "root"
    password = "rootpwd"

    client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect(login, password)

    client.db_open(dbname, login, password)

    #get the RID of the two people
    personNodeIdA = getrid(client,personIdA)
    personNodeIdB = getrid(client,personIdB)

    #determine the shortest path
    pathlist = client.command("SELECT shortestPath(" + personNodeIdA + ", " + personNodeIdB +")")
    #print(pathlist[0].__getattr__('shortestPath'))

    #get distance
    distance = len(pathlist[0].__getattr__('shortestPath'))

    for node in pathlist[0].__getattr__('shortestPath'):
        print(node)

    #pyorient.otypes.OrientRecord
    client.close()
    return distance