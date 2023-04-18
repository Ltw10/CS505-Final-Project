import pyorient
from pyorient import OrientDB
from pyorient.ogm import Graph

class GraphDBEngine:

    def __init__(self):
        # Connect to OrientDB server
        orient = pyorient.OrientDB("localhost", 2424)
        session = orient.connect("admin", "rootpwd")
        
        # Create a new database
        session.db_create("cs505graph", pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_PLOCAL)
        
        # Open the database
        db = Graph(session, "cs505graph", "admin", "rootpwd")
        
        self.clear_db(db)
        
        # Create vertex and edge classes
        patient = db.create_vertex_class("patient")
        contact_with = db.create_edge_class("contact_with")

        patient_0 = self.create_patient(db, "mrn_0")
        patient_1 = self.create_patient(db, "mrn_1")
        patient_2 = self.create_patient(db, "mrn_2")
        patient_3 = self.create_patient(db, "mrn_3")

        # Patient 0 in contact with Patient 1
        edge1 = db.create_edge(patient_0, patient_1, "contact_with")
        edge1.save()
        
        # Patient 2 in contact with Patient 0
        edge2 = db.create_edge(patient_2, patient_0, "contact_with")
        edge2.save()

        # Patient 3 in contact with Patient 2
        edge3 = db.create_edge(patient_3, patient_2, "contact_with")
        edge3.save()

        self.get_contacts(db, "mrn_0")

        session.db_close()

    def create_patient(self, db, patient_mrn):
        patient = db.create_vertex("patient")
        patient.patient_mrn = patient_mrn
        patient.save()
        return patient

    def get_contacts(self, db, patient_mrn):
        query = f"TRAVERSE in(), out(), inV(), outV() FROM (SELECT FROM patient WHERE patient_mrn = '{patient_mrn}') WHILE $depth <= 2"
        results = db.query(query)
        
        for item in results:
            print("Contact: " + item.patient_mrn)

    def clear_db(self, db):
        query = "DELETE VERTEX FROM patient"
        db.command(query)

graph_db_engine = GraphDBEngine()