from flask import Flask
import threading
from subscriber import start_subscriber
from services import retrieve_state_status, retrieve_zip_alert_list, reset_dbs, generate_hospital_report, generate_overall_report
from sqlitedb import create_patient_data_table, create_hospital_data_table, create_vax_data_table
from pyorientServices import get_graph_contacts, get_graph_event_contacts

app = Flask(__name__)

create_patient_data_table()
create_hospital_data_table()
create_vax_data_table()

subscriber_thread = threading.Thread(target=start_subscriber, daemon=True)
subscriber_thread.start()

team = {
    "team_name": "It's Data Time",
    "team_members_sids": [12295705, 12365153, 12406612],
    "app_status_code": 1
}

@app.get("/api/getteam")
def get_team():
    return team

@app.get("/api/reset")
def reset():
    reset_status = reset_dbs()
    return {"reset_status_code": reset_status}

@app.get("/api/zipalertlist")
def get_zip_alert_list():
    zip_list = retrieve_zip_alert_list()
    return {"ziplist": zip_list}

@app.get("/api/alertlist")
def get_state_alert_status():
    state_status = retrieve_state_status()
    return {"state_status": state_status}

@app.get("/api/getconfirmedcontacts/<mrn>")
def get_confirmed_contacts(mrn):
    contact_list = get_graph_contacts(mrn)
    return {"contactlist": contact_list}

@app.get("/api/getpossiblecontacts/<mrn>")
def get_possible_contacts(mrn):
    possible_contacts = get_graph_event_contacts(mrn)
    return {"contactlist": possible_contacts}

@app.get("/api/getpatientstatus/<hospital_id>")
def get_patient_status(hospital_id):
    hospital_report = generate_hospital_report(hospital_id)
    return hospital_report

@app.get("/api/getpatientstatus")
def get_all_patient_status():
    hospitals_report = generate_overall_report()
    return hospitals_report

if __name__ == "__main__":
    app.run(host="0.0.0.0",port=9999)