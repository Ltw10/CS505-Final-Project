from flask import Flask
import threading
from subscriber import start_subscriber
from services import retrieve_zip_alert_list, reset_dbs
from sqlitedb import create_patient_data_table, create_hospital_data_table, create_vax_data_table
from test import send_first_wave, send_second_wave

app = Flask(__name__)

create_patient_data_table()
create_hospital_data_table()
create_vax_data_table()

subscriber_thread = threading.Thread(target=start_subscriber, daemon=True)
subscriber_thread.start()
# send_first_wave()
# send_second_wave()

team = {
    "team_name": "It's Data Time",
    "team_members_sids": [12295705, 12365153, 12406612],
    "app_status_code": 0
}

@app.get("/api/getteam")
def get_team():
    return team

@app.get("/api/reset")
def reset():
    reset_dbs()
    return {"reset_status_code": 0}

@app.get("/api/zipalertlist")
def get_zip_alert_list():
    zip_list = retrieve_zip_alert_list()
    return {"ziplist": zip_list}

@app.get("/api/alertlist")
def get_state_alert_status():
    zip_list = retrieve_zip_alert_list()
    if len(zip_list) >= 5:
        state_status = 1
    else:
        state_status = 0
    return {"state_status": state_status}

@app.get("/api/getconfirmedcontacts/<mrn>")
def get_confirmed_contacts(mrn):
    return {"contactlist": []}

@app.get("/api/getpossiblecontacts/<mrn>")
def get_possible_contacts(mrn):
    return {"contactlist": []}

@app.get("/api/getpatientstatus/<hospital_id>")
def get_patient_status(hospital_id):
    return {"insert hospital status here": 0}

@app.get("/api/getpatientstatus")
def get_all_patient_status():
    return {"insert patient status here": 0}