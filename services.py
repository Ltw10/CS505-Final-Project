from sqlitedb import create_connection, reset_sqlite_db

zip_positive_map = {}
zip_alert_list = []

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

def retrieve_zip_alert_list():
    return zip_alert_list

def reset_dbs():
    reset_sqlite_db()


