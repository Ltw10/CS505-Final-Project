#!/usr/bin/env python
import pika
import sys
import json
from sqlitedb import insert_into_sqlite

def main():

    # Set the connection parameters to connect to rabbit-server1 on port 5672
    # on the / virtual host using the username "guest" and password "guest"

    username = 'team_19'
    password = 'myPassCS505'
    hostname = 'vbu231.cs.uky.edu'
    virtualhost = '19'


    credentials = pika.PlainCredentials(username, password)
    parameters = pika.ConnectionParameters(hostname,
                                            9099,
                                            virtualhost,
                                            credentials) #Try ports 5672 or 9099

    connection = pika.BlockingConnection(parameters)

    patient_channel = create_patient_list_channel(connection)
    hospital_channel = create_hospital_list_channel(connection)
    vax_channel = create_vax_list_channel(connection)

    print(' [*] Waiting for logs. To exit press CTRL+C')
    
    patient_channel.start_consuming()
    hospital_channel.start_consuming()
    vax_channel.start_consuming()

def create_patient_list_channel(connection):

    patient_channel = connection.channel()

    #set the appropirate channel here, there could be more than one, requiring seperate instances.
    exchange_name = 'patient_list'
    patient_channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

    result = patient_channel.queue_declare('', exclusive=True)
    queue_name = result.method.queue

    binding_keys = "#"

    if not binding_keys:
        sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
        sys.exit(1)

    for binding_key in binding_keys:
        patient_channel.queue_bind(
            exchange=exchange_name, queue=queue_name, routing_key=binding_key)


    def callback_patient(ch, method, properties, body):
        print(" [x] %r:%r" % (method.routing_key, body))

        testing_data = json.loads(body)
        for test in testing_data:
            print("*Python Class - Patient Data*")
            print("\ttesting_id: " + str(test['testing_id']))
            print("\tpatient_name: " + str(test['patient_name']))
            print("\tpatient_mrn: " + str(test['patient_mrn']))
            print("\tpatient_zipcode: " + str(test['patient_zipcode']))
            print("\tpatient_status: " + str(test['patient_status']))
            print("\tcontact_list: " + str(test['contact_list']))
            print("\tevent_list: " + str(test['event_list']))

            insert_into_sqlite(test)



    patient_channel.basic_consume(
        queue=queue_name, on_message_callback=callback_patient, auto_ack=True)

    return patient_channel

def create_hospital_list_channel(connection):

    hospital_channel = connection.channel()

    #set the appropirate channel here, there could be more than one, requiring seperate instances.
    exchange_name = 'hospital_list'
    hospital_channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

    result = hospital_channel.queue_declare('', exclusive=True)
    queue_name = result.method.queue

    binding_keys = "#"

    if not binding_keys:
        sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
        sys.exit(1)

    for binding_key in binding_keys:
        hospital_channel.queue_bind(
            exchange=exchange_name, queue=queue_name, routing_key=binding_key)


    def callback_hospital(ch, method, properties, body):
        print(" [x] %r:%r" % (method.routing_key, body))

        testing_data = json.loads(body)
        for test in testing_data:
            print("*Python Class - Hospital Data*")
            print("\thospital_id: " + str(test['hospital_id']))
            print("\tpatient_name: " + str(test['patient_name']))
            print("\tpatient_status: " + str(test['patient_status']))



    hospital_channel.basic_consume(
        queue=queue_name, on_message_callback=callback_hospital, auto_ack=True)

    return hospital_channel

def create_vax_list_channel(connection):

    vax_channel = connection.channel()

    #set the appropirate channel here, there could be more than one, requiring seperate instances.
    exchange_name = 'vax_list'
    vax_channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

    result = vax_channel.queue_declare('', exclusive=True)
    queue_name = result.method.queue

    binding_keys = "#"

    if not binding_keys:
        sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
        sys.exit(1)

    for binding_key in binding_keys:
        vax_channel.queue_bind(
            exchange=exchange_name, queue=queue_name, routing_key=binding_key)


    def callback_vax(ch, method, properties, body):
        print(" [x] %r:%r" % (method.routing_key, body))

        testing_data = json.loads(body)
        for test in testing_data:
            print("*Python Class - Vax List*")
            print("\tvaccination_id: " + str(test['vaccination_id']))
            print("\tpatient_name: " + str(test['patient_name']))
            print("\tpatient_mrn: " + str(test['patient_mrn']))



    vax_channel.basic_consume(
        queue=queue_name, on_message_callback=callback_vax, auto_ack=True)

    return vax_channel


main()