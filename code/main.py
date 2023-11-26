import os
import json
import functions_framework
import xml.etree.ElementTree as ET
from google.cloud import pubsub_v1, storage, bigquery

def publish_message(data, context):

    #defining all clients
    storage_client = storage.Client() 
    pubsub_client = pubsub_v1.PublisherClient()
    bigquery_client = bigquery.Client()

    file_name = data['name']
    bucket_name = data['bucket']
    print(f"A file named:{file_name} is picked from bucket named:{bucket_name}")

    #get the content of the xml file
    print("debug1")
    bucket = storage_client.bucket(bucket_name)
    print("debug2")
    blob = bucket.blob(file_name)
    print("debug3")
    content = blob.download_as_text()
    print("debug4")

    root = ET.fromstring(content)
    print("debug5")

    rows = [] 
    print("debug6")

    for element in root.findall('.//book'): 
        print("debug7")  
        print(f"{element.text}")  
        print("debug8")  
        row_data={
            "book": element.find('book').text
        }
        print("debug8")
        rows.append(row_data)
        print("debug9")

    for row in rows:
        print("debug10")
        message_data = str(row)
        print(f"{message_data}")
  
    return f"success"

#below is a working code which triggers the cloud function with a https trigger
#def publish_message(request):
#    #get the incoming data from http request
#    print("Debug 1")
#    request_json = request.get_json()
#    print("Debug 2")
#    print(request_json)
#    print("Debug 9")
#   topic_name = request_json.get("topic", "default topic name")
#    print("Debug 3")
#    message_data = request_json.get("message","default message content")
#    print("Debug 4")
#
#    #Create a pub/sub client
#    publisher = pubsub_v1.PublisherClient()
#    print("Debug 5")
#
#    #Create a topic name
#    topic_path = publisher.topic_path("plated-hash-405319", "pubsub_4_wf_4_tf_buk_2_pub_big")
#    print("Debug 6")
#
#    #publish message to a topic
#    future = publisher.publish(topic_path, data=message_data.encode("utf-8"))
#    print("Debug 7")
#    message_id = future.result
#    print("Debug 8")
#
#    return f"Message {message_id} is published to topic {topic_name}"
#