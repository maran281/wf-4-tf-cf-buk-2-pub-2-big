import os
import json
import functions_framework
import xml.etree.ElementTree as ET
from google.cloud import pubsub_v1, storage, bigquery

def publish_message(data, context):

    #defining all clients
    storage_client = storage.Client() 
    pubsub_client = pubsub_v1.PublisherClient()
    topic_path = pubsub_client.topic_path("plated-hash-405319","pubsub_4_wf_4_tf_buk_2_pub_big")
    bigquery_client = bigquery.Client()

    file_name = data['name']
    bucket_name = data['bucket']
    print(f"A file named:{file_name} is picked from bucket named:{bucket_name}")

    #get the content of the xml file
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text()

    root = ET.fromstring(content)

    rows = [] 

    for element in root.findall('.//book'): 
        print(f"{element.text}")  
        row_data={
            "id": element.get("id"),
            "author": element.get("author"),
            "title": element.find('title').text,
            "genre": element.find('genre').text,
            "price": float(element.find('price').text),
            "publish_date": element.find('publish_date').text,
            "description": element.find('description').text,
        }

  #Convert data into xml format
        book_xml="<book>"
        for key,value in row_data.items():
            book_xml += f"<{key}>{value}</{key}>"
        book_xml+="</book>"

        #publish xml content to pubsub

        print("debug1")

        message_future = pubsub_client.publish(topic_path,data=book_xml.encode("utf-8"))
        print("debug2")
        print("Below xml content has been published")
        print(f"{book_xml}")
  
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