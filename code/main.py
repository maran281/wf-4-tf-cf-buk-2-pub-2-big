import os
import tempfile
import xml.etree.ElementTree as ET
from google.cloud import pubsub_v1, storage, bigquery

def publish_message(data, context):

    #defining storage client
    storage_client = storage.Client() 

    #defining pubsub client and topic path
    pubsub_client = pubsub_v1.PublisherClient()
    topic_path = pubsub_client.topic_path("plated-hash-405319","pubsub_4_wf_4_tf_buk_2_pub_big")
    
    #defining bigquery client
    bigquery_client = bigquery.Client()

    #Fetching metadata when function triggers
    source_file_name = data['name']
    source_bucket = data['bucket']
    print(f"A file named:{source_file_name} is picked from bucket named:{source_bucket}")

    target_bucket = storage_client.bucket('bucket_targetfile_4_wf_4_tf_buk_2_pub_big')

    #get the content of the xml file
    bucket_ref = storage_client.bucket(source_bucket)
    source_blob = bucket_ref.blob(source_file_name)
    content = source_blob.download_as_text()

    root = ET.fromstring(content)

    file_counter = 1

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
        message_future = pubsub_client.publish(topic_path,data=book_xml.encode("utf-8"))
        print("xml content has been published to pubsub")
        print(f"{book_xml}")
    
  #Write xml content into an xml file and push it to cloud storage
        print("debug2")
        target_file_name="xml_file_processed_"+f"{file_counter}"+".xml"
        print(f"Target file name would be {target_file_name}")
        
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            print("debug3")
            temp_file.write(book_xml.encode("utf-8"))
        
        print("debug4")

        # Upload the temporary file to Google Cloud Storage
        upload_to_gcs(target_bucket, target_file_name, temp_file.name, storage_client)
        print("debug9")

        # Clean up the temporary file (optional)
        os.remove(temp_file.name)
        print("debug10")
    return f"success"

def upload_to_gcs(t_bucket, t_f_name, local_file_path, s_client):
    print("debug5")
    t_bucket_ref = s_client.bucket(t_bucket)
    print("debug6")
    target_blob = t_bucket_ref.blob(t_f_name)
    print("debug7")

    target_blob.upload_from_filename(local_file_path)
    print("debug8")


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