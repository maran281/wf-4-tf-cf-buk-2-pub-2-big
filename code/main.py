import os
import tempfile
import xml.etree.ElementTree as ET
from google.cloud import pubsub_v1, storage, bigquery

file_counter = 1

#defining storage client
storage_client = storage.Client()
target_bucket_ref = storage_client.bucket('bucket_targetfile_4_wf_4_tf_buk_2_pub_big')

def publish_message(data, context):

    #defining pubsub client and topic path
    pubsub_client = pubsub_v1.PublisherClient()
    topic_path = pubsub_client.topic_path("plated-hash-405319","pubsub_4_wf_4_tf_buk_2_pub_big")
    
    #defining bigquery client
    bigquery_client = bigquery.Client()

    #Fetching metadata after function triggers
    source_file_name = data['name']
    source_bucket = data['bucket']
    print(f"A file named:{source_file_name} is picked from bucket named:{source_bucket}")

    #get the content of the xml file
    bucket_ref = storage_client.bucket(source_bucket)
    source_blob = bucket_ref.blob(source_file_name)
    content = source_blob.download_as_text()

    #Reading an XML content from the file one by one
    root = ET.fromstring(content)

    # below for loop checks all the xml tags with name 'book', one by one 
    # and storing its content to 'element' variable, 
    # then we are fetching the key, value from 'element' 
    # and storing it into 'row_date'
    for element in root.findall('.//book'): 
        row_data={
            "id": element.get("id"),
            "author": element.get("author"),
            "title": element.find('title').text,
            "genre": element.find('genre').text,
            "price": float(element.find('price').text),
            "publish_date": element.find('publish_date').text,
            "description": element.find('description').text,
        }

  #Creating an xml content for each 'book' tag in the source xml
  # and storing it into a variable 'book_xml' 
        book_xml="<book>"
        for key,value in row_data.items():
            book_xml += f"<{key}>{value}</{key}>"
        book_xml+="</book>"

        #publish xml content to pubsub
        message_future = pubsub_client.publish(topic_path,data=book_xml.encode("utf-8"))
        print("xml content has been published to pubsub")
        print(f"{book_xml}")
    
  #Write xml content into an xml file and publishe it to cloud storage
        target_file_name="xml_file_processed_"+f"{file_counter}"+".xml"
        print(f"Target file name would be {target_file_name}")
  
  #we are using tempfile(python library) to create a temporary file WITHIN THIS INSTANCE MEMORY 
  # and storing the xml content from 'book_xml' into that temp file
  
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(book_xml.encode("utf-8"))

        # Upload the temporary file to Google Cloud Storage
        upload_to_gcs(target_bucket_ref, target_file_name, temp_file.name)

        # Clean up the temporary file (optional)
        os.remove(temp_file.name)

        file_counter = file_counter + 1
    return f"success"

def upload_to_gcs(t_bucket_ref, t_f_name, local_file_path):
    target_blob = t_bucket_ref.blob(t_f_name)
    target_blob.upload_from_filename(local_file_path)

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