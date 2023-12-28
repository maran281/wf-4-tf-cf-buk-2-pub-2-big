import os
import tempfile
import logging
import xml.etree.ElementTree as ET
from upload_to_pubsub import upload_to_pubsub
from xml_to_json_conv import xml_to_json_conv
from upload_to_gcs import upload_to_gcs
from upload_to_bq import upload_to_bq
from google.cloud import storage
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# defining storage client
storage_client = storage.Client()
target_bucket_ref = storage_client.bucket('bucket_targetfile_4_wf_4_tf_buk_2_pub_big')

def publish_message(data, context):
    
    #Fetching metadata after function triggers
    source_file_name = data['name']
    source_bucket = data['bucket']

    logger.info(f"A file named:{source_file_name} is picked from bucket named:{source_bucket}")
    #print(f"A file named:{source_file_name} is picked from bucket named:{source_bucket}")

    # get the content of the xml file
    content = extract_xml_content(source_bucket, source_file_name)

    # Reading an XML content from the file one by one
    root = ET.fromstring(content)

    file_counter = 1
    
    # below for loop checks all the xml tags with name 'book', one by one 
    # and storing its content to 'element' variable, 
    # then we are fetching the value for corresponding keys from 'element' 
    # and storing it into 'row_data'
    for element in root.findall('.//book'): 
        row_data={
            "id": element.get("id"),
            "author": element.find("author").text if element.find("author") is not None else None,
            "genre": element.find('genre').text  if element.find("genre") is not None else None,
            "price": float(element.find('price').text) if element.find("price") is not None else None,
            "publish_date": element.find('publish_date').text if element.find("publish_date") is not None else None,
            "description": element.find('description').text if element.find("description") is not None else None,
            "Age_Criteria": element.find('Age_Criteria').text if element.find("Age_Criteria") is not None else None,
        }

        # Creating an xml content for each 'book' tag in the source xml
        # and storing it into a variable 'book_xml'
        book_xml="<book>"
        for key,value in row_data.items():
            book_xml += f"<{key}>{value}</{key}>"
        book_xml+="</book>"

        # publish xml content to pubsub
        upload_to_pubsub(book_xml)

        # Convert xml data into jsonl format
        json_conv_data = xml_to_json_conv(book_xml)

        # publish json string to bigquery dataset
        upload_to_bq(json_conv_data, 'plated-hash-405319', 'bq_dataset_4_wf_4_tf_buk_2_pub_big_id', 'bq_table_4_wf_4_tf_buk_2_pub_big')
        
        file_counter, file_status = publish_xml_gcs(source_file_name, file_counter, book_xml)

        if file_status == 'success':
            bucket = storage_client.get_bucket(source_bucket)
            blob = bucket.blob(source_file_name)
            if blob.exists:
                blob.delete()
                logger.info(f"processing of file {source_file_name} is completed and file is deleted from the source bucket")
        else:
            logger.info(f"processing of file {source_file_name} is failed")


    return f"success"

def extract_xml_content(s_bucket, s_f_name):
    bucket_ref = storage_client.bucket(s_bucket)
    source_blob = bucket_ref.blob(s_f_name)
    xml_content = source_blob.download_as_text()

    return xml_content

def publish_xml_gcs(s_file_name, f_counter, b_xml):
    current_timestamp = datetime.now()
    target_file_name=f"{s_file_name}_"+f"{f_counter}_"+f"{current_timestamp}"+".xml"
    print(f"Target file name would be {target_file_name}")
  
    # we are using tempfile(python library) to create a temporary file WITHIN THIS INSTANCE MEMORY 
    # and storing the xml content from 'book_xml' into that temp file

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(b_xml.encode("utf-8"))

    # Upload the temporary file to Google Cloud Storage
    upload_to_gcs(target_bucket_ref, target_file_name, temp_file.name)

    # Clean up the temporary file (optional)
    os.remove(temp_file.name)

    f_counter = f_counter + 1 
    result = "success" 
    return f_counter, result



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