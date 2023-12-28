import os
import tempfile
import logging
import xml.etree.ElementTree as ET
from upload_to_pubsub import upload_to_pubsub
from xml_to_json_conv import xml_to_json_conv
from upload_to_gcs import upload_to_gcs
from upload_to_bq import upload_to_bq
from google.cloud import storage

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
            "author": element.find("author").text,
            "genre": element.find('genre').text,
            "price": float(element.find('price').text),
            "publish_date": element.find('publish_date').text,
            "description": element.find('description').text,
            "Age_Criteria": element.find('Age_Criteria').text,
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
        
        file_counter = publish_xml_gcs(file_counter, book_xml)
        # Write xml content into an xml file and publishe it to cloud storage

    return f"success"

def extract_xml_content(s_bucket, s_f_name):
    bucket_ref = storage_client.bucket(s_bucket)
    source_blob = bucket_ref.blob(s_f_name)
    xml_content = source_blob.download_as_text()

    return xml_content

def publish_xml_gcs(f_counter, b_xml):
    target_file_name="xml_file_processed_"+f"{f_counter}"+".xml"
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
    return f_counter



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