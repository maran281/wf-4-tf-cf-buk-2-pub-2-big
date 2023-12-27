import os
import tempfile
import json
import xmltodict
import logging
import xml.etree.ElementTree as ET
from google.cloud import pubsub_v1, storage, bigquery

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# defining storage client
storage_client = storage.Client()
target_bucket_ref = storage_client.bucket('bucket_targetfile_4_wf_4_tf_buk_2_pub_big')

# defining pubsub client and topic path
pubsub_client = pubsub_v1.PublisherClient()
topic_path = pubsub_client.topic_path("plated-hash-405319","pubsub_4_wf_4_tf_buk_2_pub_big")

# defining bigquery client and topic path
bq_client = bigquery.Client(project='plated-hash-405319')

def publish_message(data, context):
    
    #Fetching metadata after function triggers
    source_file_name = data['name']
    source_bucket = data['bucket']
    print(f"A file named:{source_file_name} is picked from bucket named:{source_bucket}")

    # get the content of the xml file
    bucket_ref = storage_client.bucket(source_bucket)
    source_blob = bucket_ref.blob(source_file_name)
    content = source_blob.download_as_text()

    # Reading an XML content from the file one by one
    root = ET.fromstring(content)

    file_counter = 1
    
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

        # Creating an xml content for each 'book' tag in the source xml
        # and storing it into a variable 'book_xml'
        book_xml="<book>"
        for key,value in row_data.items():
            book_xml += f"<{key}>{value}</{key}>"
        book_xml+="</book>"

        # publish xml content to pubsub
        upload_to_pubsub(book_xml)

        # Convert xml data into jsonl format
        logger.info(f"starting xml to json conversion")
        json_conv_data = xml_to_json_conv(book_xml)

        # publish json string to bigquery dataset
        upload_to_bq(json_conv_data, 'plated-hash-405319', 'bq_dataset_4_wf_4_tf_buk_2_pub_big_id', 'bq_table_4_wf_4_tf_buk_2_pub_big')
    
        # Write xml content into an xml file and publishe it to cloud storage
        target_file_name="xml_file_processed_"+f"{file_counter}"+".xml"
        print(f"Target file name would be {target_file_name}")
  
        # we are using tempfile(python library) to create a temporary file WITHIN THIS INSTANCE MEMORY 
        # and storing the xml content from 'book_xml' into that temp file

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(book_xml.encode("utf-8"))

        # Upload the temporary file to Google Cloud Storage
        upload_to_gcs(target_bucket_ref, target_file_name, temp_file.name)

        # Clean up the temporary file (optional)
        os.remove(temp_file.name)

        file_counter = file_counter + 1
    return f"success"

def upload_to_pubsub(xml_content_pb):
    message_future = pubsub_client.publish(topic_path,data=xml_content_pb.encode("utf-8"))
    print("xml content has been published to pubsub")
    print(f"{xml_content_pb}")

def upload_to_gcs(t_bucket_ref, t_f_name, local_file_path):
    target_blob = t_bucket_ref.blob(t_f_name)
    target_blob.upload_from_filename(local_file_path)

def xml_to_json_conv(xml_content_bq):
    # parse xml to json dictionary
    logger.info(f"inside xml_to_json_comv functionx, step1")
    
    data_dict = xmltodict.parse(xml_content_bq)
    logger.info(f"inside xml_to_json_comv functionx, step2")

    # Extract relevent data structure from dictionary
    catalog = data_dict.get('catalog', {})
    logger.info(f"inside xml_to_json_comv functionx, step3")
    books = catalog.get('book',[])
    logger.info(f"inside xml_to_json_comv functionx, step4")

    # convert each book to JSONL entry
    jsonl_entries = []
    logger.info(f"inside xml_to_json_comv functionx, step5")
    for book_var in books if isinstance(books, list) else [books]:
        jsonl_entry = json.dumps({"catalog": {"book": book_var}})
        jsonl_entries.append(jsonl_entry)
    logger.info(f"inside xml_to_json_comv functionx, step6")
    return jsonl_entries

def upload_to_bq(json_data, project_id, dataset_id, table_id):
    # convert the json string to a list of dictionaries
    data = [json.loads(line) for line in json_data.split("\n") if line.strip()]

    # create a BQ table reference
    table_ref = bq_client.dataset(dataset_id).table(table_id)

    # configuration for loading data  into Bigquery
    job_config = bigquery.LoadJobConfig(
        #autodetect=True,  # Automatically detect schema
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    # Load data into BigQuery table
    load_job = bq_client.load_table_from_json(data, table_ref, job_config=job_config)
    load_job.result()

    print(f"Data loaded to {project_id}.{dataset_id}.{table_id}")
    

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