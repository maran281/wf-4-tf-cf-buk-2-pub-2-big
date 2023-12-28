import json
import xmltodict
import logging
import xml.etree.ElementTree as ET

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def xml_to_json_conv(xml_content_bq):
    # parse xml to json dictionary
    logger.info(f"############# Execution of xml_to_json_conv has been started #############")
    print(f"Xml data which will be processed is: {xml_content_bq}")

    data_dict = xmltodict.parse(xml_content_bq)

    # Extract relevent data structure from dictionary
    catalog = data_dict.get('book', {})
    books = catalog if isinstance(catalog,list) else [catalog]

    # convert each book to JSONL entry
    jsonl_entries = []
    print("### Entering the for loop ###")
    for book_var in books:
        print("inside loop")
        jsonl_entry = json.dumps(book_var)
        jsonl_entries.append(jsonl_entry)
    print("### Exiting the for loop ###")
    print(f"############# value of the formated json data is: {jsonl_entries} ############# ")
    return jsonl_entries