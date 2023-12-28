from google.cloud import bigquery
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# defining bigquery client and topic path
bq_client = bigquery.Client(project='plated-hash-405319')

def upload_to_bq(json_data, project_id, dataset_id, table_id):

    logger.info(f"Execution of upload_to_bq has been started")

    logger.info(f"inside xml_to_json_comv functionx, step1")
    # convert the json string to a list of dictionaries
    data = [json.loads(line) for line in json_data]

    print(f"printing the json data which will be published to BQ table{data}")

    # create a BQ table reference
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    logger.info(f"inside xml_to_json_comv functionx, step3")

    # configuration for loading data  into Bigquery
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    logger.info(f"inside xml_to_json_comv functionx, step4")

    # Load data into BigQuery table
    load_job = bq_client.load_table_from_json(data, table_ref, job_config=job_config)
    
    logger.info(f"inside xml_to_json_comv functionx, step5")

    load_job.result()

    print(f"Data loaded to {project_id}.{dataset_id}.{table_id}")