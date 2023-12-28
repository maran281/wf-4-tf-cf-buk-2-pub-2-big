import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def upload_to_gcs(t_bucket_ref, t_f_name, local_file_path):
    logger.info(f"Execution of upload_to_gcs has been started")
    target_blob = t_bucket_ref.blob(t_f_name)
    target_blob.upload_from_filename(local_file_path)