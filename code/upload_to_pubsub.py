from google.cloud import pubsub_v1
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# defining pubsub client and topic path
pubsub_client = pubsub_v1.PublisherClient()
topic_path = pubsub_client.topic_path("plated-hash-405319","pubsub_4_wf_4_tf_buk_2_pub_big")

def upload_to_pubsub(xml_content_pb):
    logger.info(f"############# Execution of upload_to_pubsub is starting #############")
    
    print(f"xml content to be published in pubsub: {xml_content_pb}")
    message_future = pubsub_client.publish(topic_path,data=xml_content_pb.encode("utf-8"))
    
    print("############# xml content has been published to pubsub #############")