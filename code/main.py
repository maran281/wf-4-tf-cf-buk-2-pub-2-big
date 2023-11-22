import os
import json
import functions_framework
from google.cloud import pubsub_v1

@functions_framework.http

def publish_message(request):
    #get the incoming data from http request
    print("Debug 1")
    request_json = request.get_json()
    print("Debug 2")
    print(request_json)
    print("Debug 9")
    topic_name = request_json.get("topic", "default topic name")
    print("Debug 3")
    message_data = request_json.get("message","default message content")
    print("Debug 4")

    #Create a pub/sub client
    publisher = pubsub_v1.PublisherClient()
    print("Debug 5")

    #Create a topic name
    topic_path = publisher.topic_path("plated-hash-405319", "pubsub_4_wf_4_tf_buk_2_pub_big")
    print("Debug 6")

    #publish message to a topic
    future = publisher.publish(topic_path, data=message_data.encode("utf-8"))
    print("Debug 7")
    message_id = future.result
    print("Debug 8")
    
    return f"Message {message_id} is published to topic {topic_name}"
