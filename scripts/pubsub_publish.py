import base64, json, sys
from google.cloud import pubsub_v1

PROJECT = "project-36a10255-b110-4164-8f8"
TOPIC = "wtt-live"
DATA_PATH = "cloudrun/Publish on Pub&Sub.json"

def main():
    with open(DATA_PATH, "r") as f:
        payload = json.load(f)
    data = base64.b64encode(json.dumps(payload).encode("utf-8"))
    pub = pubsub_v1.PublisherClient()
    topic_path = pub.topic_path(PROJECT, TOPIC)
    future = pub.publish(topic_path, data)
    print("published:", future.result())

if __name__ == "__main__":
    sys.exit(main())
