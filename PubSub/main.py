import base64

import functions_framework
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()
table_id = "supple-lock-385512.analytics_us.pubsub_messages"


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def my_pubsub_function(cloud_event):
    print(cloud_event)
    # Print out the data from Pub/Sub, to prove that it worked
    message = base64.b64decode(cloud_event.data["message"]["data"]).decode()

    rows_to_insert = [
        {"message": message}
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

