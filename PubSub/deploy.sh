gcloud functions deploy my-pubsub-function \
--gen2 \
--region=us-east4 \
--runtime=python39 \
--memory=512MB \
--entry-point=my_pubsub_function \
--trigger-topic=sensorReading
