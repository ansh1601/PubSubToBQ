# PubSubToBQ

Hello , this project is used for building a DataFlow pipeline to read message from PubSub topic and check for its validity according to the schema of Big Query table.
If the message is valid, put it into bigquery table and invalid message should be forwarded to dead letter topics.

This project contains both maven and gradle build files.

#enter the following command on the command line to run.

export LAB_ID=18
export MAIN_CLASS_NAME=StreamingPipeline
export PROJECT_ID=$(gcloud config get-value project)
export OutputTableName=${PROJECT_ID}:uc1_${LAB_ID}.account
export dlqTopic=projects/nttdata-c4e-bde/topics/uc1-dlq-topic-${LAB_ID}
export subscription=projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-18

mvn compile exec:java -Dexec.mainClass=${MAIN_CLASS_NAME} -Dexec.args="--subscription=projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-18 --outputTableName=uc1_18.account --dlqTopic=projects/nttdata-c4e-bde/topics/uc1-dlq-topic-18 --project=$PROJECT_ID --jobName=usecase1-labid-${LAB_ID} --region=europe-west4 --serviceAccount=c4e-uc1-sa-${LAB_ID}@nttdata-c4e-bde.iam.gserviceaccount.com  --maxNumWorkers=1 --workerMachineType=n1-standard-1 --gcpTempLocation=gs://c4e-uc1-dataflow-temp-${LAB_ID}/temp --stagingLocation=gs://c4e-uc1-dataflow-temp-${LAB_ID}/staging --subnetwork=regions/europe-west4/subnetworks/subnet-uc1-${LAB_ID} --runner=DataflowRunner --streaming=true"



