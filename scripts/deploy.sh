# Script to do a maven build and submit a job to Spark cluster
rm ./target/*.jar
JAR_NAME=original-schemainfer-0.0.1-shaded.jar
GCS_JAR_ARTIFACT_BUCKET=gs://dataproc-temp-us-central1-21673414111-z46je66v/artifacts/scanseq
GCS_BUCKET_DATA_OUT=gs://dataproc-temp-us-central1-21673414111-z46je66v/sampledata/out4
GCS_BUCKET_DATA_OUT2=gs://dataproc-temp-us-central1-21673414111-z46je66v/sampledata/out4
INPUT_DATA=gs://schema-inference-sample-data/internal__legs_gameevents/dt=2020-05-15/h=06/batchid=190936cc-84d9-45f9-af54-81de9f460ee2/000000_0
REGION_NAME=us-central1
CLUSTER_NAME=cluster-b546

mvn package -DskipTests=true

gsutil rm $GCS_JAR_ARTIFACT_BUCKET/$JAR_NAME
#gsutil rm -r $GCS_BUCKET_DATA_OUT
#gsutil rm -r $GCS_BUCKET_DATA_OUT2
gsutil cp ./target/$JAR_NAME $GCS_JAR_ARTIFACT_BUCKET

gcloud dataproc jobs submit spark \
--cluster $CLUSTER_NAME  \
--properties spark:spark.executor.memory=64g \
--jars gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
--region $REGION_NAME \
--jar $GCS_JAR_ARTIFACT_BUCKET/$JAR_NAME \
-- -m cluster -i $INPUT_DATA \
-s true -o schema-inference-out -ds schema_infer -t schema-inference-out -tb proto_schema4

