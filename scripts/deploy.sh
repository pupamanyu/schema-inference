
JAR_NAME=protoudf-0.0.1-jar-with-dependencies.jar
GCS_JAR_ARTIFACT_BUCKET=gs://dataproc-temp-us-central1-21673414111-z46je66v/artifacts/scanseq
GCS_BUCKET_DATA_OUT=gs://dataproc-temp-us-central1-21673414111-z46je66v/sampledata/out2
REGION_NAME=us-central1
CLUSTER_NAME=cluster-487a

mvn package -DskipTests=true

gsutil rm $GCS_JAR_ARTIFACT_BUCKET/$JAR_NAME
gsutil rm -r $GCS_BUCKET_DATA_OUT
gsutil cp ./target/$JAR_NAME $GCS_JAR_ARTIFACT_BUCKET

gcloud dataproc jobs submit spark \
--cluster $CLUSTER_NAME  \
--region $REGION_NAME \
--jar $GCS_JAR_ARTIFACT_BUCKET/$JAR_NAME
