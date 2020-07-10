#!/usr/bin/env bash
# Script to do a maven build and submit a job to Spark cluster

usage() {
  echo "Usage:"
  echo "$0: $0 <EXECUTORCORES> <EXECUTORMEMORYGB> <NUMBEROFEXECUTORS> <SPARKDYNAMICALLOCATION-FLAG>"
  exit 1
}

[ $# -ne 4 ] && usage
JAR_NAME=original-schemainfer-0.0.1-shaded.jar
GCS_JAR_ARTIFACT_BUCKET=gs://dataproc-temp-us-central1-21673414111-z46je66v/artifacts/thin
GCS_BUCKET_DATA_OUT=gs://dataproc-temp-us-central1-21673414111-z46je66v/sampledata/out4
GCS_BUCKET_DATA_OUT2=gs://dataproc-temp-us-central1-21673414111-z46je66v/sampledata/out4
GCS_PROTO_DIR="gs://schema-inference-out/${CLUSTERNAME}/protoudf"
BQ_GCS_TEMP_BUCKET_NAME="schema-inference-out"
BQ_DATASET="schema-inference-out"
BQ_TABLENAME="proto_schema5"
INPUT_DATA=gs://schema-inference-sample-data/internal__legs_gameevents/dt=2020-05-15/h=06/batchid=190936cc-84d9-45f9-af54-81de9f460ee2/000000_0
REGION_NAME=us-central1
SPARKEXECUTORCORES=$1
SPARKEXECUTORMEMORYGB=$2
SPARKNUMBEROFEXECUTORS=$3
SPARKDYNAMICALLOCATIONFLAG=$4
SPARKDRIVERMEMORYGB=57
SPARKOPTIONS="spark.dynamicAllocation.enabled=${SPARKDYNAMICALLOCATIONFLAG},spark.shuffle.service.enabled=${SPARKDYNAMICALLOCATIONFLAG},spark.executor.cores=${SPARKEXECUTORCORES},spark.executor.memory=${SPARKEXECUTORMEMORYGB}g,spark.num.executors=${SPARKNUMBEROFEXECUTORS},spark.driver.memory=${SPARKDRIVERMEMORYGB}g"
CLUSTERNAME=schema-${SPARKEXECUTORCORES}cores-${SPARKEXECUTORMEMORYGB}gb-${SPARKNUMBEROFEXECUTORS}-executors-${SPARKDYNAMICALLOCATIONFLAG}

gcloud beta dataproc clusters create ${CLUSTERNAME} --enable-component-gateway --region us-central1 --subnet rdp-data-platform-dev-subnet-01 --no-address \
  --zone us-central1-c \
  --properties "spark:spark.dynamicAllocation.enabled=${SPARKDYNAMICALLOCATIONFLAG},spark:spark.shuffle.service.enabled=${SPARKDYNAMICALLOCATIONFLAG}" \
  --master-machine-type n1-standard-16 --master-boot-disk-type pd-ssd --master-boot-disk-size 1000 --num-master-local-ssds 1 \
  --num-workers 10 \
  --worker-machine-type n1-standard-16 --worker-boot-disk-type pd-ssd --worker-boot-disk-size 1000 --num-worker-local-ssds 1 \
--image-version 1.5-debian10 \
--scopes 'https://www.googleapis.com/auth/cloud-platform' \
--project rdp-data-platform-dev-f8e8

gcloud dataproc jobs submit spark \
  --cluster ${CLUSTERNAME}  \
  --properties ${SPARKOPTIONS} \
  --jars gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
  --region $REGION_NAME \
  --jar $GCS_JAR_ARTIFACT_BUCKET/$JAR_NAME \
  -- -m cluster -i $INPUT_DATA \
  -s true \
  -o ${GCS_PROTO_DIR} \
  -t ${BQ_GCS_TEMP_BUCKET_NAME} \
  -ds ${BQ_DATASET} \
  -tb ${BQ_TABLENAME} \
  -pa ${SPARKNUMBEROFEXECUTORS}
