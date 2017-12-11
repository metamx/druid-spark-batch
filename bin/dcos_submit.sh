#!/usr/bin/shs

set -x

# Define target environment
if [ "$environment" = "staging" ]; then SUFFIX="-staging"; fi


# SPARK_DCOS_URL the spark service url
# SPARK_MASTER spark master url, including port
# SPARK_ARGS spark arguments, as a json array's content
# SPARK_JARS list of urls where to download jars from
# SPARK_CLASS_NAME class name of the spark app
# SPARK_APP_NAME spark app name for display
# SPARK_DRIVER_CORES driver cores
# SPARK_DRIVER_MEMORY driver memory
# SPARK_CORES_MAX max available cores on mesos
# SPARK_EXECUTOR_MEMORY executor memory
# SPARK_CASSANDRA_HOST cassandra host name
# SPARK_CASSANDRA_USER cassandra user name
# SPARK_CASSANDRA_PASSWORD cassandra password
# SPARK_POSTGRES_HOST postgres host
# SPARK_POSTGRES_USER postgres user
# SPARK_POSTGRES_PASSWORD postgres password

SPARK_DCOS_URL="mydcosurl"
SPARK_MASTER="mysparkmaster"
SPARK_ARGS=""
SPARK_JARS="myjarurl"
SPARK_CLASS_NAME="$jobclass"
SPARK_APP_NAME="SparkJobs$SUFFIX"
SPARK_DRIVER_CORES="2"
SPARK_DRIVER_MEMORY="1024"
SPARK_CORES_MAX="10"
SPARK_EXECUTOR_MEMORY="2g"
SPARK_CASSANDRA_HOST="xxx"
DCOS_ACS_TOKEN="read acs token"
SPARK_DOCKER_IMAGE="mesosphere/spark:1.1.0-2.1.1-hadoop-2.6"

if [ "$environment" = "staging" ]; then
SPARK_CASSANDRA_USER="xxx"
SPARK_CASSANDRA_PASSWORD="xxxx"
SPARK_POSTGRES_HOST="xxxx"
SPARK_POSTGRES_USER="xxxx"
SPARK_POSTGRES_PASSWORD="xxxx"
else
SPARK_CASSANDRA_USER="xxx"
SPARK_CASSANDRA_PASSWORD="xxxx"
SPARK_POSTGRES_HOST="xxxx"
SPARK_POSTGRES_USER="xxxx"
SPARK_POSTGRES_PASSWORD="xxxx"
fi

generate_payload() {
  cat <<-EOF
{
  "action" : "CreateSubmissionRequest",
  "appArgs" : [ $SPARK_ARGS ],
  "appResource" : "$SPARK_JARS",
  "clientSparkVersion" : "2.1.1",
  "environmentVariables" : {
    "SPARK_ENV_LOADED" : "1"
  },
  "mainClass" : "$SPARK_CLASS_NAME",
  "sparkProperties" : {
    "spark.jars" : "$SPARK_JARS",
    "spark.driver.supervise" : "false",
    "spark.app.name" : "$SPARK_APP_NAME",
    "spark.eventLog.enabled": "true",
    "spark.submit.deployMode" : "cluster",
    "spark.master" : "spark://${SPARK_MASTER}",
    "spark.driver.cores" : "${SPARK_DRIVER_CORES}",
    "spark.driver.memory" : "${SPARK_DRIVER_MEMORY}",
    "spark.cores.max" : "${SPARK_CORES_MAX}",
    "spark.executor.memory" : "${SPARK_EXECUTOR_MEMORY}",
    "spark.postgres.hostname" : "${SPARK_POSTGRES_HOST}",
    "spark.postgres.username" : "${SPARK_POSTGRES_USER}",
    "spark.postgres.password" : "${SPARK_POSTGRES_PASSWORD}",
    "spark.mesos.executor.docker.image": "${SPARK_DOCKER_IMAGE}",
    "spark.driver.supervise": false,
    "spark.ssl.noCertVerification": true
  }
}
EOF
}

acsHeader="Authorization: token=$DCOS_ACS_TOKEN"

submission=$(curl -sS -X POST "${SPARK_DCOS_URL}/v1/submissions/create" --header "$acsHeader" --header "Content-Type:application/json;charset=UTF-8" --data "$(generate_payload)")
submissionSuccess=$(echo $submission | jq -r 'select(.success)')

if [[ $submissionSuccess ]]; then
  SECONDS=0

  submissionId=$(echo $submission | jq -rj '.submissionId')
  # Wait for the job to finish
  until test "$(curl --header "$acsHeader" -sS $SPARK_DCOS_URL/v1/submissions/status/$submissionId | jq -r '.driverState')" != "RUNNING"; do sleep 10; echo "Waiting..."; done
  lastStatus=$(curl --header "$acsHeader" -sS $SPARK_DCOS_URL/v1/submissions/status/$submissionId | jq -r '.')
  echo $lastStatus
  taskState=$(echo $lastStatus | jq -r '.message' | grep state | cut -d " " -f 2)
  taskMessage=$(echo $lastStatus | jq -r '.message' | grep message | cut -d " " -f 2)

  if [ "$taskState" = "TASK_FINISHED" ] || [ "$taskState" = "TASK_OK" ]; then
    echo "Job succeeded."
    echo "$taskMessage"
    DURATION=$SECONDS
    curl -s https://metrics-api.librato.com/v1/metrics -u $LIBRATO_USERNAME:$LIBRATO_TOKEN -H 'Content-Type: application/json' \
      -d "{\"gauges\":{\"spark_jobs.$SPARK_APP_NAME.duration.s\":{\"value\":$DURATION,\"source\":\"jenkins\"}}}"
    exit 0;
  else
    echo "Job failed:"
    echo "$taskMessage"
    # TODO: notify people!
    exit 1;
  fi
else
  echo "Submission failed ! Exiting..."
  echo $submission | jq -r '.message'
  exit 1
fi

