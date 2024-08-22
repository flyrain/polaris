#!/bin/bash
#
# Copyright (c) 2024 Snowflake Computing Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# Run this to open an interactive spark-sql shell talking to a catalog named "manual_spark"
#

REGTEST_HOME=$(dirname $(realpath $0))
cd ${REGTEST_HOME}

export SPARK_VERSION=spark-3.5.2
export SPARK_DISTRIBUTION=${SPARK_VERSION}-bin-hadoop3-scala2.13

./setup.sh

if [ -z "${SPARK_HOME}"]; then
  export SPARK_HOME=$(realpath ~/${SPARK_DISTRIBUTION})
fi

SPARK_BEARER_TOKEN="${REGTEST_ROOT_BEARER_TOKEN:-principal:root;realm:default-realm}"

# If argument 1 doesn't exist, use local filesystem
if [ -z "$1" ]; then
  # Use local filesystem by default
  curl -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" \
       -H 'Accept: application/json' \
       -H 'Content-Type: application/json' \
       http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs \
       -d '{
             "catalog": {
               "name": "manual_spark",
               "type": "INTERNAL",
               "readOnly": false,
               "properties": {
                 "default-base-location": "file:///tmp/polaris/"
               },
               "storageConfigInfo": {
                 "storageType": "FILE",
                 "allowedLocations": [
                   "file:///tmp"
                 ]
               }
             }
           }'
else
  # Check if AWS environment variables are set
  if [ -z "${AWS_TEST_BASE}" ] || [ -z "${AWS_ROLE_ARN}" ]; then
    echo "AWS_TEST_BASE or AWS_ROLE_ARN not set. Please set them to enable S3 integration."
    exit 1
  fi

  # Use S3
  curl -i -X POST -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" \
       -H 'Accept: application/json' \
       -H 'Content-Type: application/json' \
       http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs \
       -d "{
             \"name\": \"manual_spark\",
             \"id\": 100,
             \"type\": \"INTERNAL\",
             \"readOnly\": false,
             \"properties\": {
               \"default-base-location\": \"${AWS_TEST_BASE}\"
             },
             \"storageConfigInfo\": {
               \"storageType\": \"S3\",
               \"allowedLocations\": [\"${AWS_TEST_BASE}/\"],
               \"roleArn\": \"${AWS_ROLE_ARN}\"
             }
           }"
fi

# Add TABLE_WRITE_DATA to the catalog's catalog_admin role since by default it can only manage access and metadata
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/manual_spark/catalog-roles/catalog_admin/grants \
  -d '{"type": "catalog", "privilege": "TABLE_WRITE_DATA"}' > /dev/stderr

# For now, also explicitly assign the catalog_admin to the service_admin. Remove once GS fully rolled out for auto-assign.
curl -i -X PUT -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/principal-roles/service_admin/catalog-roles/manual_spark \
  -d '{"name": "catalog_admin"}' > /dev/stderr

curl -X GET -H "Authorization: Bearer ${SPARK_BEARER_TOKEN}" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://${POLARIS_HOST:-localhost}:8181/api/management/v1/catalogs/manual_spark

echo ${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.catalog.polaris.token="${SPARK_BEARER_TOKEN}"
${SPARK_HOME}/bin/spark-sql -S --conf spark.sql.catalog.polaris.token="${SPARK_BEARER_TOKEN}" \
  --conf spark.sql.catalog.polaris.warehouse=manual_spark \
  --conf spark.sql.defaultCatalog=polaris \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
