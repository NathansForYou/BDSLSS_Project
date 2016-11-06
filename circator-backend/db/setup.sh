#!/bin/bash

if [ $# -ne 3 ]; then
  echo "Usage: $0 <hostname> <db user> <db name>"
  exit 1
fi

HOST=$1
USER=$2
DB=$3

# Clean all objects in the database.
psql "host=$HOST user=$USER dbname=$DB sslmode=require"  <<EOF
  DROP SCHEMA public CASCADE;
  CREATE SCHEMA public;
  GRANT ALL ON SCHEMA public TO postgres;
  GRANT ALL ON SCHEMA public TO public;
  COMMENT ON SCHEMA public IS 'standard public schema';
EOF

for i in init measures measures_tasks measures_udf measures_etl measures_ivm nhanes; do
  psql -f "$i.sql" "host=$HOST user=$USER dbname=$DB sslmode=require"
done

if [ -d nhanes_data ]; then
  ## Run from the <repo>/db directory.
  NHANES_DATA_DIR=`pwd | sed 's/^\/c/C\:/'`/nhanes_data
  sed "s#@@PATH@@#$NHANES_DATA_DIR#" < nhanes_load.sql > nhanes_load.run.sql
  psql -f nhanes_load.run.sql "host=$HOST user=$USER dbname=$DB sslmode=require"
else
  echo "Skipping NHANES data ingestion... dataset not found."
  echo "Please make sure you are running from the circator-backend/db directory."
fi

psql -c "vacuum" "host=$HOST user=$USER dbname=$DB sslmode=require"
psql -c "vacuum analyze" "host=$HOST user=$USER dbname=$DB sslmode=require"
