#!/bin/bash
set -e

DATABASES="snowex test"

# psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
#     CREATE USER docker;
#     CREATE DATABASE docker;
#     GRANT ALL PRIVILEGES ON DATABASE docker TO docker;
# EOSQL
# Perform some clean up just in case
for DB in $DATABASES
do
  echo $DB
  # Drop the db if it exists
  dropdb --if-exists --username "$POSTGRES_USER" $DB
done

for DB in $DATABASES
do
  echo $DB
  # Create it
  createdb --username "$POSTGRES_USER" $DB

  # Install postgis
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" $DB -c 'CREATE EXTENSION postgis; CREATE EXTENSION postgis_raster;'

done