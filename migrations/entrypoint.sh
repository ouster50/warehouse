#!/bin/bash
set -e

echo "Waiting for Cassandra..."
until cqlsh "$CASSANDRA_HOST" -e "DESCRIBE KEYSPACES;" > /dev/null 2>&1; do
  sleep 2
done

echo "Applying schema..."
cqlsh "$CASSANDRA_HOST" -f /schema.cql
echo "Done."
