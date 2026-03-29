#!/bin/bash
set -e
set -u

# Quick connect to diagnostic PostgreSQL via psql.
docker exec -it diagnostic-postgres psql -U dba -d ecommerce_db
