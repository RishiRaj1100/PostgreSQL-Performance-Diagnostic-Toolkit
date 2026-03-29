#!/bin/bash
set -e
set -u

# Runs the before/after optimisation demonstration.
# Shows measurable performance improvement from adding indexes.
# This is the key demonstration of EXPLAIN ANALYZE knowledge.

PSQL="docker exec diagnostic-postgres psql -U dba -d ecommerce_db"
mkdir -p results
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTFILE="results/optimisation_${TIMESTAMP}.txt"

run_sql() {
	$PSQL -c "$1" | tee -a "$OUTFILE"
}

echo "OPTIMISATION DEMO - Before/After Index" | tee "$OUTFILE"
echo "=======================================" | tee -a "$OUTFILE"
echo "Output file: $OUTFILE" | tee -a "$OUTFILE"
echo "" | tee -a "$OUTFILE"

echo "Resetting benchmark state (dropping target index if exists)..." | tee -a "$OUTFILE"
run_sql "DROP INDEX IF EXISTS idx_orders_customer_date_status;"
run_sql "ANALYZE orders;"

echo "" | tee -a "$OUTFILE"
echo "BEFORE - Expected Seq Scan (index removed):" | tee -a "$OUTFILE"
run_sql "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT * FROM orders WHERE customer_id = 500 AND status = 'DELIVERED' ORDER BY order_date DESC;"

echo "" | tee -a "$OUTFILE"
echo "Adding composite index..." | tee -a "$OUTFILE"
run_sql "CREATE INDEX IF NOT EXISTS idx_orders_customer_date_status ON orders (customer_id, order_date DESC, status);"
run_sql "ANALYZE orders;"

echo "" | tee -a "$OUTFILE"
echo "AFTER - Expected Index Scan (with index):" | tee -a "$OUTFILE"
run_sql "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT * FROM orders WHERE customer_id = 500 AND status = 'DELIVERED' ORDER BY order_date DESC;"

echo "" | tee -a "$OUTFILE"
echo "Index usage stats:" | tee -a "$OUTFILE"
run_sql "SELECT indexrelname AS indexname, idx_scan, pg_size_pretty(pg_relation_size(indexrelid)) AS size FROM pg_stat_user_indexes WHERE indexrelname LIKE 'idx_orders%';"

echo "" | tee -a "$OUTFILE"
echo "Optimisation demo complete. Report saved to: $OUTFILE" | tee -a "$OUTFILE"
