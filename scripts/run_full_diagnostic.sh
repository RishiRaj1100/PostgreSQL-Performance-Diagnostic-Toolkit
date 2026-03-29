#!/bin/bash
set -e
set -u

# Runs the complete PostgreSQL diagnostic suite.
# Usage: ./run_full_diagnostic.sh
# Output: results/diagnostic_TIMESTAMP.txt

mkdir -p results
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTFILE="results/diagnostic_${TIMESTAMP}.txt"
PSQL="docker exec diagnostic-postgres psql -U dba -d ecommerce_db"

print_section() {
  echo "" >> "$OUTFILE"
  echo "======================================" >> "$OUTFILE"
  echo " $1" >> "$OUTFILE"
  echo "======================================" >> "$OUTFILE"
}

echo "Starting PostgreSQL Diagnostic Suite..."
echo "Output: $OUTFILE"

print_section "1. SLOW QUERY ANALYSIS"
$PSQL -c "CALL analyse_slow_queries(3, 50.0);" >> "$OUTFILE" 2>&1

print_section "2. INDEX HEALTH"
$PSQL -c "CALL check_index_health();" >> "$OUTFILE" 2>&1

print_section "3. CACHE HIT RATIO"
$PSQL -c "CALL check_cache_hit_ratio();" >> "$OUTFILE" 2>&1

print_section "4. TABLE BLOAT"
$PSQL -c "CALL detect_table_bloat();" >> "$OUTFILE" 2>&1

print_section "5. QUERY PLAN - BEFORE INDEX"
$PSQL -c "CALL analyse_query_plan('SELECT * FROM orders WHERE customer_id = 500 AND status = ''DELIVERED''');" >> "$OUTFILE" 2>&1

print_section "6. DIAGNOSTIC SUMMARY"
$PSQL -c "SELECT severity, COUNT(*) AS count FROM diagnostic_log GROUP BY severity ORDER BY severity;" >> "$OUTFILE" 2>&1

print_section "7. CRITICAL AND WARN FINDINGS"
$PSQL -c "SELECT check_name, severity, finding, recommendation FROM diagnostic_log WHERE severity IN ('CRITICAL','WARN') ORDER BY checked_at DESC LIMIT 20;" >> "$OUTFILE" 2>&1

echo ""
echo "Diagnostic complete."
echo "Results saved to: $OUTFILE"
echo ""
cat "$OUTFILE"
