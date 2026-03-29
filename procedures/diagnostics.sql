-- PostgreSQL Performance Diagnostic Toolkit - core PL/pgSQL diagnostics.

/*
Procedure: analyse_slow_queries(p_min_calls, p_min_avg_ms)
Reads: pg_stat_statements
Metrics: calls, avg/total execution time, shared_blks_hit/read cache ratio.
Thresholds: >=5 calls and >=100ms avg by default; >1000ms CRITICAL, >500ms WARN.
DBA action: tune indexes/query patterns; low cache hit suggests shared_buffers review.
*/
CREATE OR REPLACE PROCEDURE analyse_slow_queries(
  p_min_calls  INT   DEFAULT 5,
  p_min_avg_ms FLOAT DEFAULT 100.0
)
LANGUAGE plpgsql AS $$
DECLARE
  v_query_record RECORD;
  v_count        INT := 0;
BEGIN
  RAISE NOTICE '========================================';
  RAISE NOTICE ' SLOW QUERY ANALYSIS';
  RAISE NOTICE '========================================';

  IF NOT EXISTS (
    SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
  ) THEN
    RAISE EXCEPTION 'pg_stat_statements extension not installed. Run: CREATE EXTENSION pg_stat_statements;';
  END IF;

  FOR v_query_record IN
    SELECT
      LEFT(query, 120) AS query_preview,
      calls,
      ROUND((total_exec_time / calls)::NUMERIC, 2) AS avg_ms,
      ROUND(total_exec_time::NUMERIC, 2) AS total_ms,
      ROUND(rows::NUMERIC / calls, 1) AS avg_rows,
      ROUND(
        100.0 * shared_blks_hit /
        NULLIF(shared_blks_hit + shared_blks_read, 0),
        2
      ) AS cache_hit_pct
    FROM pg_stat_statements
    WHERE calls >= p_min_calls
      AND total_exec_time / calls >= p_min_avg_ms
    ORDER BY total_exec_time / calls DESC
    LIMIT 15
  LOOP
    v_count := v_count + 1;
    RAISE NOTICE '--- Query #% ---', v_count;
    RAISE NOTICE 'Preview    : %', v_query_record.query_preview;
    RAISE NOTICE 'Calls      : %', v_query_record.calls;
    RAISE NOTICE 'Avg Time   : % ms', v_query_record.avg_ms;
    RAISE NOTICE 'Total Time : % ms', v_query_record.total_ms;
    RAISE NOTICE 'Avg Rows   : %', v_query_record.avg_rows;
    RAISE NOTICE 'Cache Hit  : %%%', COALESCE(v_query_record.cache_hit_pct, 0);

    INSERT INTO diagnostic_log (check_name, severity, finding, recommendation)
    VALUES (
      'slow_query_' || v_count,
      CASE
        WHEN v_query_record.avg_ms > 1000 THEN 'CRITICAL'
        WHEN v_query_record.avg_ms > 500  THEN 'WARN'
        ELSE 'OK'
      END,
      'Query averaging ' || v_query_record.avg_ms || 'ms over '
        || v_query_record.calls || ' calls',
      CASE
        WHEN COALESCE(v_query_record.cache_hit_pct, 0) < 95
          THEN 'Low cache hit: review shared_buffers and working set size.'
        ELSE 'Review predicates and add/selective indexes where needed.'
      END
    );
  END LOOP;

  IF v_count = 0 THEN
    RAISE NOTICE 'No slow queries found with >= % calls and >= %ms avg',
      p_min_calls, p_min_avg_ms;
  ELSE
    RAISE NOTICE '========================================';
    RAISE NOTICE '% slow queries logged to diagnostic_log', v_count;
  END IF;

  COMMIT;
END;
$$;

/*
Procedure: check_index_health()
Reads: pg_stat_user_indexes, pg_stat_user_tables, pg_tables.
Metrics: idx_scan, seq_scan, index size.
Thresholds: seq_scan > 10 and idx_scan < seq_scan/2 => low index utilization.
DBA action: validate and drop truly unused indexes; add selective indexes where needed.
*/
CREATE OR REPLACE PROCEDURE check_index_health()
LANGUAGE plpgsql AS $$
DECLARE
  v_rec RECORD;
BEGIN
  RAISE NOTICE '========================================';
  RAISE NOTICE ' INDEX HEALTH ANALYSIS';
  RAISE NOTICE '========================================';

  RAISE NOTICE '--- UNUSED INDEXES (never scanned) ---';
  FOR v_rec IN
    SELECT
      schemaname,
      relname AS tablename,
      indexrelname AS indexname,
      pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
      idx_scan AS times_scanned
    FROM pg_stat_user_indexes
    WHERE idx_scan = 0
      AND indexrelname NOT LIKE '%_pkey'
    ORDER BY pg_relation_size(indexrelid) DESC
  LOOP
    RAISE NOTICE 'Table: %.%  Index: %  Size: %  Scans: 0',
      v_rec.schemaname, v_rec.tablename, v_rec.indexname, v_rec.index_size;

    INSERT INTO diagnostic_log (check_name, severity, finding, recommendation)
    VALUES (
      'unused_index',
      'WARN',
      'Index ' || v_rec.indexname || ' on ' || v_rec.tablename
        || ' (' || v_rec.index_size || ') has never been scanned',
      'Consider DROP INDEX ' || v_rec.indexname || '; after workload validation.'
    );
  END LOOP;

  RAISE NOTICE '--- TABLES WITH NO/LOW INDEX UTILIZATION ---';
  FOR v_rec IN
    SELECT
      t.tablename,
      pg_size_pretty(pg_total_relation_size((t.schemaname || '.' || t.tablename)::REGCLASS)) AS table_size,
      s.seq_scan,
      s.idx_scan
    FROM pg_tables t
    JOIN pg_stat_user_tables s
      ON t.schemaname = s.schemaname
     AND t.tablename = s.relname
    WHERE t.schemaname = 'public'
      AND s.seq_scan > 10
      AND s.idx_scan < s.seq_scan / 2
    ORDER BY s.seq_scan DESC
  LOOP
    RAISE NOTICE 'Table: %  Size: %  Seq Scans: %  Index Scans: %',
      v_rec.tablename, v_rec.table_size, v_rec.seq_scan, v_rec.idx_scan;

    INSERT INTO diagnostic_log (check_name, severity, finding, recommendation)
    VALUES (
      'high_seq_scan',
      CASE WHEN v_rec.seq_scan > 1000 THEN 'CRITICAL' ELSE 'WARN' END,
      'Table ' || v_rec.tablename || ' has '
        || v_rec.seq_scan || ' sequential scans vs '
        || v_rec.idx_scan || ' index scans',
      'Review query predicates and add selective index strategy.'
    );
  END LOOP;

  RAISE NOTICE '--- TOP 10 MOST USED INDEXES ---';
  FOR v_rec IN
    SELECT
      relname AS tablename,
      indexrelname AS indexname,
      idx_scan,
      pg_size_pretty(pg_relation_size(indexrelid)) AS size
    FROM pg_stat_user_indexes
    ORDER BY idx_scan DESC
    LIMIT 10
  LOOP
    RAISE NOTICE 'Index: %  Table: %  Scans: %  Size: %',
      v_rec.indexname, v_rec.tablename, v_rec.idx_scan, v_rec.size;
  END LOOP;

  COMMIT;
END;
$$;

/*
Procedure: check_cache_hit_ratio()
Reads: pg_statio_user_tables, pg_statio_user_indexes.
Metrics: heap_blks_hit/read and idx_blks_hit/read.
Thresholds: >=99 OK, 95-99 WARN, <95 CRITICAL.
DBA action: low ratio indicates disk-heavy workload; review shared_buffers and memory sizing.
*/
CREATE OR REPLACE PROCEDURE check_cache_hit_ratio()
LANGUAGE plpgsql AS $$
DECLARE
  v_table_hit_ratio NUMERIC := 0;
  v_index_hit_ratio NUMERIC := 0;
  v_overall_ratio   NUMERIC := 0;
  v_severity        VARCHAR(10);
BEGIN
  RAISE NOTICE '========================================';
  RAISE NOTICE ' BUFFER CACHE HIT RATIO ANALYSIS';
  RAISE NOTICE '========================================';

  SELECT COALESCE(
    ROUND(
      SUM(heap_blks_hit)::NUMERIC /
      NULLIF(SUM(heap_blks_hit) + SUM(heap_blks_read), 0) * 100,
      2
    ),
    0
  )
  INTO v_table_hit_ratio
  FROM pg_statio_user_tables;

  SELECT COALESCE(
    ROUND(
      SUM(idx_blks_hit)::NUMERIC /
      NULLIF(SUM(idx_blks_hit) + SUM(idx_blks_read), 0) * 100,
      2
    ),
    0
  )
  INTO v_index_hit_ratio
  FROM pg_statio_user_indexes;

  v_overall_ratio := ROUND((v_table_hit_ratio + v_index_hit_ratio) / 2, 2);

  v_severity := CASE
    WHEN v_overall_ratio >= 99 THEN 'OK'
    WHEN v_overall_ratio >= 95 THEN 'WARN'
    ELSE 'CRITICAL'
  END;

  RAISE NOTICE 'Table  Cache Hit Ratio : %%%', v_table_hit_ratio;
  RAISE NOTICE 'Index  Cache Hit Ratio : %%%', v_index_hit_ratio;
  RAISE NOTICE 'Overall Cache Hit      : %%% - Status: %',
    v_overall_ratio, v_severity;
  RAISE NOTICE 'Target: >= 99%%';

  INSERT INTO diagnostic_log (check_name, severity, finding, recommendation)
  VALUES (
    'cache_hit_ratio',
    v_severity,
    'Table hit: ' || v_table_hit_ratio || '% | Index hit: '
      || v_index_hit_ratio || '% | Overall: ' || v_overall_ratio || '%',
    CASE v_severity
      WHEN 'OK'       THEN 'Cache performance is healthy.'
      WHEN 'WARN'     THEN 'Consider increasing shared_buffers and monitor I/O.'
      WHEN 'CRITICAL' THEN 'Increase shared_buffers promptly; excessive disk reads observed.'
    END
  );

  COMMIT;
END;
$$;

/*
Procedure: detect_table_bloat()
Reads: pg_stat_user_tables.
Metrics: n_dead_tup / n_live_tup as bloat_pct, vacuum timestamps.
Thresholds: logs when bloat_pct > 20; >50 marked CRITICAL.
DBA action: use VACUUM ANALYZE; VACUUM FULL only during controlled maintenance.
*/
CREATE OR REPLACE PROCEDURE detect_table_bloat()
LANGUAGE plpgsql AS $$
DECLARE
  v_rec RECORD;
BEGIN
  RAISE NOTICE '========================================';
  RAISE NOTICE ' TABLE BLOAT DETECTION';
  RAISE NOTICE '========================================';

  FOR v_rec IN
    SELECT
      relname AS table_name,
      n_live_tup AS live_rows,
      n_dead_tup AS dead_rows,
      CASE
        WHEN n_live_tup > 0 THEN ROUND(n_dead_tup::NUMERIC / n_live_tup * 100, 2)
        ELSE 0
      END AS bloat_pct,
      last_vacuum,
      last_autovacuum,
      pg_size_pretty(pg_total_relation_size(relid)) AS total_size
    FROM pg_stat_user_tables
    WHERE n_live_tup > 100
    ORDER BY n_dead_tup DESC
    LIMIT 10
  LOOP
    RAISE NOTICE 'Table: %  Live: %  Dead: %  Bloat: %%%  Last Vacuum: %',
      v_rec.table_name,
      v_rec.live_rows,
      v_rec.dead_rows,
      v_rec.bloat_pct,
      COALESCE(v_rec.last_vacuum::TEXT, COALESCE(v_rec.last_autovacuum::TEXT, 'Never'));

    IF v_rec.bloat_pct > 20 THEN
      INSERT INTO diagnostic_log (check_name, severity, finding, recommendation)
      VALUES (
        'table_bloat_' || v_rec.table_name,
        CASE WHEN v_rec.bloat_pct > 50 THEN 'CRITICAL' ELSE 'WARN' END,
        v_rec.table_name || ' has ' || v_rec.bloat_pct
          || '% bloat (' || v_rec.dead_rows || ' dead rows)',
        'Run VACUUM ANALYZE ' || v_rec.table_name
          || '; consider VACUUM FULL during maintenance for severe bloat.'
      );
    END IF;
  END LOOP;

  COMMIT;
END;
$$;

/*
Procedure: analyse_query_plan(p_query)
Reads: EXPLAIN (ANALYZE, BUFFERS) output for caller SQL.
Metrics: scan/join/sort node presence and buffer behavior.
Purpose: surface plan issues in plain language for DBAs and developers.
DBA action: if Seq Scan appears on large selective queries, add targeted indexes.
*/
CREATE OR REPLACE PROCEDURE analyse_query_plan(
  p_query TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
  v_plan_row      RECORD;
  v_has_seqscan   BOOLEAN := FALSE;
  v_has_index     BOOLEAN := FALSE;
  v_has_bitmap    BOOLEAN := FALSE;
  v_has_join      BOOLEAN := FALSE;
  v_has_sort      BOOLEAN := FALSE;
BEGIN
  RAISE NOTICE '========================================';
  RAISE NOTICE ' QUERY PLAN ANALYSIS';
  RAISE NOTICE '========================================';
  RAISE NOTICE 'Query: %', LEFT(p_query, 200);
  RAISE NOTICE '--- EXECUTION PLAN ---';

  FOR v_plan_row IN
    EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) ' || p_query
  LOOP
    RAISE NOTICE '%', v_plan_row."QUERY PLAN";

    IF v_plan_row."QUERY PLAN" ILIKE '%Seq Scan%' THEN
      v_has_seqscan := TRUE;
    END IF;

    IF v_plan_row."QUERY PLAN" ILIKE '%Index Scan%'
       OR v_plan_row."QUERY PLAN" ILIKE '%Index Only Scan%' THEN
      v_has_index := TRUE;
    END IF;

    IF v_plan_row."QUERY PLAN" ILIKE '%Bitmap Heap Scan%'
       OR v_plan_row."QUERY PLAN" ILIKE '%Bitmap Index Scan%' THEN
      v_has_bitmap := TRUE;
    END IF;

    IF v_plan_row."QUERY PLAN" ILIKE '%Hash Join%'
       OR v_plan_row."QUERY PLAN" ILIKE '%Nested Loop%'
       OR v_plan_row."QUERY PLAN" ILIKE '%Merge Join%' THEN
      v_has_join := TRUE;
    END IF;

    IF v_plan_row."QUERY PLAN" ILIKE '%Sort%' THEN
      v_has_sort := TRUE;
    END IF;
  END LOOP;

  RAISE NOTICE '--- INTERPRETATION ---';

  IF v_has_seqscan THEN
    RAISE NOTICE '[WARN] Sequential Scan detected on at least one node.';
    INSERT INTO diagnostic_log (check_name, severity, finding, recommendation)
    VALUES (
      'query_plan_analysis',
      'WARN',
      'Sequential scan found in query: ' || LEFT(p_query, 100),
      'Add/selectively tune indexes for filter and join columns.'
    );
  END IF;

  IF v_has_index THEN
    RAISE NOTICE '[OK] Index access path detected.';
  END IF;

  IF v_has_bitmap THEN
    RAISE NOTICE '[INFO] Bitmap access path detected.';
  END IF;

  IF v_has_join THEN
    RAISE NOTICE '[INFO] Join strategy detected (Hash/Nested/Merge).';
  END IF;

  IF v_has_sort THEN
    RAISE NOTICE '[INFO] Sort node detected; validate work_mem for large sorts.';
  END IF;

  IF NOT v_has_seqscan AND NOT v_has_index AND NOT v_has_bitmap THEN
    RAISE NOTICE '[INFO] No scan node matched, inspect full plan text.';
  END IF;
EXCEPTION
  WHEN OTHERS THEN
    INSERT INTO diagnostic_log (check_name, severity, finding, recommendation)
    VALUES (
      'query_plan_analysis_error',
      'CRITICAL',
      'Failed to analyze query plan: ' || LEFT(SQLERRM, 300),
      'Validate SQL syntax and permissions, then retry.'
    );
    RAISE;
END;
$$;
