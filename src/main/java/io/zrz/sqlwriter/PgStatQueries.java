package io.zrz.sqlwriter;

import io.zrz.jpgsql.client.AbstractQueryExecutionBuilder.Tuple;

public class PgStatQueries {

  private static Tuple indexIoStats = QueryGenerator.from(DbIdent.of("pg_catalog", "pg_statio_all_indexes"))
      .selectIdent("relid")
      .selectIdent("indexrelid")
      .selectIdent("schemaname")
      .selectIdent("relname")
      .selectIdent("indexrelname")
      .selectIdent("idx_blks_read")
      .selectIdent("idx_blks_hit")
      .asTuple();

  public static Tuple indexIoStats() {
    return indexStats;
  }

  private static Tuple tableIoStats = QueryGenerator.from(DbIdent.of("pg_catalog", "pg_statio_all_tables"))
      .selectIdent("relid")
      .selectIdent("schemaname")
      .selectIdent("relname")
      .selectIdent("heap_blks_read")
      .selectIdent("heap_blks_hit")
      .selectIdent("idx_blks_read")
      .selectIdent("idx_blks_hit")
      .selectIdent("toast_blks_read")
      .selectIdent("toast_blks_hit")
      .selectIdent("tidx_blks_read")
      .selectIdent("tidx_blks_hit")
      .asTuple();
  //

  public static Tuple tableIoStats() {
    return tableIoStats;
  }

  private static Tuple indexStats = QueryGenerator.from(DbIdent.of("pg_catalog", "pg_stat_all_indexes"))
      .selectIdent("relid")
      .selectIdent("indexrelid")
      .selectIdent("schemaname")
      .selectIdent("relname")
      .selectIdent("indexrelname")
      .selectIdent("idx_scan")
      .selectIdent("idx_tup_read")
      .selectIdent("idx_tup_fetch")
      .asTuple();

  public static Tuple indexStats() {
    return indexStats;
  }

  private static Tuple tableStats = QueryGenerator.from(DbIdent.of("pg_catalog", "pg_stat_all_tables"))
      .selectIdent("relid")
      .selectIdent("schemaname")
      .selectIdent("relname")
      .selectIdent("seq_scan")
      .selectIdent("seq_tup_read")
      .selectIdent("idx_scan")
      .selectIdent("idx_tup_fetch")
      .selectIdent("n_tup_ins")
      .selectIdent("n_tup_upd")
      .selectIdent("n_tup_del")
      .selectIdent("n_tup_hot_upd")
      .selectIdent("n_live_tup")
      .selectIdent("n_dead_tup")
      .selectIdent("n_mod_since_analyze")
      .selectIdent("last_vacuum")
      .selectIdent("last_autovacuum")
      .selectIdent("last_analyze")
      .selectIdent("last_autoanalyze")
      .selectIdent("vacuum_count")
      .selectIdent("autovacuum_count")
      .selectIdent("analyze_count")
      .selectIdent("autoanalyze_count")
      .asTuple();

  public static Tuple tableStats() {
    return tableStats;
  }

}
