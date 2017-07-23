package io.zrz.jpgsql.client.opj;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.postgresql.core.NativeQuery;
import org.postgresql.core.Oid;
import org.postgresql.core.ParameterList;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.TransactionState;
import org.postgresql.jdbc.PgConnection;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import io.reactivex.FlowableEmitter;
import io.zrz.jpgsql.client.Query;
import io.zrz.jpgsql.client.QueryParameters;
import io.zrz.jpgsql.client.QueryResult;
import io.zrz.jpgsql.client.WarningResult;
import lombok.extern.slf4j.Slf4j;

/**
 * deal with postgresql API directly, avoid the JDBC bullshit.
 */

@Slf4j
class PgLocalConnection {

  private final PgConnection conn;

  /**
   *
   * @param conn
   * @param pool
   */

  PgLocalConnection(PgConnection conn) {

    this.conn = conn;

    try {

      conn.setAutoCommit(false);

    } catch (final SQLException e) {
      // should this ever fail?
      // either way, we abosrb and handle later.
      log.error("exception setting auto commit mode", e);
    }

  }

  /**
   * close the connection. don't throw if it fails.
   */

  void close() {
    try {
      if (!this.conn.isClosed()) {
        this.conn.close();
      }
    } catch (final Exception ex) {
      // nothing to do about this ...
      log.warn("got exception closing connection", ex);
    }
  }

  private final LoadingCache<Query, org.postgresql.core.Query> cache = CacheBuilder.newBuilder()
      .maximumSize(1000)
      .expireAfterWrite(5, TimeUnit.MINUTES)
      .build(
          new CacheLoader<Query, org.postgresql.core.Query>() {
            @Override
            public org.postgresql.core.Query load(Query query) throws Exception {
              final List<NativeQuery> nqs = query.getSubqueries().stream()
                  .map(q -> PgLocalNativeQuery.create(q.sql(), q.parameterCount()))
                  .collect(Collectors.toList());
              return PgLocalConnection.this.conn.getQueryExecutor().wrap(nqs);
            }
          });

  /**
   * note: this weirdness is because we have no way to call multiple prepared
   * statements in a single batch, so we resort to calling EXECUTE ourselves.
   * woo.
   *
   * @param named
   * @throws SQLException
   */

  void execute(Query query, QueryParameters params, FlowableEmitter<QueryResult> emitter) throws SQLException {

    final org.postgresql.core.Query pgquery = this.cache.getUnchecked(query);

    final ParameterList pl;

    if (params != null) {

      pl = pgquery.createParameterList();
      for (int i = 0; i < params.count(); ++i) {

        final int oid = params.getOid(i);
        switch (oid) {
          case Oid.UNSPECIFIED:
            break;
          case Oid.INT4:
            pl.setIntParameter(i, (int) params.getValue(i));
            break;
          case Oid.VARCHAR:
            pl.setStringParameter(i, (String) params.getValue(i), oid);
            break;
          default:
            break;
        }
      }

    } else {
      pl = null;
    }

    // pl.setIntParameter(1, 1000);
    // pl.setStringParameter(1, "[ 1, 2, 3 ]", Oid.JSON);
    // pl.setStringParameter(1, "[ 1, 2, 3 ]", Oid.TEXT);
    final int flags = 0;

    final QueryExecutor exec = this.conn.getQueryExecutor();

    // flags |= QueryExecutor.QUERY_ONESHOT;
    // flags |= QueryExecutor.QUERY_SUPPRESS_BEGIN;
    // flags |= QueryExecutor.QUERY_EXECUTE_AS_SIMPLE;
    // flags |= QueryExecutor.QUERY_BOTH_ROWS_AND_STATUS;
    // flags |= QueryExecutor.QUERY_NO_RESULTS |
    // QueryExecutor.QUERY_NO_METADATA;

    log.info("-------");
    final long start = System.nanoTime();
    exec.execute(pgquery, pl, new PgObservableResultHandler(query, emitter), 10000, 10000, flags);
    final long stop = System.nanoTime();
    log.info("-------");

    log.info(String.format("done in %.02f ms", (stop - start) / 1000 / 1000.0));

    final SQLWarning warnings = this.conn.getWarnings();

    if (warnings != null) {
      log.warn("SQL warning: {}", warnings);
      emitter.onNext(new WarningResult(warnings));
    }

  }

  TransactionState transactionState() {
    return this.conn.getTransactionState();
  }

  /**
   * clean up the transaction, by aborting.
   *
   * @throws SQLException
   */

  public void rollback() throws SQLException {

    this.conn.rollback();

  }

}
