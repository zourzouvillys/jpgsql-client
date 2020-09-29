// Generated by delombok at Tue Sep 22 10:54:18 PDT 2020
package io.zrz.jpgsql.client.opj;

import java.sql.SQLException;

import io.reactivex.rxjava3.core.FlowableEmitter;
import io.zrz.jpgsql.client.PostgresqlUnavailableException;
import io.zrz.jpgsql.client.Query;
import io.zrz.jpgsql.client.QueryParameters;
import io.zrz.jpgsql.client.QueryResult;

/**
 * dispatched in the {@link PgConnectionThreadPoolExecutor} to send and receive the results of a *single* query, running
 * atomically inside a txn block.
 */
class PgQueryRunner implements Runnable {
  @java.lang.SuppressWarnings("all")
  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(PgQueryRunner.class);
  private final FlowableEmitter<QueryResult> emitter;
  private final Query query;
  private final QueryParameters params;
  private int fetchSize;

  public PgQueryRunner(final Query query, final QueryParameters params, final FlowableEmitter<QueryResult> emitter, final AmbientContext ctx, final int fetchSize) {
    this.emitter = emitter;
    this.query = query;
    this.params = params;
    this.fetchSize = fetchSize;
  }

  /**
   * perform the action execution.
   *
   * @throws SQLException
   */
  private void run(final PgLocalConnection conn) throws SQLException {
    try {
      conn.execute(this.query, this.params, this.emitter, fetchSize, fetchSize == 0 ? PgLocalConnection.SuppressBegin : 0);
    } finally {
      log.debug("command completed with txnstatus {}", conn.transactionState());
      switch (conn.transactionState()) {
      case OPEN: 
        log.debug("open transaction after one-shot command");
        conn.commit();
        break;
      case FAILED: 
        log.debug("txn failed, rolling back");
        conn.rollback();
        break;
      case IDLE: 
      default: 
        break;
      }
    }
  }

  @Override
  public void run() {
    try {
      this.run(PgConnectionThread.connection());
      this.emitter.onComplete();
    } catch (final SQLException ex) {
      // Any propagated SQLException results in the connection being closed.
      ex.printStackTrace();
      PgConnectionThread.close();
      this.emitter.onError(new PostgresqlUnavailableException(ex));
    } catch (final Throwable ex) {
      ex.printStackTrace();
      this.emitter.onError(new PostgresqlUnavailableException(ex));
    }
  }
}
