package io.zrz.jpgsql.client.opj;

import java.sql.SQLException;

import io.reactivex.FlowableEmitter;
import io.zrz.jpgsql.client.PostgresqlUnavailableException;
import io.zrz.jpgsql.client.Query;
import io.zrz.jpgsql.client.QueryParameters;
import io.zrz.jpgsql.client.QueryResult;
import lombok.extern.slf4j.Slf4j;

/**
 * dispatched in the {@link PgConnectionThreadPoolExecutor} to send and receive
 * the results of a query.
 */

@Slf4j
class PgQueryRunner implements Runnable {

  private final FlowableEmitter<QueryResult> emitter;
  private final Query query;
  private final QueryParameters params;

  public PgQueryRunner(Query query, QueryParameters params, FlowableEmitter<QueryResult> emitter) {
    this.emitter = emitter;
    this.query = query;
    this.params = params;
  }

  /**
   * perform the action execution.
   *
   * @throws SQLException
   */

  private void run(PgLocalConnection conn) throws SQLException {
    conn.execute(this.query, this.params, this.emitter);
  }

  @Override
  public void run() {
    log.trace("running query");
    try {
      this.run(PgConnectionThread.connection());
    } catch (final SQLException ex) {
      // Any propagated SQLException results in the connection being closed.
      PgConnectionThread.close();
      this.emitter.onError(new PostgresqlUnavailableException(ex));
    } catch (final Throwable ex) {
      this.emitter.onError(new PostgresqlUnavailableException(ex));
    }
  }

}
