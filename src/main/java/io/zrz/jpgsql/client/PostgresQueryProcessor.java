package io.zrz.jpgsql.client;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * common API shared between {@link TransactionalSession} and {@link PostgresClient}.
 */

public interface PostgresQueryProcessor {

  /**
   * Create a new query to be executed.
   *
   * @param sql
   *          The SQL statement to execute, without all the JDBC parsing bullshit. Uses PostgreSQL's native form for
   *          placeholders ($1, $2, etc).
   *
   * @param paramcount
   *          The number of parameters in this query. May be 0.
   *
   * @return A reference for the query which can be used to submit for processing by PostgreSQL.
   */

  Query createQuery(String sql, int paramcount);

  /**
   * An SQL statement which takes no parameters.
   *
   * @see #createQuery(String, int).
   */

  default Query createQuery(String sql) {
    return createQuery(sql, 0);
  }

  /**
   * Creates a query which will be sent as a single batch.
   *
   * @param combine
   *          The queries to batch together.
   *
   * @return A reference for the query which can be used to submit for processing by PostgreSQL.
   */

  Query createQuery(List<Query> combine);

  /**
   * @see #createQuery(List)
   */

  default Query createQuery(Query... combine) {
    return this.createQuery(Arrays.asList(combine));
  }

  /**
   * submit a query (which may consist of multiple statements) to execute within a single transaction. they will either
   * all fail or all complete.
   *
   * The query will be wrapped in a BEGIN and COMMIT/ROLLBACK, so these should not be included in the query.
   *
   * flow control is used here, but consumers should be very careful not to cause a transaction to stay open longer than
   * needed because it is not consuming results.
   *
   * if the work queue is full (or times out), then the returned {@link Publisher} will fail with
   * {@link PostgresqlCapacityExceededException}.
   *
   * The returned {@link Publisher} can be cancelled.
   *
   */

  Publisher<QueryResult> submit(Query query, QueryParameters params);

  /**
   * performs a copy.
   * 
   * @param sql
   * @param data
   * 
   * @return
   */

  Flowable<QueryResult> copy(String sql, InputStream data);

  /**
   * execute query without any parameters.
   *
   * @see #submit(Query, QueryParameters).
   */

  default Publisher<QueryResult> submit(Query query) {
    return submit(query, null);
  }

  default Publisher<QueryResult> submit(String sql) {
    return submit(createQuery(sql));
  }

  default QueryExecutionBuilder executionBuilder() {
    return QueryExecutionBuilder.with(this);
  }

  default Publisher<QueryResult> submit(String sql, Object... params) {
    final Query query = this.createQuery(sql);
    final QueryParameters qp = query.createParameters();
    qp.setFrom(params);
    return this.submit(query, qp);
  }

}
