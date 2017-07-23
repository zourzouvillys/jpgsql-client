package io.zrz.jpgsql.client;

import org.reactivestreams.Publisher;

/**
 * common API shared between {@link TransactionalSession} and
 * {@link PostgresClient}.
 */

public interface PostgresQueryProcessor {

  /**
   * submit a query (which may consist of multiple statements) to execute within
   * a single transaction. they will either all fail or all complete.
   *
   * The query will be wrapped in a BEGIN and COMMIT/ROLLBACK, so these should
   * not be included in the query.
   *
   * flow control is used here, but consumers should be very careful not to
   * cause a transaction to stay open longer than needed because it is not
   * consuming results.
   *
   * if the work queue is full (or times out), then the returned
   * {@link Publisher} will fail with
   * {@link PostgresqlCapacityExceededException}.
   *
   * The returned {@link Publisher} can be cancelled.
   *
   */

  Publisher<QueryResult> submit(Query query, QueryParameters params);

  /**
   * execute query without any parameters.
   *
   * @see #submit(Query, QueryParameters).
   */

  default Publisher<QueryResult> submit(Query query) {
    return submit(query, null);
  }

}
