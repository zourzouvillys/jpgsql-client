package io.zrz.jpgsql.client;

import java.util.Arrays;
import java.util.List;

import org.reactivestreams.Publisher;

import com.google.common.io.ByteSource;

import io.netty.buffer.ByteBuf;
import io.reactivex.Flowable;
import io.zrz.sqlwriter.SqlWriter.SqlGenerator;
import io.zrz.sqlwriter.Tuple;

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

  default Query createQuery(final String sql) {
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

  default Query createQuery(final Query... combine) {
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
   * @param upstream
   *
   * @return
   */

  Publisher<Long> copyTo(String sql, Publisher<ByteBuf> upstream);

  /**
   *
   * @param sql
   * @param upstream
   * @return
   */

  Publisher<Long> copyTo(String sql, ByteSource source);

  /**
   *
   * @param query
   * @return
   */

  default Publisher<QueryResult> submit(final SqlGenerator query) {
    return query.submitTo(this);
  }

  /**
   * execute query without any parameters.
   *
   * @see #submit(Query, QueryParameters).
   */

  default Publisher<QueryResult> submit(final Query query) {
    return submit(query, null);
  }

  default Publisher<QueryResult> submit(final String sql) {
    return submit(createQuery(sql));
  }

  default QueryExecutionBuilder executionBuilder() {
    return QueryExecutionBuilder.with(this);
  }

  default Publisher<QueryResult> submit(final String sql, final Object... params) {
    final Query query = this.createQuery(sql, params.length);
    final QueryParameters qp = query.createParameters();
    qp.setFrom(params);
    return this.submit(query, qp);
  }

  Flowable<QueryResult> fetch(int batchSize, Tuple tuple);

  default Flowable<QueryResult> fetch(final int batchSize, final String sql) {
    return fetch(batchSize, Tuple.of(createQuery(sql), DefaultParametersList.emptyParameters()));
  }

  default Flowable<QueryResult> fetch(final int batchSize, final SqlGenerator g) {
    return fetch(batchSize, g.asTuple());
  }

  PostgresClient client();

}
