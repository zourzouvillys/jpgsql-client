// Generated by delombok at Tue Sep 22 10:54:18 PDT 2020
package io.zrz.jpgsql.client;

import java.util.function.Function;

import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.zrz.sqlwriter.SqlWriters;
import io.zrz.sqlwriter.Tuple;

public class QueryExecutionBuilder extends AbstractQueryExecutionBuilder<QueryExecutionBuilder> {
  @java.lang.SuppressWarnings("all")
  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(QueryExecutionBuilder.class);

  public QueryExecutionBuilder(PostgresQueryProcessor ds) {
    super(ds);
  }

  public static QueryExecutionBuilder with(PostgresQueryProcessor ds) {
    return new QueryExecutionBuilder(ds);
  }

  /**
   * executes the statements.
   */

  public Publisher<QueryResult> execute() {
    final Tuple t = this.buildQuery();
    return this.client.submit(t.query(), t.params());
  }

  public <T> T execute(Function<Publisher<QueryResult>, T> converter) {
    return converter.apply(execute());
  }

  public Flowable<ResultRow> fetch() {
    return Flowable.fromPublisher(execute()).flatMap(x -> {
      switch (x.getKind()) {
        case RESULTS:
          return (RowBuffer) x;
        case ERROR:
          return Flowable.error((ErrorResult) x);
        case STATUS: {
          CommandStatus status = (CommandStatus) x;
          switch (status.getStatus()) {
            case "BEGIN":
            case "COMMIT":
              return Flowable.empty();
          }
          log.info("unexpected fetch status {}", x);
          return Flowable.empty();
        }
        case PROGRESS:
        case WARNING:
          log.info("unexpected fetch event {}", x);
        default:
          return Flowable.empty();
      }
    });
  }

  @Override
  protected QueryExecutionBuilder result(int i, Tuple tuple) {
    return this;
  }

  public Tuple toQuery() {
    return this.buildQuery();
  }

  public QueryExecutionBuilder notify(String channel) {
    SqlWriters.notify(channel).addTo(this, true);
    return this;
  }

  public QueryExecutionBuilder notify(String channel, String payload) {
    SqlWriters.notify(channel, payload).addTo(this, true);
    return this;
  }
}
