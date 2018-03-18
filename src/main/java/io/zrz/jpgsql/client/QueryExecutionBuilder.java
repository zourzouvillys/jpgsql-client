package io.zrz.jpgsql.client;

import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.zrz.sqlwriter.SqlWriters;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryExecutionBuilder extends AbstractQueryExecutionBuilder<QueryExecutionBuilder> {

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
    return this.client.submit(t.getQuery(), t.getParams());
  }

  public Flowable<ResultRow> fetch() {
    return Flowable.fromPublisher(execute())
        .flatMap(x -> {
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
