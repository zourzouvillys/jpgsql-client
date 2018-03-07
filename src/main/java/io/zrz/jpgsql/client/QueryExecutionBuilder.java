package io.zrz.jpgsql.client;

import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
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
            case PROGRESS:
            case STATUS:
            case WARNING:
              log.info("got {}", x);
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

}
