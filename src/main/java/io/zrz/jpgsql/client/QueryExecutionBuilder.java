package io.zrz.jpgsql.client;

import org.reactivestreams.Publisher;

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

  @Override
  protected QueryExecutionBuilder result(int i, Tuple tuple) {
    return this;
  }

  public Tuple toQuery() {
    return this.buildQuery();
  }

}
