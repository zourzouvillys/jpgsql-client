package io.zrz.sqlwriter;

import io.zrz.jpgsql.client.Query;
import io.zrz.jpgsql.client.QueryParameters;

public class Tuple {

  private final Query query;
  private final QueryParameters params;

  private Tuple(Query query, QueryParameters params) {
    this.query = query;
    this.params = params;
  }

  public static Tuple of(Query query, QueryParameters params) {
    return new Tuple(query, params);
  }

  public Query query() {
    return this.query;
  }

  public QueryParameters params() {
    return this.params;
  }

}
