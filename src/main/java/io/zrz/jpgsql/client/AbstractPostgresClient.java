package io.zrz.jpgsql.client;

import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractPostgresClient implements PostgresClient {

  @Override
  public Query createQuery(String sql, int paramcount) {
    return new SimpleQuery(sql, paramcount);
  }

  @Override
  public Query createQuery(List<Query> combine) {
    return new CombinedQuery(combine.stream().flatMap(q -> q.getSubqueries().stream()).collect(Collectors.toList()));
  }

}
