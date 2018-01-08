package io.zrz.jpgsql.client;

import java.util.List;
import java.util.stream.Collectors;

import lombok.EqualsAndHashCode;

/**
 * multiple queries to be sent for processing together.
 */

@EqualsAndHashCode
public class CombinedQuery implements Query {

  private final List<SimpleQuery> queries;
  private final int params;

  public CombinedQuery(List<SimpleQuery> queries) {
    this.queries = queries;
    this.params = queries.stream().mapToInt(q -> q.parameterCount()).sum();
  }

  @Override
  public List<SimpleQuery> getSubqueries() {
    return this.queries;
  }

  @Override
  public int parameterCount() {
    return this.params;
  }

  public String toString() {
    return this.queries.stream().map(t -> t.sql()).collect(Collectors.joining(";\n"));
  }

}
