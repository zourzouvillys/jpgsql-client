package io.zrz.jpgsql.client;

import java.util.Arrays;
import java.util.List;

import lombok.EqualsAndHashCode;

/**
 * A single SQL statement.
 */

@EqualsAndHashCode
public class SimpleQuery implements Query {

  private final int paramcount;
  private final String sql;

  public SimpleQuery(String sql) {
    this(sql, 0);
  }

  public SimpleQuery(String sql, int paramcount) {
    this.sql = sql;
    this.paramcount = paramcount;
  }

  @Override
  public QueryParameters createParameters() {
    return new DefaultParametersList(this.paramcount);
  }

  @Override
  public List<SimpleQuery> getSubqueries() {
    return Arrays.asList(this);
  }

  @Override
  public int parameterCount() {
    return this.paramcount;
  }

  /**
   * The SQL this query represents.
   */

  public String sql() {
    return this.sql;
  }

  /**
   * 
   */

  @Override
  public SimpleQuery statement(int statementId) {
    return this;
  }

  public String toString() {
    return this.sql;
  }

}
