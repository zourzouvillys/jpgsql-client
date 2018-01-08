package io.zrz.jpgsql.client;

import java.util.List;

/**
 * represents a query that can be executed. may be a simple single statement, or multiple to be executed together as a
 * batch.
 *
 * a {@link Query} is passed to {@link PostgresClient#submit(Query, QueryParameters)} to be executed.
 *
 */

public interface Query {

  /**
   * create parameters to execute this query.
   */

  default QueryParameters createParameters() {
    return new DefaultParametersList(this.parameterCount());
  }

  /**
   * how many parameters this query needs.
   */

  int parameterCount();

  /**
   * if this query consists of multiple others, returns them here.
   */

  List<SimpleQuery> getSubqueries();

  default SimpleQuery statement(int statementId) {
    return getSubqueries().get(statementId);
  }

}
