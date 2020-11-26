package io.zrz.jpgsql.client;

/**
 * when a query is executed, a sequence of instances of this interface will be
 * delivered.
 */

public interface QueryResult {

  /**
   * the kind of result
   */

  QueryResultKind getKind();

  /**
   * the statement ID that is associated with this result.
   */

  int statementId();

}
