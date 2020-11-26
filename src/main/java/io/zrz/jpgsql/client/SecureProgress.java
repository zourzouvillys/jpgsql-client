package io.zrz.jpgsql.client;

/**
 */

public class SecureProgress implements QueryResult {

  private final int statementId;

  public SecureProgress(final int statementId) {
    this.statementId = statementId;
  }

  @Override
  public int statementId() {
    return this.statementId;
  }

  @Override
  public QueryResultKind getKind() {
    return QueryResultKind.PROGRESS;
  }

}
