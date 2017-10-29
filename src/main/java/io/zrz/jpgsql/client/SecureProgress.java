package io.zrz.jpgsql.client;

import io.zrz.visitors.annotations.Visitable;

/**
 */

@Visitable.Type
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
