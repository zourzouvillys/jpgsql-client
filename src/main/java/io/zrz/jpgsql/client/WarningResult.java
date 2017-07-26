package io.zrz.jpgsql.client;

import org.postgresql.util.ServerErrorMessage;

import io.zrz.visitors.annotations.Visitable;

@Visitable.Type
public final class WarningResult implements QueryResult {

  private final int statementId;
  private final ServerErrorMessage servermsg;

  public WarningResult(int statementId, ServerErrorMessage servermsg) {
    this.statementId = statementId;
    this.servermsg = servermsg;
  }

  @Override
  public int statementId() {
    return this.statementId;
  }

  public ServerErrorMessage serverErrorMessage() {
    return this.servermsg;
  }

}
