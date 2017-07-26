package io.zrz.jpgsql.client;

import org.postgresql.util.ServerErrorMessage;

import io.zrz.visitors.annotations.Visitable;

@Visitable.Type
public final class ErrorResult extends RuntimeException implements QueryResult {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  private final int statementId;
  private final String message;
  private final ServerErrorMessage serverErrorMessage;
  private final Throwable cause;
  private final String state;

  public ErrorResult(int statementId, String message, String state, ServerErrorMessage serverErrorMessage, Throwable cause) {
    this.statementId = statementId;
    this.state = state;
    this.message = message;
    this.serverErrorMessage = serverErrorMessage;
    this.cause = cause;
  }

  @Override
  public int statementId() {
    return this.statementId;
  }

  public String state() {
    return this.state;
  }

  public String message() {
    return this.message;
  }

  public ServerErrorMessage serverErrorMessage() {
    return this.serverErrorMessage;
  }

  public Throwable cause() {
    return this.cause;
  }

}
