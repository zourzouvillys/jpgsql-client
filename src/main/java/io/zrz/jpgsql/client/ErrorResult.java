package io.zrz.jpgsql.client;

import org.postgresql.util.PSQLState;
import org.postgresql.util.ServerErrorMessage;

import io.zrz.visitors.annotations.Visitable;

@Visitable.Type
public final class ErrorResult extends RuntimeException implements QueryResult {

  /**
   * TODO:see {@link PSQLState}
   */
  private static final long serialVersionUID = 1L;

  private final int statementId;
  private final String message;
  private final ServerErrorMessage serverErrorMessage;
  private final Throwable cause;
  private final String state;

  public ErrorResult(final int statementId, final String message, final String state, final ServerErrorMessage serverErrorMessage, final Throwable cause) {
    super(message, cause);
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

  @Override
  public QueryResultKind getKind() {
    return QueryResultKind.ERROR;
  }

}
