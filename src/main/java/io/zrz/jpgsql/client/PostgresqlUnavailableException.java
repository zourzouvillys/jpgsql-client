package io.zrz.jpgsql.client;

/**
 * When postgres unavailable.
 */

public class PostgresqlUnavailableException extends RuntimeException {

  public PostgresqlUnavailableException(Throwable ex) {
    super("postgresql is currently unavailable", ex);
  }

  public PostgresqlUnavailableException(String string) {
    super(string);
  }

  private static final long serialVersionUID = 1L;

}
