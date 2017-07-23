package io.zrz.jpgsql.client;

/**
 * Raised when a {@link TransactionalSession} instance was rolled back and
 * failed because it was idle for too long.
 */

public class TransactionalSessionDeadlineExceededException extends RuntimeException {

  private static final long serialVersionUID = 1L;

}
