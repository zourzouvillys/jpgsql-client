package io.zrz.jpgsql.client;

/**
 * raised when a job is submitted but there is no spare connections and the work
 * queue is full.
 */

public class PostgresqlCapacityExceededException extends RuntimeException {

  private static final long serialVersionUID = 1L;

}
