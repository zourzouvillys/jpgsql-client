package io.zrz.jpgsql.client;

public interface PgSession extends PostgresQueryProcessor, AutoCloseable {

  @Override
  void close();

}
