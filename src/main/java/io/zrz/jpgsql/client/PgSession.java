package io.zrz.jpgsql.client;

import org.reactivestreams.Publisher;

public interface PgSession extends PostgresQueryProcessor, AutoCloseable {

  @Override
  void close();

  Publisher<NotifyMessage> listen(String channel);

}
