package io.netlibs.psql.client;

import org.junit.Test;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import io.zrz.jpgsql.proto.client.PgConnection;
import io.zrz.jpgsql.proto.client.PgConnectionBuilder;

public class PgConnectionTest {

  @Test
  public void test() throws Exception {

    InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);

    PgConnection conn = new PgConnectionBuilder()
        .database("us3")
        .username("theo")
        .newConnection("127.0.0.1", 5432);

    Thread.sleep(3000);

  }

}
