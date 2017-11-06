package io.zrz.jpgsql.client.opj;

import java.util.Map;

import org.postgresql.jdbc.PgConnection;

/**
 * exposed API for consumers to interact directly with the connection
 */

public interface PgRawConnection {

  PgConnection getConnection();

  PgThreadPooledClient getClient();

  void blockingSet(Map<String, String> clientProperties);

  void blockingExecute(String string);

}
