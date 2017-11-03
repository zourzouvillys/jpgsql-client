package io.zrz.jpgsql.client.opj;

import org.postgresql.jdbc.PgConnection;

/**
 * exposed API for consumers to interact directly with the connection
 */

public interface PgRawConnection {

  PgConnection getConnection();

  PgThreadPooledClient getClient();

}
