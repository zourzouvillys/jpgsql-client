package io.zrz.jpgsql.client;

import java.util.Collection;

import org.reactivestreams.Publisher;

/**
 * common client API for interacting with PostgreSQL.
 *
 * This interface is shared by the org.postgresql JDBC driver (both threaded and direct), as well as my netty async
 * implementation.
 *
 * @author theo
 *
 */

public interface PostgresClient extends PostgresQueryProcessor, AutoCloseable {

  /**
   * provide a dedicated session to the server. this is a direct mapping to a single client connection.
   * 
   * the session may be provided from a pool, or may result in a new connection.
   * 
   */

  PgSession openSession();

  /**
   * open a {@link TransactionalSession} to perform multiple request/response interactions within a single transaction.
   * only should be used for transactions which span multiple round trips.
   */

  TransactionalSession open();

  /**
   * opens a dedicated connection that monitors for notify messages, and can optionally collect other stats
   * periodically.
   */

  Publisher<NotifyMessage> notifications(Collection<String> channels);

  /**
   * close the client
   */

  @Override
  default void close() {

  }

  PostgresConnectionProperties config();

}
