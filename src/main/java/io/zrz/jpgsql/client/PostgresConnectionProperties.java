package io.zrz.jpgsql.client;

import java.time.Duration;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;

@Value
@Builder
public class PostgresConnectionProperties {

  /**
   * The hostname. Localhost by default.
   */

  private String hostname;

  /**
   * The port to connect to. 5432 by default.
   */

  private int port;

  /**
   * The database name to connect to.
   */

  private String dbname;

  /**
   * The username to use for connecting.
   */

  private String username;

  /**
   * The password to use for connecting.
   */

  private String password;

  /**
   * The minimum number of idle connections.
   */

  private int minIdle;

  /**
   * how long a connection is idle before it is closed and removed from the
   * pool.
   */

  @Default
  private Duration idleTimeout = Duration.ofSeconds(60);

  /**
   * how long a connection tries to establish before timing out.
   */

  @Default
  private Duration connectTimeout = Duration.ofSeconds(10);

  /**
   * maximum number of connections.
   *
   * be aware that if the {@link org.postgresql} version of the PostgresClient
   * is used and configured to use a thread for async emulation, then this will
   * be equal to the number of threads created (urgh).
   *
   */

  @Default
  private int maxPoolSize = 10;

  /**
   * The number of queries that can be queued for execution.
   *
   * If set to zero, this will not allow more than the {@link #getMaxPoolSize()}
   * number of pending/executing queries.
   *
   */

  private int queueDepth;

}
