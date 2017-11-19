package io.zrz.jpgsql.client.opj;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.PSQLException;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

/**
 * Thread that interacts with the {@link PgConnection}.
 */

@Slf4j
public class PgConnectionThread extends Thread {

  private final PgThreadPooledClient pool;
  private PgLocalConnection conn;
  private final Runnable run;

  public PgConnectionThread(final PgThreadPooledClient pool, final Runnable run) {
    super();
    log.debug("started thread");
    this.setDaemon(true);
    this.run = run;
    this.pool = pool;
  }

  @SneakyThrows
  @Override
  public void run() {

    try {

      log.debug("connecting");

      try (final ResultSet res = connection().getConnection().execSQLQuery("SELECT 1")) {
        log.debug("established connection");
      }

      // the loop
      this.run.run();

      log.debug("thread finished");

    }
    catch (final Throwable t) {
      log.info("connection thread error", t);
      throw t;
    }
    finally {
      close();
    }

  }

  // TODO: make configurable at runtime
  private static RetryPolicy RETRY_POLICY = new RetryPolicy()
      .retryOn(PSQLException.class)
      .withDelay(250, TimeUnit.MILLISECONDS)
      .withBackoff(1, 5, TimeUnit.SECONDS)
      .withJitter(0.25)
      .withMaxDuration(30, TimeUnit.SECONDS);

  public static PgLocalConnection connection() throws SQLException {

    final PgConnectionThread thd = (PgConnectionThread) Thread.currentThread();

    if (thd.conn == null) {

      log.debug("Creating new connection thread");

      // this may throw.
      PgConnection raw = Failsafe.with(RETRY_POLICY)
          .onFailedAttempt(e -> log.warn("connection failed, retying {}", e.getMessage()))
          .get(() -> thd.pool.createConnection());

      thd.conn = new PgLocalConnection(thd.pool, raw);

      if (thd.pool.getListener() != null) {
        thd.pool.getListener().connectionCreated(thd.conn);
      }

    }
    return thd.conn;

  }

  public static void close() {
    log.debug("connection thread closing");
    final PgConnectionThread thd = (PgConnectionThread) Thread.currentThread();
    if (thd.conn != null) {
      if (thd.pool.getListener() != null) {
        thd.pool.getListener().connectionClosed(thd.conn);
      }
      thd.conn.close();
      thd.conn = null;
    }
  }

}
