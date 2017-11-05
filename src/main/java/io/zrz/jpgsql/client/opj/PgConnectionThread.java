package io.zrz.jpgsql.client.opj;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.postgresql.jdbc.PgConnection;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

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
      final ResultSet res = connection().getConnection().execSQLQuery("SELECT 1");
      log.debug("established connection");
      res.close();

      // the loop
      this.run.run();

      log.debug("thread finished");

    } catch (final Throwable t) {
      log.info("connection thread error", t);
      throw t;
    } finally {
      close();
    }

  }

  public static PgLocalConnection connection() throws SQLException {

    final PgConnectionThread thd = (PgConnectionThread) Thread.currentThread();

    if (thd.conn == null) {

      log.debug("Creating new connection thread");

      thd.conn = new PgLocalConnection(thd.pool, thd.pool.createConnection());

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
