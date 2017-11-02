package io.zrz.jpgsql.client.opj;

import java.sql.SQLException;

import org.postgresql.jdbc.PgConnection;

/**
 * Thread that interacts with the {@link PgConnection}.
 */

public class PgConnectionThread extends Thread {

  private final PgThreadPooledClient pool;
  private PgLocalConnection conn;
  private final Runnable run;

  public PgConnectionThread(final PgThreadPooledClient pool, final Runnable run) {
    super();
    this.setDaemon(true);
    this.run = run;
    this.pool = pool;
  }

  @Override
  public void run() {
    try {
      this.run.run();
    } finally {
      close();
    }
  }

  public static PgLocalConnection connection() throws SQLException {

    final PgConnectionThread thd = (PgConnectionThread) Thread.currentThread();

    if (thd.conn == null) {

      thd.conn = new PgLocalConnection(thd.pool, thd.pool.createConnection());

      if (thd.pool.getListener() != null) {
        thd.pool.getListener().connectionCreated(thd.conn);
      }

    }
    return thd.conn;

  }

  public static void close() {
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
