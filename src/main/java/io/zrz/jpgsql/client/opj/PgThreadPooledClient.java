package io.zrz.jpgsql.client.opj;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Optional;

import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.PreferQueryMode;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.zrz.jpgsql.client.AbstractPostgresClient;
import io.zrz.jpgsql.client.NotifyMessage;
import io.zrz.jpgsql.client.PostgresClient;
import io.zrz.jpgsql.client.PostgresConnectionProperties;
import io.zrz.jpgsql.client.Query;
import io.zrz.jpgsql.client.QueryParameters;
import io.zrz.jpgsql.client.QueryResult;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * the primary run thread never block on DB io. Instead, we expose a
 * Connection-like API that only deals with prepared statements.
 *
 * the connections are run on background threads. we leverage the caching thread
 * pools to avoid doing much ourselves.
 *
 */

@Slf4j
public class PgThreadPooledClient extends AbstractPostgresClient implements PostgresClient {

  private final PgConnectionThreadPoolExecutor pool;

  @Getter
  private final PostgresConnectionProperties config;

  // we use the basic datasource.
  private final PGSimpleDataSource ds;

  PgThreadPooledClient(PostgresConnectionProperties config) {
    this.pool = new PgConnectionThreadPoolExecutor(this, config);
    this.config = config;
    this.ds = new PGSimpleDataSource();
    this.ds.setReWriteBatchedInserts(false);
    this.ds.setAssumeMinServerVersion("9.5");
    this.ds.setPreparedStatementCacheQueries(10000);
    this.ds.setPreparedStatementCacheSizeMiB(10);
    this.ds.setPrepareThreshold(1);
    this.ds.setSendBufferSize(1028 * 32);
    this.ds.setLogUnclosedConnections(true);
    this.ds.setDisableColumnSanitiser(true);
    this.ds.setConnectTimeout(1000);
    this.ds.setPreferQueryMode(PreferQueryMode.EXTENDED_CACHE_EVERYTHING);
    this.ds.setTcpKeepAlive(true);
    this.ds.setDatabaseName(Optional.ofNullable(config.getDbname()).orElse("saasy"));
  }

  PgConnection createConnection() throws SQLException {
    // new connection
    return (PgConnection) this.ds.getConnection();
  }

  @Override
  public Flowable<QueryResult> submit(Query query, QueryParameters params) {
    return Flowable.create(emitter -> {
      try {
        final PgQueryRunner runner = new PgQueryRunner(query, params, emitter);
        this.pool.execute(runner);
      } catch (final Exception ex) {
        log.warn("failed to dispatch work", ex);
        emitter.onError(ex);
      }
    }, BackpressureStrategy.ERROR);
  }

  @Override
  public Flowable<QueryResult> submit(Query query) {
    return this.submit(query, null);
  }

  @Override
  public PgTransactionalSession open() {
    final PgTransactionalSession runner = new PgTransactionalSession(this);
    try {
      this.pool.execute(runner);
    } catch (final Exception ex) {
      log.warn("failed to dispatch work", ex);
      runner.failed(ex);
    }
    return runner;
  }

  public static PgThreadPooledClient create(PostgresConnectionProperties config) {
    return new PgThreadPooledClient(config);
  }

  public static PgThreadPooledClient create(String hostname, String dbname) {
    return create(hostname, dbname, 10);
  }

  public static PgThreadPooledClient create(String hostname, String dbname, int maxPoolSize) {
    return create(PostgresConnectionProperties.builder()
        .hostname(hostname)
        .dbname(dbname)
        .maxPoolSize(maxPoolSize)
        .build());
  }

  /**
   * Opens a connection which monitors for NOTIFY messages.
   *
   * @param channels
   */

  @Override
  public Flowable<NotifyMessage> notifications(Collection<String> channels) {
    return Flowable.create(emitter -> {
      final PgConnectionThread thd = new PgConnectionThread(this, () -> {
        try {
          final PgLocalConnection conn = PgConnectionThread.connection();
          try {
            conn.notifications(channels, emitter);
          } finally {
            conn.close();
          }
        } catch (final Throwable th) {
          emitter.onError(th);
        }
      });
      thd.start();
      log.info("started notify thread");
    }, BackpressureStrategy.BUFFER);
  }

  public void shutdown() {
    this.pool.shutdownNow();
  }

}
