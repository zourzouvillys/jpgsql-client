package io.zrz.jpgsql.client.opj;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.postgresql.PGProperty;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.PGCopyOutputStream;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.PreferQueryMode;
import org.postgresql.util.HostSpec;
import org.reactivestreams.Publisher;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import io.netty.buffer.ByteBuf;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import io.zrz.jpgsql.client.AbstractPostgresClient;
import io.zrz.jpgsql.client.AbstractQueryExecutionBuilder.Tuple;
import io.zrz.jpgsql.client.ErrorResult;
import io.zrz.jpgsql.client.NotifyMessage;
import io.zrz.jpgsql.client.PostgresClient;
import io.zrz.jpgsql.client.PostgresConnectionProperties;
import io.zrz.jpgsql.client.Query;
import io.zrz.jpgsql.client.QueryParameters;
import io.zrz.jpgsql.client.QueryResult;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

/**
 * the primary run thread never block on DB io. Instead, we expose a Connection-like API that only deals with prepared
 * statements. the connections are run on background threads. we leverage the caching thread pools to avoid doing much
 * ourselves.
 */

@Slf4j
public class PgThreadPooledClient extends AbstractPostgresClient implements PostgresClient, AutoCloseable {

  private final PgConnectionThreadPoolExecutor pool;

  @Getter
  private final PostgresConnectionProperties config;

  // we use the basic datasource.
  private final PGSimpleDataSource ds;

  @Getter
  private final Listener listener;

  private RetryPolicy retryPolicy = new RetryPolicy()
      .withDelay(1, TimeUnit.SECONDS)
      .withMaxRetries(10);

  /**
   *
   */

  public static interface Listener {

    /**
     * called from a thread when a new connection is established.
     */

    void connectionCreated(PgRawConnection conn);

    void connectionClosed(PgRawConnection conn);

    void executingQuery(PgRawConnection conn, Query query, QueryParameters params);

    void queryCompleted(PgRawConnection conn);

  }

  PgThreadPooledClient(PostgresConnectionProperties config, final Listener listener) {

    if (config == null) {
      config = PostgresConnectionProperties.builder().build();
    }

    this.listener = listener;

    this.config = config;

    this.ds = new PGSimpleDataSource();

    this.ds.setServerName(config.getHostname());

    if (config.getPort() != 0)
      this.ds.setPortNumber(config.getPort());

    this.ds.setUser(this.getUsername());

    if (config.getPassword() != null) {
      this.ds.setPassword(config.getPassword());
    }

    if (config.getDbname() != null) {
      this.ds.setDatabaseName(config.getDbname());
    }

    if (config.getApplicationName() == null)
      this.ds.setApplicationName("jpgsql");
    else
      this.ds.setApplicationName(config.getApplicationName());

    this.ds.setReWriteBatchedInserts(false);
    this.ds.setAssumeMinServerVersion("9.6");
    this.ds.setSendBufferSize(config.getSendBufferSize());
    this.ds.setReceiveBufferSize(config.getRecvBufferSize());
    this.ds.setLogUnclosedConnections(true);
    this.ds.setDisableColumnSanitiser(true);

    //
    // this.ds.setPreparedStatementCacheQueries(0);
    // this.ds.setPreparedStatementCacheSizeMiB(0);
    this.ds.setPrepareThreshold(-1); // force binary
    this.ds.setPreferQueryMode(PreferQueryMode.EXTENDED);

    this.ds.setBinaryTransfer(true);

    // reasonably large default fetch size

    if (config.getDefaultRowFetchSize() > 0)
      this.ds.setDefaultRowFetchSize(config.getDefaultRowFetchSize());

    // this.ds.setLoggerLevel("loggerLevel");

    this.ds.setSsl(this.config.isSsl());

    if (this.config.isSsl()) {
      this.ds.setSslMode(this.config.getSslMode());
    }

    if (this.config.getSocketTimeout() != null) {
      this.ds.setSocketTimeout((int) Math.ceil(config.getSocketTimeout().toMillis() / 1000.0));
    }

    this.ds.setConnectTimeout((Ints.checkedCast(config.getConnectTimeout().toMillis()) / 1000) + 1);
    this.ds.setTcpKeepAlive(true);

    if (this.config.isReadOnly()) {
      this.ds.setReadOnly(true);
    }

    log.debug("connparm {}", config);

    this.pool = new PgConnectionThreadPoolExecutor(this, config);

  }

  String getUsername() {
    return (this.config.getUsername() == null) ? System.getProperty("user.name") : this.config.getUsername();
  }

  /**
   * blocks until a connection is active.
   */

  public PgConnection createConnection() {

    // new connection
    Preconditions.checkState(this.ds != null);

    HostSpec spec = new HostSpec(config.getHostname(), config.getPort() == 0 ? 5432 : config.getPort());
    final Properties info = org.postgresql.Driver.parseURL(ds.getUrl(), new Properties());

    // ahem
    info.setProperty("charSet", "UTF-8");
    info.setProperty("characterEncoding", "UTF-8");
    info.setProperty("useUnicode", "true");

    PGProperty.SEND_BUFFER_SIZE.set(info, config.getSendBufferSize());

    if (config.getPassword() != null) {
      info.setProperty("password", config.getPassword());
    }

    return Failsafe.with(retryPolicy)
        .onFailure(err -> log.error("error opening connection: {}", err.getMessage(), err))
        .get(() -> new PgConnection(new HostSpec[] { spec }, this.getUsername(), this.config.getDbname(), info, ds.getUrl()));

  }

  /**
   * non blocking reactive fetch of a connection.
   */

  public Single<PgConnection> requestConnection() {

    // new connection
    Preconditions.checkState(this.ds != null);

    HostSpec spec = new HostSpec(config.getHostname(), config.getPort() == 0 ? 5432 : config.getPort());
    final Properties info = org.postgresql.Driver.parseURL(ds.getUrl(), new Properties());

    // ahem
    info.setProperty("charSet", "UTF-8");
    info.setProperty("characterEncoding", "UTF-8");
    info.setProperty("useUnicode", "true");

    PGProperty.SEND_BUFFER_SIZE.set(info, 1024 * 1024);

    if (config.getPassword() != null) {
      info.setProperty("password", config.getPassword());
    }

    return Single.just(Failsafe.with(retryPolicy)
        .onFailure(err -> log.error("error opening connection: {}", err.getMessage(), err))
        .get(() -> new PgConnection(new HostSpec[] { spec }, this.getUsername(), this.config.getDbname(), info, ds.getUrl())));

  }

  @Override
  public Flowable<QueryResult> submit(final Query query, final QueryParameters params) {
    return submit(query, params, 0);
  }

  public Flowable<QueryResult> submit(final Query query, final QueryParameters params, int fetchSize) {

    final AmbientContext ctx = AmbientContext.capture();
    Preconditions.checkState(!pool.isShutdown(), query.toString());

    Flowable<QueryResult> res = Flowable.create(emitter -> {
      try {
        final PgQueryRunner runner = new PgQueryRunner(query, params, emitter, ctx, fetchSize);
        this.pool.execute(ctx.wrap(runner));
      }
      catch (final Throwable ex) {
        log.warn("failed to dispatch work", ex.getMessage());
        emitter.onError(ex);
      }
    }, BackpressureStrategy.BUFFER);

    // map so we have the stacktrace from caller, not nested.
    // StackTraceElement[] trace = Thread.currentThread().getStackTrace();
    PostgresQueryException trace = new PostgresQueryException(query);

    return res
        .onErrorResumeNext(err -> {
          // err.addSuppressed(err);
          trace.initCause(err);
          if (err instanceof ErrorResult) {
            trace.setErrorResult((ErrorResult) err);
          }
          return Flowable.error(trace);
        })
        // always submit responses on a trampoline thread, to avoid blocking the pool.
        .rebatchRequests(8)
        .observeOn(Schedulers.computation(), true);

  }

  @Override
  public Flowable<QueryResult> fetch(int fetchSize, Tuple tuple) {
    return submit(tuple.getQuery(), tuple.getParams(), fetchSize);
  }

  @Override
  public Flowable<QueryResult> submit(final Query query) {
    return this.submit(query, null);

  }

  @Override
  public Flowable<QueryResult> submit(final String sql) {
    return this.submit(this.createQuery(sql));
  }

  @Override
  public Flowable<QueryResult> submit(final String sql, final Object... params) {
    final Query query = this.createQuery(sql, params.length);
    final QueryParameters qp = query.createParameters();
    qp.setFrom(params);
    return this.submit(query, qp);
  }

  @Override
  public PgTransactionalSession open() {
    log.debug("opening transactional session");
    final PgTransactionalSession runner = new PgTransactionalSession(this);
    try {
      this.pool.execute(runner);
    }
    catch (final Exception ex) {
      log.warn("failed to open session", ex);
      runner.failed(ex);
    }
    return runner;
  }

  @Override
  public PgSingleSession openSession() {
    log.debug("opening single session");
    final PgSingleSession runner = new PgSingleSession(this);
    try {
      this.pool.execute(runner);
    }
    catch (final Exception ex) {
      log.warn("failed to open session", ex);
      runner.failed(ex);
    }
    return runner;
  }

  public static PgThreadPooledClient create(final PostgresConnectionProperties config, final Listener listener) {
    return new PgThreadPooledClient(config, listener);
  }

  public static PgThreadPooledClient create(final PostgresConnectionProperties config) {
    return create(config, null);
  }

  public static PgThreadPooledClient create(final String hostname, final String dbname) {
    return create(hostname, dbname, 8);
  }

  public static PgThreadPooledClient create(final String hostname, final String dbname, final int maxPoolSize) {
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
  public Flowable<NotifyMessage> notifications(final Collection<String> channels) {
    return Flowable.create(emitter -> {
      final PgConnectionThread thd = new PgConnectionThread(this, () -> {
        try {
          final PgLocalConnection conn = PgConnectionThread.connection();
          try {
            conn.notifications(channels, emitter);
          }
          finally {
            conn.close();
          }
        }
        catch (final Throwable th) {
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

  @Override
  public void close() {
    this.shutdown();
  }

  public static final byte[] BINARY_PREAMBLE = new byte[] {
      'P',
      'G',
      'C',
      'O',
      'P',
      'Y',
      '\n',
      -1,
      '\r',
      '\n',
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0
  };

  /**
   * start a new connection and copy to it.
   */

  @SneakyThrows
  @Override
  public Publisher<Long> copyTo(String sql, Publisher<ByteBuf> data) {

    Flowable<ByteBuf> upstream = Flowable.fromPublisher(data);

    return this.requestConnection()
        .flatMapCompletable(conn -> {

          CopyIn copy = conn.getCopyAPI().copyIn(sql);
          PGCopyOutputStream out = new PGCopyOutputStream(copy, 1024 * 1024 * 8);

          out.write(BINARY_PREAMBLE);

          return upstream
              // .observeOn(Schedulers.newThread())
              .doOnNext(buf -> {

                while (buf.isReadable()) {
                  buf.readBytes(out, buf.readableBytes());
                }
                buf.release();

              })
              .ignoreElements()
              .doOnComplete(() -> {

                out.close();

              })
              .doAfterTerminate(conn::close);

        })
        .toSingleDefault(1L)
        .toFlowable();

  }

  @Override
  public PostgresConnectionProperties config() {
    return config;
  }

  @Override
  public PostgresClient client() {
    return this;
  }

}
