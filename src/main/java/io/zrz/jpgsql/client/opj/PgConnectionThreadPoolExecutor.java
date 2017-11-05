package io.zrz.jpgsql.client.opj;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.zrz.jpgsql.client.PostgresConnectionProperties;
import io.zrz.jpgsql.client.PostgresqlCapacityExceededException;
import lombok.extern.slf4j.Slf4j;

/**
 * responsible for keeping the pool of connections open.
 */

@Slf4j
public class PgConnectionThreadPoolExecutor extends ThreadPoolExecutor implements ThreadFactory, RejectedExecutionHandler {

  private final PgThreadPooledClient pool;

  public PgConnectionThreadPoolExecutor(final PgThreadPooledClient pool, final PostgresConnectionProperties config) {

    super(
        0,
        config.getMaxPoolSize(),
        config.getIdleTimeout().toMillis(),
        TimeUnit.MILLISECONDS,
        new SynchronousQueue<Runnable>());

    // new LinkedBlockingQueue<Runnable>(config.getMaxPoolSize() +
    // config.getQueueDepth() + 1));

    this.pool = pool;

    super.setRejectedExecutionHandler(this);
    super.setThreadFactory(this);

    super.setCorePoolSize(1);
    super.setMaximumPoolSize(16);

    this.prestartAllCoreThreads();

    log.info("prestarting : core={} max={} size={}", this.getCorePoolSize(), this.getMaximumPoolSize(), this.getPoolSize());

  }

  /**
   * if the execution queue is full, then reject at submission time.
   */

  @Override
  public void rejectedExecution(final Runnable r, final ThreadPoolExecutor e) {

    log.error("execution of {} rejected", r);

    throw new PostgresqlCapacityExceededException();

  }

  @Override
  protected void beforeExecute(final Thread t, final Runnable r) {
    super.beforeExecute(t, r);
  }

  @Override
  protected void afterExecute(final Runnable r, final Throwable t) {
    super.afterExecute(r, t);
  }

  @Override
  public Thread newThread(final Runnable r) {
    return new PgConnectionThread(this.pool, r);
  }

}
