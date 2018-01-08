package io.zrz.jpgsql.client.opj;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.zrz.jpgsql.client.PostgresConnectionProperties;
import io.zrz.jpgsql.client.PostgresqlCapacityExceededException;
import lombok.extern.slf4j.Slf4j;

/**
 * responsible for keeping the pool of connections open.
 */

@Slf4j
public class PgConnectionThreadPoolExecutor extends ThreadPoolExecutor implements ThreadFactory, RejectedExecutionHandler, UncaughtExceptionHandler {

  private final PgThreadPooledClient pool;

  public PgConnectionThreadPoolExecutor(final PgThreadPooledClient pool, final PostgresConnectionProperties config) {

    super(
        0,
        config.getMaxPoolSize(),
        config.getIdleTimeout().toMillis(),
        TimeUnit.MILLISECONDS,
        (config.getQueueDepth() == 0)
            ? new SynchronousQueue<Runnable>()
            : new LinkedBlockingQueue<Runnable>(config.getMaxPoolSize() + config.getQueueDepth() + 1))

    ;
    // ;

    // super.setThreadFactory(this);

    this.setThreadFactory(new ThreadFactoryBuilder()
        .setThreadFactory(this)
        .setUncaughtExceptionHandler(this)
        .setNameFormat("psql-%d-" + Integer.toHexString(this.hashCode()))
        .build());

    this.pool = pool;

    super.setRejectedExecutionHandler(this);

    super.setCorePoolSize(config.getMinIdle() + 1);
    super.setMaximumPoolSize(config.getMaxPoolSize());

    this.prestartAllCoreThreads();

    log.debug("prestarting : core={} max={} size={}", this.getCorePoolSize(), this.getMaximumPoolSize(), this.getPoolSize());

  }

  /**
   * if the execution queue is full, then reject at submission time.
   */

  @Override
  public void rejectedExecution(final Runnable r, final ThreadPoolExecutor e) {
    log.error("execution of {} rejected terminated={}, shutdown={}", r, this.isTerminating(), this.isShutdown());
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

  @Override
  public void uncaughtException(Thread t, Throwable e) {
    log.error("error in group " + t.getThreadGroup().getName() + " failed with an uncaught exception", e);
  }

}
