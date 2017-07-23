package io.zrz.jpgsql.client.opj;

import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
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

  public PgConnectionThreadPoolExecutor(PgThreadPooledClient pool, PostgresConnectionProperties config) {
    super(
        config.getMinIdle(),
        config.getMaxPoolSize(),
        config.getIdleTimeout().map(Duration::toMillis).orElse(0L),
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(config.getMaxPoolSize() + config.getQueueDepth()));

    this.pool = pool;

    super.setRejectedExecutionHandler(this);
    super.setThreadFactory(this);

  }

  /**
   * if the execution queue is full, then reject at submission time.
   */

  @Override
  public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
    log.error("execution of {} rejected", r);
    throw new PostgresqlCapacityExceededException();
  }

  @Override
  public Thread newThread(Runnable r) {
    return new PgConnectionThread(this.pool, r);
  }

}
