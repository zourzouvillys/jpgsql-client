package io.zrz.jpgsql.client.opj;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Publisher;

import io.netty.buffer.ByteBuf;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.SingleSubject;
import io.zrz.jpgsql.client.AbstractQueryExecutionBuilder.Tuple;
import io.zrz.jpgsql.client.PgSession;
import io.zrz.jpgsql.client.PostgresqlUnavailableException;
import io.zrz.jpgsql.client.Query;
import io.zrz.jpgsql.client.QueryParameters;
import io.zrz.jpgsql.client.QueryResult;
import io.zrz.jpgsql.client.SessionTxnState;
import io.zrz.jpgsql.client.TransactionalSessionDeadlineExceededException;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * a single seized connection, outside the scope of a transaction (although transactions may be started).
 */

@Slf4j
class PgSingleSession implements Runnable, PgSession {

  @Value
  private static class Work {
    private Query query;
    private QueryParameters params;
    private FlowableEmitter<QueryResult> emitter;
  }

  private static final Duration LOOP_WAIT = Duration.ofSeconds(1);
  private static final Duration MAX_IDLE = Duration.ofSeconds(5);

  private final SingleSubject<SessionTxnState> txnstate = SingleSubject.create();
  private final LinkedTransferQueue<Work> workqueue = new LinkedTransferQueue<>();

  // if we are accepting work still
  private boolean accepting = true;
  private final PgThreadPooledClient pool;

  PgSingleSession(PgThreadPooledClient pool) {
    this.pool = pool;
  }

  @Override
  public Flowable<QueryResult> submit(Query query, QueryParameters params) {

    if (!this.accepting) {
      throw new IllegalStateException(String.format("This session is no longer active"));
    }

    final Flowable<QueryResult> flowable = Flowable.create(emitter -> {

      log.debug("added work item: {}", query);
      this.workqueue.add(new Work(query, params, emitter));

    }, BackpressureStrategy.BUFFER);

    return flowable
        .publish()
        .autoConnect()
        .observeOn(Schedulers.computation())
        .doOnEach(e -> log.debug("notif: {}", e));

  }

  @Override
  public Publisher<Long> copyTo(String sql, Publisher<ByteBuf> data) {
    throw new IllegalArgumentException();
  }

  /*
   * run in the thread with the connection any exception propogated from here will dispatch an onError on the txnstate.
   */

  private void run(PgLocalConnection conn) throws SQLException, InterruptedException {

    log.debug("starting session");

    conn.getConnection().setAutoCommit(false);

    Instant startidle = Instant.now();

    while (true) {

      final Work work = this.workqueue.poll(LOOP_WAIT.toNanos(), TimeUnit.NANOSECONDS);

      if (work != null) {

        if (work.getEmitter() == null) {

          log.debug("no emitter - rolling back, work was {}", work);
          conn.rollback();

        }
        else {

          log.info("processing work item {}", work);
          startidle = null;
          conn.execute(work.getQuery(), work.getParams(), work.getEmitter(), 0, PgLocalConnection.SuppressBegin);

        }
        startidle = Instant.now();

      }
      else {

        final Duration idle = Duration.between(startidle, Instant.now());

        if (idle.compareTo(MAX_IDLE) > 0) {
          log.warn("aborting transaction due to {} idle", idle);
          this.accepting = false;
          conn.rollback();
          this.txnstate.onError(new TransactionalSessionDeadlineExceededException());
          return;
        }

        log.warn("idle {} loops in open transaction", Duration.between(startidle, Instant.now()));

      }

      log.debug("txn state now {}", conn.transactionState());

      switch (conn.transactionState()) {
        case IDLE:
          // this.accepting = false;
          // this.txnstate.onSuccess(SessionTxnState.Closed);
          // if (!this.workqueue.isEmpty()) {
          // log.warn("work queue is not empty after session completed");
          // this.workqueue.forEach(e -> {
          // if (e.emitter != null)
          // e.emitter.onError(new IllegalStateException("Session has already completed (in IDLE)"));
          // });
          // }
          break;
        case FAILED:
          this.accepting = false;
          this.txnstate.onSuccess(SessionTxnState.Error);
          if (!this.workqueue.isEmpty()) {
            log.warn("work queue is not empty after session completed");
            this.workqueue.forEach(e -> {
              if (e.emitter != null)
                e.emitter.onError(new IllegalStateException("Session has already completed (in FAILED) for " + e.getQuery()));
            });
          }
          return;
        case OPEN:
          if (!this.accepting && this.workqueue.isEmpty()) {
            // rollback - which will terminate us.
            log.info("rolling back");
            conn.rollback();
          }
          // still going ..
          break;
      }

    }

  }

  /*
   * called when the job is allocated - runs in the thread.
   */

  @Override
  public void run() {
    log.trace("running query");
    try {
      this.run(PgConnectionThread.connection());
    }
    catch (final SQLException ex) {
      // Any propagated SQLException results in the connection being closed.
      log.warn("connection failed: {}", ex.getMessage(), ex);
      this.accepting = false;
      PgConnectionThread.close();
      this.txnstate.onError(new PostgresqlUnavailableException(ex));
    }
    catch (final Exception ex) {
      log.warn("connection failed: {}", ex.getMessage(), ex);
      this.accepting = false;
      // TODO: release the transaction, but don't close the connection.
      this.txnstate.onError(ex);
    }
  }

  /*
   * rollback the transaction if there is one.
   */

  @Override
  public void close() {
    // Preconditions.checkState(this.accepting, "session is no longer active");
    if (accepting) {
      this.accepting = false;
      this.workqueue.add(new Work(this.pool.createQuery("ROLLBACK"), null, null));
    }
  }

  /*
   * called from the consumer thread. jusr raise the failure, nothing else.
   */

  public void failed(Exception ex) {
    this.accepting = false;
    this.txnstate.onError(ex);
  }

  @Override
  public Query createQuery(String sql, int paramcount) {
    return this.pool.createQuery(sql, paramcount);
  }

  @Override
  public Query createQuery(List<Query> combine) {
    return this.pool.createQuery(combine);
  }

  @Override
  public Flowable<QueryResult> fetch(int batchSize, Tuple query) {
    throw new IllegalArgumentException();
  }

}
