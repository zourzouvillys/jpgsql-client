package io.zrz.jpgsql.client.opj;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

import org.postgresql.copy.CopyIn;
import org.postgresql.jdbc.PgConnection;
import org.reactivestreams.Publisher;

import com.google.common.primitives.Ints;

import io.netty.buffer.ByteBuf;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.SingleSubject;
import io.zrz.jpgsql.client.AbstractQueryExecutionBuilder.Tuple;
import io.zrz.jpgsql.client.CommandStatus;
import io.zrz.jpgsql.client.PgSession;
import io.zrz.jpgsql.client.PostgresqlUnavailableException;
import io.zrz.jpgsql.client.Query;
import io.zrz.jpgsql.client.QueryParameters;
import io.zrz.jpgsql.client.QueryResult;
import io.zrz.jpgsql.client.SessionTxnState;
import io.zrz.jpgsql.client.TransactionalSessionDeadlineExceededException;
import lombok.SneakyThrows;
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
    private Publisher<ByteBuf> source;
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
      this.workqueue.add(new Work(query, params, emitter, null));

    }, BackpressureStrategy.BUFFER);

    return flowable
        .publish()
        .autoConnect()
        .observeOn(Schedulers.computation(), true)
        .doOnEach(e -> log.debug("notif: {}", e));

  }

  @Override
  public Publisher<Long> copyTo(String sql, Publisher<ByteBuf> data) {

    if (!this.accepting) {
      throw new IllegalStateException(String.format("This session is no longer active"));
    }

    final Flowable<QueryResult> flowable = Flowable.create(emitter -> {

      log.debug("starting COPY");
      this.workqueue.add(new Work(this.createQuery(sql), null, emitter, data));

    }, BackpressureStrategy.BUFFER);

    return flowable
        .publish()
        .autoConnect()
        .doOnEach(e -> log.debug("notif: {}", e))
        .map(x -> (long) (((CommandStatus) x).getUpdateCount()))
        .observeOn(Schedulers.computation(), true)
        .singleOrError()
        .toFlowable();

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

        if (work.getQuery() == null && work.getEmitter() == null) {

          log.debug("single session finished");

          switch (conn.transactionState()) {
            case IDLE:
              return;
            case FAILED:
              return;
            case OPEN:
              log.warn("rolling back");
              conn.rollback();
              return;
          }

          return;

        }
        else if (work.getEmitter() == null) {

          log.debug("no emitter - rolling back, work was {}", work);
          conn.rollback();
          return;

        }
        else if (work.getSource() != null) {

          String sql = work.getQuery().statement(0).sql();

          log.info("starting {}", sql);

          try {

            long value = processCopy(conn.getConnection(), sql, work.getSource())
                .blockingGet();

            log.info("copy completed {}", value);

            work.emitter.onNext(new CommandStatus(0, "COPY", Ints.checkedCast(value), 0));

            work.emitter.onComplete();

          }
          catch (Throwable t) {

            log.warn("copy error: {}", t.getMessage(), t);
            work.emitter.onError(t);

          }
          finally {

            log.debug("copy finished");

          }

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

  @SneakyThrows
  private Single<Long> processCopy(PgConnection conn, String sql, Publisher<ByteBuf> source) {

    CopyIn copy = conn.getCopyAPI().copyIn(sql);

    // PGCopyOutputStream out = new PGCopyOutputStream(copy, 1024 * 1024 * 64);

    copy.writeToCopy(PgThreadPooledClient.BINARY_PREAMBLE, 0, PgThreadPooledClient.BINARY_PREAMBLE.length);

    return Flowable.fromPublisher(source)

        .doOnNext(buf -> {

          while (buf.isReadable()) {
            byte[] out = new byte[buf.readableBytes()];
            buf.readBytes(out);
            copy.writeToCopy(out, 0, out.length);
          }

          buf.release();

        })
        .ignoreElements()
        .andThen(Single.defer(() -> {

          log.debug("closing copy stream");

          long len = copy.endCopy();

          log.debug("CopyIn finished, {} rows", len);

          return Single.just(len);

        }));

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
    // if (accepting) {
    // this.accepting = false;
    // this.workqueue.add(new Work(this.pool.createQuery("ROLLBACK"), null, null, null));
    // }

    this.workqueue.add(new Work(null, null, null, null));
    this.accepting = false;

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
