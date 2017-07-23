package io.zrz.jpgsql.client.opj;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Publisher;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.subjects.SingleSubject;
import io.zrz.jpgsql.client.PostgresqlUnavailableException;
import io.zrz.jpgsql.client.Query;
import io.zrz.jpgsql.client.QueryParameters;
import io.zrz.jpgsql.client.QueryResult;
import io.zrz.jpgsql.client.SessionTxnState;
import io.zrz.jpgsql.client.TransactionalSession;
import io.zrz.jpgsql.client.TransactionalSessionDeadlineExceededException;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * a seized connection for a consumer who is performing multiple operations on a
 * txn.
 */

@Slf4j
class PgSessionRunner implements TransactionalSession, Runnable {

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

  PgSessionRunner() {
  }

  /*
   * provide the handle for observing our state.
   */

  @Override
  public CompletableFuture<SessionTxnState> txnstate() {
    final CompletableFuture<SessionTxnState> future = new CompletableFuture<>();
    this.txnstate
        .doOnError(future::completeExceptionally)
        .subscribe(future::complete);
    return future;
  }

  /*
   * called from the consumer thread.
   *
   * we still use a Flowable so that we can stop fetching from the server on
   * huge datasets if the consumer isn't keeping up.
   *
   * however, we need to make it hot - otherwise it will never start, which
   * would be unexpected for submitting a simple "COMMMIT". although consumers
   * should really be checkign result status ... ahem.
   *
   */

  @Override
  public Publisher<QueryResult> submit(Query query, QueryParameters params) {
    final Flowable<QueryResult> flowable = Flowable.create(emitter -> {
      log.debug("added work item");
      this.workqueue.add(new Work(query, params, emitter));
    }, BackpressureStrategy.BUFFER);
    flowable.publish().connect();
    return flowable;
  }

  /*
   * run in the thread with the connection
   *
   * any exception propogated from here will dispatch an onError on the
   * txnstate.
   *
   */

  private void run(PgLocalConnection conn) throws SQLException, InterruptedException {

    Instant startidle = Instant.now();

    while (true) {

      final Work work = this.workqueue.poll(LOOP_WAIT.toNanos(), TimeUnit.NANOSECONDS);

      if (work != null) {

        startidle = null;
        conn.execute(work.getQuery(), work.getParams(), work.getEmitter());
        startidle = Instant.now();

      } else {

        final Duration idle = Duration.between(startidle, Instant.now());

        if (idle.compareTo(MAX_IDLE) > 0) {
          log.warn("aborting transaction due to {} idle", idle);
          conn.rollback();
          this.txnstate.onError(new TransactionalSessionDeadlineExceededException());
          return;
        }

        log.warn("idle {} loops in open transaction", Duration.between(startidle, Instant.now()));

      }

      log.debug("txn state now {}", conn.transactionState());

      switch (conn.transactionState()) {
        case IDLE:
          this.txnstate.onSuccess(SessionTxnState.Closed);
          return;
        case FAILED:
          this.txnstate.onSuccess(SessionTxnState.Error);
          return;
        case OPEN:
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
    } catch (final SQLException ex) {
      // Any propagated SQLException results in the connection being closed.
      PgConnectionThread.close();
      this.txnstate.onError(new PostgresqlUnavailableException(ex));
    } catch (final Exception ex) {
      // TODO: release the transaction, but don't close the connection.
      this.txnstate.onError(ex);
    }
  }

  public void failed(Exception ex) {
    // TODO Auto-generated method stub

  }

}
