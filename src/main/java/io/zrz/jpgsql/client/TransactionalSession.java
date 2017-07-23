package io.zrz.jpgsql.client;

import java.util.concurrent.CompletableFuture;

/**
 * A single active transaction with a server.
 */

public interface TransactionalSession extends PostgresQueryProcessor {

  /**
   * A single value which emits when the state changes to
   * {@link SessionTxnState#Closed} or {@link SessionTxnState#Error}.
   */

  CompletableFuture<SessionTxnState> txnstate();

}
