package io.zrz.jpgsql.client;

public enum SessionTxnState {

  /**
   * The session is open. It must be closed by sending a COMMIT or ROLLBACK.
   */

  Open,

  /**
   * An error occured in processing, so the transaction has been rolled back.
   */

  Error,

  /**
   * The transaction was closed - it has either been commited or aborted.
   */

  Closed

}
