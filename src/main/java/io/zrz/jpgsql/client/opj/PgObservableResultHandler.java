package io.zrz.jpgsql.client.opj;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.postgresql.core.Field;
import org.postgresql.core.ResultCursor;
import org.postgresql.core.ResultHandlerBase;

import io.reactivex.FlowableEmitter;
import io.zrz.jpgsql.client.CommandStatus;
import io.zrz.jpgsql.client.Query;
import io.zrz.jpgsql.client.QueryResult;
import io.zrz.jpgsql.client.SecureProgress;
import lombok.extern.slf4j.Slf4j;

/**
 * converts the responses from postgres to observable events.
 */

@Slf4j
public class PgObservableResultHandler extends ResultHandlerBase {

  private final FlowableEmitter<QueryResult> emitter;
  private final Query query;

  PgObservableResultHandler(Query query, FlowableEmitter<QueryResult> emitter) {
    this.emitter = emitter;
    this.query = query;
  }

  @Override
  public void handleResultRows(org.postgresql.core.Query fromQuery, Field[] fields, List<byte[][]> tuples, ResultCursor cursor) {

    if (cursor != null) {
      log.error("don't support cursor queries - leak probable!");
    }

    log.debug("{} rows for {}[{}]: - >>>> {}: {}", tuples.size(), fromQuery, tuples.size(), fields, Arrays.asList(tuples).stream()
        .flatMap(a -> a.stream())
        .map(col -> "{" + PgResultDecoder.toString(fields[0], col[0]) + "}")
        .collect(Collectors.joining(", ")));

    this.emitter.onNext(new PgResultRows(this.query, fields, tuples));

  }

  @Override
  public void handleCommandStatus(String status, int updateCount, long insertOID) {
    log.debug("comman status={} updateCount={}, insertOID={}", status, updateCount, insertOID);
    this.emitter.onNext(new CommandStatus(status, updateCount, insertOID));
  }

  @Override
  public void handleCompletion() throws SQLException {
    log.debug("finished Query");
    if (this.getException() != null) {
      this.emitter.onError(this.getException());
    } else {
      this.emitter.onComplete();
    }
    // we don't throw, and instead let the emitter take care of the onError.
  }

  @Override
  public void secureProgress() {
    log.debug("secured progress");
    this.emitter.onNext(new SecureProgress());
  }

}
