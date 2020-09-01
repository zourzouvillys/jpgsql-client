package io.zrz.jpgsql.client;

import io.zrz.visitors.annotations.Visitable;
import lombok.Value;

/**
 * For Queries which do not provide a result set, this is returned on completion
 * instead.
 */

@Value
@Visitable.Type
public class CommandStatus implements QueryResult {

  private int statementId;

  private String status;

  private long updateCount;

  private long insertOID;

  @Override
  public int statementId() {
    return this.statementId;
  }

  @Override
  public QueryResultKind getKind() {
    return QueryResultKind.STATUS;
  }

}
