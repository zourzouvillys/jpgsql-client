package io.zrz.jpgsql.client;

import lombok.Value;

/**
 * For Queries which do not provide a result set, this is returned on completion
 * instead.
 */

@Value
public class CommandStatus implements QueryResult {
  private String status;
  private int updateCount;
  private long insertOID;
}
