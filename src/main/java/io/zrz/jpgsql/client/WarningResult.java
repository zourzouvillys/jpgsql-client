package io.zrz.jpgsql.client;

import java.sql.SQLWarning;

public final class WarningResult implements QueryResult {

  public WarningResult(SQLWarning warnings) {
  }

}
