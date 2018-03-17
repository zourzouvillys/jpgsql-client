package io.zrz.sqlwriter;

import java.time.Duration;

public class SqlUtils {

  public static String toSqlString(Duration d) {
    return d.toString();
  }

}
