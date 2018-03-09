package io.zrz.jpgsql.client;

import java.time.Instant;
import java.util.Optional;

public interface ResultRow {

  int statementId();

  ResultField field(final int index);

  int fields();

  int intval(final int field);

  int intval(final int field, final int defaultValue);

  String strval(final int field);

  default String strval(final int field, String defaultValue) {
    String value = strval(field);
    if (value == null) {
      return defaultValue;
    }
    return value;
  }

  Optional<byte[]> bytes(final int field);

  long longval(final int field);

  boolean boolval(int field);

  byte[] bytes(int field, byte[] defaultValue);

  byte[] bytea(int i);

  Instant instant(int i);

  default boolean isNull(int index) {
    return bytes(index) == null;
  }

  /**
   * the row id within the statement result set.
   */

  int rowId();

}
