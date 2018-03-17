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

  int[] int2vector(int column);

  default boolean isNull(int index) {
    return bytes(index) == null;
  }

  /**
   * the row id within the statement result set.
   */

  int rowId();

  default String strval(String name) {
    return strval(this.field(name).column());
  }

  ResultField field(String label);

  default int intval(String label) {
    return intval(this.field(label).column());
  }

  default boolean boolval(String label) {
    return boolval(field(label).column());
  }

  default int[] int2vector(String label) {
    return int2vector(field(label).column());
  }

  default Optional<byte[]> bytes(String label) {
    return bytes(field(label).column());
  }

  default byte[] bytea(String label) {
    return bytea(field(label).column());
  }

  default long longval(String label) {
    return longval(field(label).column());
  }

  default Instant instant(String label) {
    return instant(field(label).column());
  }

}
