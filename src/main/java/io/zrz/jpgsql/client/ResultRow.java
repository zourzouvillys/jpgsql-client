package io.zrz.jpgsql.client;

import java.time.Instant;
import java.util.Optional;

public interface ResultRow {

  int statementId();

  ResultField field(final int index);

  int fields();

  int intval(final int field);

  int intval(final int field, final int defaultValue);

  default int intval(final String label) {
    return intval(this.field(label).column());
  }

  default long intval(final String label, final int defaultValue) {
    return intval(this.field(label).column(), defaultValue);
  }
  
  double doubleval(final int field);


  String strval(final int field);

  default String strval(final int field, final String defaultValue) {
    final String value = strval(field);
    if (value == null) {
      return defaultValue;
    }
    return value;
  }

  Optional<byte[]> bytes(final int field);

  long longval(final int field);

  long longval(int field, long defaultValue);

  default long longval(final String label, final long defaultValue) {
    return longval(this.field(label).column(), defaultValue);
  }

  boolean boolval(int field);

  byte[] bytes(int field, byte[] defaultValue);

  byte[] bytea(int i);

  Instant instant(int i);

  int[] int2vector(int column);

  default boolean isNull(final int index) {
    return bytes(index) == null;
  }

  default boolean isNull(final String name) {
    return bytes(field(name).column()) == null;
  }

  /**
   * the row id within the statement result set.
   */

  int rowId();

  default String strval(final String name) {
    return strval(this.field(name).column());
  }

  ResultField field(String label);

  default boolean boolval(final String label) {
    return boolval(field(label).column());
  }

  default int[] int2vector(final String label) {
    return int2vector(field(label).column());
  }

  default Optional<byte[]> bytes(final String label) {
    return bytes(field(label).column());
  }

  default byte[] bytea(final String label) {
    return bytea(field(label).column());
  }

  default long longval(final String label) {
    return longval(field(label).column());
  }

  default Instant instant(final String label) {
    return instant(field(label).column());
  }

}
