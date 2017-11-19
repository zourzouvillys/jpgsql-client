package io.zrz.jpgsql.client;

import java.util.Optional;

public interface ResultRow {

  int statementId();

  ResultField field(final int index);

  int fields();

  int intval(final int field);

  int intval(final int field, final int defaultValue);

  String strval(final int field);

  Optional<byte[]> bytes(final int field);

  long longval(final int field);

}
