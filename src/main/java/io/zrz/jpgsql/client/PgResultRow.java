package io.zrz.jpgsql.client;

import java.time.Instant;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.postgresql.core.Oid;

public class PgResultRow implements ResultRow {

  private final RowBuffer buffer;
  private final int row;

  public PgResultRow(final RowBuffer buffer, final int row) {
    this.buffer = buffer;
    this.row = row;
  }

  @Override
  public int statementId() {
    return this.buffer.statementId();
  }

  @Override
  public ResultField field(final int index) {
    return this.buffer.field(index);
  }

  @Override
  public int fields() {
    return this.buffer.fields();
  }

  @Override
  public int intval(final int field) {
    return this.buffer.intval(this.row, field);
  }

  @Override
  public int intval(final int field, final int defaultValue) {
    return this.buffer.intval(this.row, field, defaultValue);
  }

  @Override
  public String strval(final int field) {
    return this.buffer.strval(this.row, field);
  }

  @Override
  public byte[] bytes(final int field, byte[] defaultValue) {
    byte[] value = this.buffer.bytes(this.row, field);
    if (value == null)
      return defaultValue;
    return value;
  }

  @Override
  public Optional<byte[]> bytes(final int field) {
    return Optional.ofNullable(this.buffer.bytes(this.row, field));
  }

  @Override
  public long longval(final int field) {
    return this.buffer.longval(this.row, field);
  }

  @Override
  public boolean boolval(int field) {
    return buffer.boolval(this.row, field);
  }

  @Override
  public byte[] bytea(int i) {
    return buffer.bytea(this.row, i);
  }

  public String toString() {
    return "[" + statementId() + ":" + rowId() + "]: " + IntStream.range(0, this.fields())
        .mapToObj(field -> this.field(field).label() + "=" + this.toString(field))
        .collect(Collectors.joining(", ", "{ ", " }"));
  }

  private String toString(int field) {
    switch (this.field(field).oid()) {
      case Oid.BYTEA: {
        byte[] data = bytea(field);
        if (data == null)
          return "(null)";
        return "(" + data.length + " bytes)";
      }
      default: {
        byte[] data = buffer.bytes(row, field);
        return "(" + data.length + " bytes)";
      }
    }
  }

  @Override
  public Instant instant(int field) {
    return this.buffer.instant(this.row, field);
  }

  @Override
  public int rowId() {
    return row;
  }

}
