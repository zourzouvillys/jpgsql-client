package io.zrz.jpgsql.client;

import java.time.Instant;
import java.util.Collection;
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
  public ResultField field(final String label) {
    return this.buffer.field(label);
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
  public byte[] bytes(final int field, final byte[] defaultValue) {
    final byte[] value = this.buffer.bytes(this.row, field);
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
  public long longval(final int field, final long defaultValue) {
    return this.buffer.longval(this.row, field, defaultValue);
  }

  @Override
  public boolean boolval(final int field) {
    return buffer.boolval(this.row, field);
  }

  @Override
  public byte[] bytea(final int i) {
    return buffer.bytea(this.row, i);
  }

  @Override
  public String toString() {
    return "[" + statementId() + ":" + rowId() + "]: " + IntStream.range(0, this.fields())
        .mapToObj(field -> this.field(field).label() + "=" + this.toString(field))
        .collect(Collectors.joining(", ", "{ ", " }"));
  }

  private String toString(final int field) {

    final byte[] bf = buffer.bytes(row, field);

    if (bf == null || bf.length == 0)
      return "(null)";

    switch (this.field(field).oid()) {
      case Oid.BYTEA: {
        final byte[] data = bytea(field);
        if (data == null)
          return "(null)";
        return "(" + data.length + " bytes)";
      }
      default: {
        final byte[] data = buffer.bytes(row, field);
        return "(" + data.length + " bytes)";
      }
    }
  }

  @Override
  public Instant instant(final int field) {
    return this.buffer.instant(this.row, field);
  }

  @Override
  public int rowId() {
    return row;
  }

  @Override
  public int[] int2vector(final int column) {
    return buffer.int2vector(this.row, column);
  }

  public Collection<String> textArray(final int column) {
    return buffer.textArray(row, column);
  }

}
