package io.zrz.jpgsql.client;

import java.util.Optional;

public class PgResultRow {

  private final RowBuffer buffer;
  private final int row;

  public PgResultRow(RowBuffer buffer, int row) {
    this.buffer = buffer;
    this.row = row;
  }

  public int statementId() {
    return this.buffer.statementId();
  }

  public ResultField field(int index) {
    return this.buffer.field(index);
  }

  public int fields() {
    return this.buffer.fields();
  }

  public int intval(int field) {
    return this.buffer.intval(this.row, field);
  }

  public int intval(int field, int defaultValue) {
    return this.buffer.intval(this.row, field, defaultValue);
  }

  public String strval(int field) {
    return this.buffer.strval(this.row, field);
  }

  public Optional<byte[]> bytes(int field) {
    return Optional.ofNullable(this.buffer.bytes(this.row, field));
  }

  public long longval(int field) {
    return this.buffer.longval(this.row, field);
  }

}
