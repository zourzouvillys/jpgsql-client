package io.zrz.jpgsql.client;

import java.util.Optional;

public class PgResultRow {

  private final RowBuffer buffer;
  private final int row;

  public PgResultRow(RowBuffer buffer, int row) {
    this.buffer = buffer;
    this.row = row;
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

  public String strval(int field) {
    return this.buffer.strval(this.row, field);
  }

  public Optional<byte[]> bytes(int field) {
    return Optional.ofNullable(this.buffer.bytes(this.row, field));
  }

}
