package io.zrz.jpgsql.client;

import java.util.Optional;

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
  public Optional<byte[]> bytes(final int field) {
    return Optional.ofNullable(this.buffer.bytes(this.row, field));
  }

  @Override
  public long longval(final int field) {
    return this.buffer.longval(this.row, field);
  }

}
