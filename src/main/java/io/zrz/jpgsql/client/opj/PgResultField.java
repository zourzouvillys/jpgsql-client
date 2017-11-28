package io.zrz.jpgsql.client.opj;

import org.postgresql.core.Field;

import io.zrz.jpgsql.client.ResultField;

public class PgResultField implements ResultField {

  private final Field field;
  private final int col;

  public PgResultField(int col, Field field) {
    this.col = col;
    this.field = field;
  }

  @Override
  public int length() {
    return this.field.getLength();
  }

  @Override
  public String label() {
    return this.field.getColumnLabel();
  }

  @Override
  public int position() {
    return this.field.getPositionInTable();
  }

  @Override
  public int format() {
    return this.field.getFormat();
  }

  @Override
  public int tableoid() {
    return this.field.getTableOid();
  }

  @Override
  public int modifier() {
    return this.field.getMod();
  }

  @Override
  public int oid() {
    return this.field.getOID();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(this.label());
    sb.append("[").append(this.col).append(":").append(this.position()).append("]");
    sb.append(" = { ");
    sb.append("oid:").append(this.oid()).append(" ");
    sb.append("tbl:").append(this.tableoid()).append(" ");
    sb.append("len:").append(this.length()).append(" ");
    sb.append("fmt:").append(this.format()).append(" ");
    sb.append("mod:").append(this.modifier()).append(" ");
    sb.append("}");
    return sb.toString();
  }

  public Field pgfield() {
    return this.field;
  }

}
