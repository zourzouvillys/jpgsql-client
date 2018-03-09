package io.zrz.jpgsql.client.opj;

import org.postgresql.core.Field;

import io.zrz.jpgsql.client.ResultField;

public class PgResultField implements ResultField {

  private final int col;
  private int length;
  private int format;
  private int tableoid;
  private int position;
  private String label;
  private int modifier;
  private int oid;

  public PgResultField(int col, Field field) {
    this.col = col;
    this.length = field.getLength();
    this.format = field.getFormat();
    this.tableoid = field.getTableOid();
    this.label = field.getColumnLabel();
    this.position = field.getPositionInTable();
    this.modifier = field.getMod();
    this.oid = field.getOID();
  }

  @Override
  public int length() {
    return length;
  }

  @Override
  public String label() {
    return this.label;
  }

  @Override
  public int position() {
    return this.position;
  }

  @Override
  public int format() {
    return format;
  }

  @Override
  public int tableoid() {
    return this.tableoid;
  }

  @Override
  public int modifier() {
    return this.modifier;
  }

  @Override
  public int oid() {
    return this.oid;
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

}
