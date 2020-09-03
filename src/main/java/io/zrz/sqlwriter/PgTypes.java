package io.zrz.sqlwriter;

public enum PgTypes implements SqlType {

  TEXT,
  VARCHAR,
  INT4,
  INT8,
  BOOLEAN,
  TIMESTAMP,
  TIMESTAMPTZ,
  INTERVAL,
  JSONB,
  TSQUERY,
  BIT,
  DATE,
  TIME.

  ;

  @Override
  public DbIdent ident() {
    return DbIdent.of(name().toLowerCase());
  }

}
