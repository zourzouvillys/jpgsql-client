package io.zrz.sqlwriter;

public enum PgTypes implements SqlType {

  TEXT,
  VARCHAR,
  CHAR,
  INT4,
  INT8,
  BOOLEAN,
  BYTEA,
  BIT,
  NUMERIC,
  REAL,

  DATE,
  TIME,

  TIMESTAMP,
  TIMESTAMPTZ,
  INTERVAL,
  INT4RANGE,
  INT8RANGE,
  NUMRANGE,
  TSRANGE,
  TSTZRANGE,
  DATERANGE,

  JSON,
  JSONB,

  INET,
  CIDR,

  UUID,

  TSQUERY,
  TSVECTOR,

  MONEY,

  XML,

  POLYGON,
  POINT,
  PATH,
  LSEG,
  LINE,
  CIRCLE,
  BOX,

  XID,
  TID,
  REGCLASS,
  OID,

  ;

  @Override
  public DbIdent ident() {
    return DbIdent.of(name().toLowerCase());
  }

}
