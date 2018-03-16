package io.zrz.jpgsql.client.catalog;

import io.zrz.jpgsql.client.PgResultRow;

public class PgClassRecord {

  private String relname;
  private String nspname;

  public PgClassRecord(PgResultRow row) {
    this.nspname = row.strval("nspname");
    this.relname = row.strval("relname");
  }

  public String getNamespaceName() {
    return this.nspname;
  }

  public String getSimpleName() {
    return this.relname;
  }

}
