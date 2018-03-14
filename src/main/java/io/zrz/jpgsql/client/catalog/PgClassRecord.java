package io.zrz.jpgsql.client.catalog;

import io.zrz.jpgsql.client.PgResultRow;

public class PgClassRecord {

  private String relname;

  public PgClassRecord(PgResultRow row) {
    this.relname = row.strval("relname");
  }

  public String getSimpleName() {
    return this.relname;
  }

}
