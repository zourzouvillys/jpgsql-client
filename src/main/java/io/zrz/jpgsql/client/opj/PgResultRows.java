package io.zrz.jpgsql.client.opj;

import java.util.List;

import org.postgresql.core.Field;

import io.zrz.jpgsql.client.Query;
import io.zrz.jpgsql.client.ResultsRow;
import lombok.Getter;

class PgResultRows implements ResultsRow {

  @Getter
  private final Query query;

  @Getter
  private final Field[] fields;

  @Getter
  private final List<byte[][]> tuples;

  PgResultRows(Query query, Field[] fields, List<byte[][]> tuples) {
    this.query = query;
    this.fields = fields;
    this.tuples = tuples;
  }

}
