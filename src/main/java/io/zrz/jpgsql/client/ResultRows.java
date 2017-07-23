package io.zrz.jpgsql.client;

import java.util.List;

import org.postgresql.core.Field;

import lombok.Getter;

/**
 * A set of result rows from a query.
 *
 * note that there may be more rows coming. the observable will be closed once
 * all the rows have been consumed.
 *
 */

public class ResultRows implements QueryResult {

  @Getter
  private final Query query;

  @Getter
  private final Field[] fields;

  @Getter
  private final List<byte[][]> tuples;

  ResultRows(Query query, Field[] fields, List<byte[][]> tuples) {
    this.query = query;
    this.fields = fields;
    this.tuples = tuples;
  }

}
