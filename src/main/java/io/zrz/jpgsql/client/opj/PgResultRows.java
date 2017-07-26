package io.zrz.jpgsql.client.opj;

import java.util.List;

import com.google.common.primitives.Ints;

import io.zrz.jpgsql.client.Query;
import io.zrz.jpgsql.client.ResultField;
import io.zrz.jpgsql.client.RowBuffer;

final class PgResultRows implements RowBuffer {

  private final Query query;
  private final PgResultMeta fields;
  private final List<byte[][]> tuples;
  private final boolean done;
  private final int statementId;

  PgResultRows(Query query, int statementId, PgResultMeta fields, List<byte[][]> tuples, boolean done) {
    this.statementId = statementId;
    this.query = query;
    this.fields = fields;
    this.tuples = tuples;
    this.done = done;
  }

  @Override
  public Query query() {
    return this.query;
  }

  @Override
  public int count() {
    return this.tuples.size();
  }

  @Override
  public boolean maybeMore() {
    return !this.done;
  }

  @Override
  public ResultField field(int index) {
    return this.fields.field(index);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("rowbuffer { count:").append(this.count()).append(" ");
    sb.append("more:").append(this.maybeMore() ? "maybe" : "no").append(" ");
    sb.append("fields: ").append(this.fields);
    sb.append(" }");
    return sb.toString();
  }

  @Override
  public int intval(int row, int col) {
    final byte[] val = this.tuples.get(row)[col];
    if (val == null) {
      throw new NullPointerException();
    }
    return Ints.saturatedCast(PgResultDecoder.toLong(this.fields.field(col).pgfield(), val));
  }

  @Override
  public int intval(int row, int col, int defaultValue) {
    final byte[] val = this.tuples.get(row)[col];
    if (val == null) {
      return defaultValue;
    }
    return Ints.saturatedCast(PgResultDecoder.toLong(this.fields.field(col).pgfield(), val));
  }

  @Override
  public byte[] bytes(int row, int col) {
    return this.tuples.get(row)[col];
  }

  @Override
  public int fields() {
    return this.fields.count();
  }

  @Override
  public String strval(int row, int col) {
    final byte[] val = this.tuples.get(row)[col];
    if (val == null) {
      return null;
    }
    return PgResultDecoder.toString(this.fields.field(col).pgfield(), val);
  }

  @Override
  public int statementId() {
    return this.statementId;
  }

}
