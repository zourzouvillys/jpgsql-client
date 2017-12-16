package io.zrz.jpgsql.client.opj;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;

import io.zrz.jpgsql.client.PgResultRow;
import io.zrz.jpgsql.client.Query;
import io.zrz.jpgsql.client.ResultField;
import io.zrz.jpgsql.client.ResultRow;
import io.zrz.jpgsql.client.RowBuffer;

final class PgResultRows implements RowBuffer {

  private final Query query;
  private final PgResultMeta fields;
  private final List<byte[][]> tuples;
  private final boolean done;
  private final int statementId;

  PgResultRows(final Query query, final int statementId, final PgResultMeta fields, final List<byte[][]> tuples, final boolean done) {
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
  public ResultField field(final int index) {
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
  public int intval(final int row, final int col) {
    try {
      final byte[] val = this.tuples.get(row)[col];
      if (val == null) {
        throw new NullPointerException();
      }
      return Ints.saturatedCast(PgResultDecoder.toLong(this.fields.field(col).pgfield(), val));
    }
    catch (Exception ex) {
      throw new RuntimeException(String.format("intval(%s): %s", fields.field(col), BaseEncoding.base16().encode(tuples.get(row)[col])), ex);
    }
  }

  @Override
  public int intval(final int row, final int col, final int defaultValue) {
    final byte[] val = this.tuples.get(row)[col];
    if (val == null) {
      return defaultValue;
    }
    return Ints.saturatedCast(PgResultDecoder.toLong(this.fields.field(col).pgfield(), val));
  }

  @Override
  public byte[] bytes(final int row, final int col) {
    return this.tuples.get(row)[col];
  }

  @Override
  public int fields() {
    return this.fields.count();
  }

  @Override
  public String strval(final int row, final int col) {
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

  @Override
  public long longval(final int row, final int col) {
    final byte[] val = this.tuples.get(row)[col];
    if (val == null) {
      throw new NullPointerException();
    }
    return PgResultDecoder.toLong(this.fields.field(col).pgfield(), val);
  }

  @Override
  public long longval(final int row, final int col, final long defaultValue) {
    final byte[] val = this.tuples.get(row)[col];
    if (val == null) {
      return defaultValue;
    }
    return PgResultDecoder.toLong(this.fields.field(col).pgfield(), val);
  }

  @Override
  public BigDecimal decimal(final int row, final int col) {
    final byte[] val = this.tuples.get(row)[col];
    if (val == null) {
      return null;
    }
    return PgResultDecoder.toBigDecimal(this.fields.field(col).pgfield(), val);
  }

  @Override
  public Instant instant(final int row, final int col) {
    final byte[] val = this.tuples.get(row)[col];
    if (val == null) {
      return null;
    }
    return PgResultDecoder.toInstant(this.fields.field(col).pgfield(), val);
  }

  @Override
  public boolean boolval(int row, int field) {
    final byte[] val = this.tuples.get(row)[field];
    if (val == null) {
      throw new NullPointerException();
    }
    return PgResultDecoder.toBoolean(this.fields.field(field).pgfield(), val);
  }

  @Override
  public ResultRow row(int offset) {
    return new PgResultRow(this, offset);
  }

}
