package io.zrz.jpgsql.client.opj;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.postgresql.core.Field;
import org.postgresql.util.PGbytea;
import org.postgresql.core.Tuple;

import com.google.common.base.Splitter;
import com.google.common.primitives.Ints;

import io.zrz.jpgsql.client.PgResultRow;
import io.zrz.jpgsql.client.Query;
import io.zrz.jpgsql.client.ResultField;
import io.zrz.jpgsql.client.ResultRow;
import io.zrz.jpgsql.client.RowBuffer;
import lombok.SneakyThrows;

final class PgResultRows implements RowBuffer {

  private final Query query;
  private final PgResultMeta fields;
  private final List<Tuple> tuples;
  private final boolean done;
  private final int statementId;

  PgResultRows(final Query query, final int statementId, final PgResultMeta fields, final List<Tuple> tuples, final boolean done) {
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
  public PgResultMeta meta() {
    return this.fields;
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
    final byte[] val = this.tuples.get(row).get(col);
    if (val == null) {
      throw new NullPointerException();
    }
    try {
      return Ints.checkedCast(PgResultDecoder.toLong(this.fields.field(col), val));
    }
    catch (Exception ex) {
      System.err.println(this.fields.field(col));
      System.err.println(this.row(row));
      System.err.println(this);
      throw new RuntimeException("converting " + val.length + " bytes to int (" + Arrays.toString(val) + ")", ex);
    }
  }

  @Override
  public int intval(final int row, final int col, final int defaultValue) {
    final byte[] val = this.tuples.get(row).get(col);
    if (val == null) {
      return defaultValue;
    }
    return Ints.checkedCast(PgResultDecoder.toLong(this.fields.field(col), val));
  }

  @Override
  public byte[] bytes(final int row, final int col) {
    return this.tuples.get(row).get(col);
  }

  @Override
  public int fields() {
    return this.fields.count();
  }

  @Override
  public String strval(final int row, final int col) {
    final byte[] val = this.tuples.get(row).get(col);
    if (val == null) {
      return null;
    }
    return PgResultDecoder.toString(this.fields.field(col), val);
  }

  @Override
  public int statementId() {
    return this.statementId;
  }

  @Override
  public long longval(final int row, final int col) {
    final byte[] val = this.tuples.get(row).get(col);
    if (val == null) {
      throw new NullPointerException();
    }
    return PgResultDecoder.toLong(this.fields.field(col), val);
  }

  @Override
  public long longval(final int row, final int col, final long defaultValue) {
    final byte[] val = this.tuples.get(row).get(col);
    if (val == null) {
      return defaultValue;
    }
    return PgResultDecoder.toLong(this.fields.field(col), val);
  }

  @Override
  public BigDecimal decimal(final int row, final int col) {
    final byte[] val = this.tuples.get(row).get(col);
    if (val == null) {
      return null;
    }
    return PgResultDecoder.toBigDecimal(this.fields.field(col), val);
  }

  @Override
  public Instant instant(final int row, final int col) {
    final byte[] val = this.tuples.get(row).get(col);
    if (val == null) {
      return null;
    }
    return PgResultDecoder.toInstant(this.fields.field(col), val);
  }

  @Override
  public boolean boolval(int row, int field) {
    final byte[] val = this.tuples.get(row).get(field);
    if (val == null) {
      throw new NullPointerException();
    }
    return PgResultDecoder.toBoolean(this.fields.field(field), val);
  }

  @Override
  public ResultRow row(int offset) {
    return new PgResultRow(this, offset);
  }

  @SneakyThrows
  @Override
  public byte[] bytea(int row, int i) {

    PgResultField field = this.fields.field(i);

    final int oid = field.oid();

    byte[] raw = tuples.get(row).get(i);

    if (field.format() == Field.TEXT_FORMAT) {
      return PGbytea.toBytes(raw);
    }

    return raw;
  }

  @Override
  public ResultField field(String label) {
    return this.fields.field(label);
  }

  @Override
  public int[] int2vector(int row, int column) {

    PgResultField field = this.fields.field(column);

    switch (field.format()) {
      case Field.TEXT_FORMAT:
        return Splitter.on(' ').splitToList(strval(row, column)).stream().mapToInt(x -> Integer.parseInt(x)).toArray();
    }

    throw new IllegalArgumentException();

  }

  @Override
  public Collection<String> textArray(int row, int column) {

    if (this.row(row).isNull(column)) {
      return null;
    }

    PgResultField field = this.fields.field(column);

    switch (field.format()) {
      case Field.TEXT_FORMAT: {
        String value = strval(row, column);
        value = value.substring(1, value.length() - 1);
        return Splitter.on(",").splitToList(value);
      }

    }

    throw new IllegalArgumentException();

  }

}
