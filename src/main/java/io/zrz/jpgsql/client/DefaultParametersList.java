package io.zrz.jpgsql.client;

import java.util.Arrays;
import java.util.Collection;

import org.eclipse.jdt.annotation.NonNull;
import org.postgresql.core.Oid;

import com.google.common.base.Preconditions;

import io.zrz.jpgsql.client.opj.BinaryParamValue;

/**
 * Default implementation of {@link QueryParameters}. Nothing to write home about.
 */

public class DefaultParametersList implements QueryParameters {

  private final Object[] values;
  private final int[] oids;

  public DefaultParametersList(int count) {
    Preconditions.checkArgument(count >= 0);
    this.values = new Object[count];
    this.oids = new int[count];
  }

  private void checkIndex(int index) {
    if (this.count() == 0) {
      throw new IndexOutOfBoundsException("query has no parameters");
    }
    if (index < 1 || index > this.count()) {
      throw new IndexOutOfBoundsException(String.format("bad element index %d (valid range %d-%d)", index, 1, this.count()));
    }
  }

  @Override
  public DefaultParametersList setInteger(int pnum, int val) {
    this.checkIndex(pnum);
    this.values[pnum - 1] = val;
    this.oids[pnum - 1] = Oid.INT4;
    return this;
  }

  @Override
  public DefaultParametersList setLong(int pnum, long val) {
    this.checkIndex(pnum);
    this.values[pnum - 1] = val;
    this.oids[pnum - 1] = Oid.INT8;
    return this;
  }

  @Override
  public DefaultParametersList setString(int pnum, String value, int oid) {
    this.checkIndex(pnum);
    this.values[pnum - 1] = value;
    this.oids[pnum - 1] = oid;
    return this;
  }

  @Override
  public DefaultParametersList setNull(int pnum, int oid) {
    this.checkIndex(pnum);
    this.values[pnum - 1] = null;
    this.oids[pnum - 1] = oid;
    return this;
  }

  @Override
  public QueryParameters setBytes(int pnum, byte[] bytes, int oid) {
    this.checkIndex(pnum);
    this.values[pnum - 1] = bytes;
    this.oids[pnum - 1] = oid;
    return this;
  }

  @Override
  public Object getValue(int pnum) {
    this.checkIndex(pnum);
    return this.values[pnum - 1];
  }

  @Override
  public QueryParameters setStringArray(int pnum, Collection<String> value, int oid) {
    this.checkIndex(pnum);
    this.values[pnum - 1] = value.toArray(new String[0]);
    this.oids[pnum - 1] = oid;
    return this;
  }

  @Override
  public QueryParameters setStringArray(int pnum, Collection<String> value) {
    this.checkIndex(pnum);
    this.values[pnum - 1] = value.toArray(new String[0]);
    this.oids[pnum - 1] = Oid.TEXT_ARRAY;
    return this;
  }

  @Override
  public QueryParameters setIntArray(int pnum, int[] value) {
    this.checkIndex(pnum);
    this.values[pnum - 1] = value;
    this.oids[pnum - 1] = Oid.INT4_ARRAY;
    return this;
  }

  @Override
  public QueryParameters set(int pnum, BinaryParamValue value) {
    this.checkIndex(pnum);
    this.values[pnum - 1] = value;
    this.oids[pnum - 1] = Oid.UNSPECIFIED;
    return this;
  }

  @Override
  public int getOid(int pnum) {
    this.checkIndex(pnum);
    return this.oids[pnum - 1];
  }

  @Override
  public int count() {
    return this.values.length;
  }

  @Override
  public int append(int offset, QueryParameters source) {
    for (int i = 0; i < source.count(); ++i) {
      this.oids[offset + i - 1] = source.getOid(i + 1);
      this.values[offset + i - 1] = source.getValue(i + 1);
    }
    return offset + source.count();
  }

  @Override
  public QueryParameters validate() {
    return this;
  }

  @Override
  public QueryParameters setFrom(Object... args) {

    if (args.length != this.count()) {
      throw new IllegalArgumentException(String.format("statement expected %d arguments, but %d provided", this.count(), args.length));
    }

    for (int i = 0; i < args.length; ++i) {

      final Object arg = args[i];

      if (arg == null) {

        // throw new IllegalArgumentException("can't handle null parameters");
        this.oids[i] = Oid.UNSPECIFIED;
        this.values[i] = null;

      }
      else if (arg.getClass().equals(byte[].class)) {

        this.oids[i] = Oid.BYTEA;
        this.values[i] = arg;

      }
      else if (arg.getClass().isArray()) {

        if (arg.getClass().getComponentType().equals(String.class)) {
          this.setStringArray(i + 1, Arrays.asList((String[]) arg));
        }
        else {
          throw new IllegalArgumentException("array types not yet supported");
        }

      }
      else if (arg instanceof String) {

        this.setString(i + 1, (String) arg, Oid.VARCHAR);

      }
      else if (arg instanceof Integer) {

        this.setInteger(i + 1, (int) arg);

      }
      else if (arg instanceof Long) {

        this.setLong(i + 1, (long) arg);

      }
      else if (arg instanceof BinaryParamValue) {

        this.set(i + 1, (BinaryParamValue) arg);

      }
      else {

        throw new IllegalArgumentException("don't support mapping of " + arg.getClass());

      }
    }

    return this;

  }

  @Override
  public String toString() {

    final StringBuilder sb = new StringBuilder("{ ");

    for (int i = 0; i < this.count(); ++i) {

      if (i > 0) {
        sb.append(", ");
      }

      sb.append(i + 1).append(" = ");

      Class<? extends @NonNull Object> klass = this.values[i].getClass();

      if (klass.isArray()) {

        if (this.values[i].getClass().equals(byte[].class)) {

          sb.append("(");
          sb.append(((byte[]) this.values[i]).length);
          sb.append(" bytes)");

        }
        else if (klass.getComponentType().equals(String.class)) {

          sb.append(Arrays.toString((Object[]) this.values[i]));

        }

      }
      else {
        sb.append(this.values[i]);
      }

      sb.append(" [").append(Oid.toString(this.oids[i])).append("]");

    }

    sb.append(" }");
    return sb.toString();
  }

  public static QueryParameters emptyParameters() {
    return new DefaultParametersList(0);
  }

}
