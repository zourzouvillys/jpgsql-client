package io.zrz.jpgsql.client;

import org.postgresql.core.Oid;

/**
 * Default implementation of {@link QueryParameters}. Nothing to write home
 * about.
 */

public class DefaultParametersList implements QueryParameters {

  private final Object[] values;
  private final int[] oids;

  public DefaultParametersList(int count) {
    this.values = new Object[count];
    this.oids = new int[count];
  }

  @Override
  public void setInteger(int pnum, int val) {
    this.values[pnum] = val;
    this.oids[pnum] = Oid.INT4;
  }

  @Override
  public void setString(int pnum, String value, int oid) {
    this.values[pnum] = value;
    this.oids[pnum] = oid;
  }

  @Override
  public void setNull(int pnum, int oid) {
    this.values[pnum] = null;
    this.oids[pnum] = oid;
  }

  @Override
  public Object getValue(int pnum) {
    return this.values[pnum];
  }

  @Override
  public int getOid(int pnum) {
    return this.oids[pnum];
  }

  @Override
  public int count() {
    return this.values.length;
  }

}
