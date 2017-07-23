package io.zrz.jpgsql.client;

/**
 * Parameters passed to execute a query.
 */

public interface QueryParameters {

  void setInteger(int pnum, int val);

  void setString(int pnum, String value, int oid);

  void setNull(int pnum, int oid);

  Object getValue(int pnum);

  int getOid(int pnum);

  int count();

}
