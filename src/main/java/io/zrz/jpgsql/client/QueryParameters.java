package io.zrz.jpgsql.client;

/**
 * Parameters passed to execute a query.
 */

public interface QueryParameters {

  QueryParameters setInteger(int pnum, int val);

  QueryParameters setString(int pnum, String value, int oid);

  QueryParameters setNull(int pnum, int oid);

  Object getValue(int pnum);

  int getOid(int pnum);

  int count();

  /**
   * sets by guessing from the passed arguments. the number of parameters must
   * match the number provided, otherwise an {@link IllegalArgumentException}
   * will be thrown.
   */

  QueryParameters setFrom(Object... args);

  /**
   * append the parameters from another query parameter to this one.
   *
   * @param target_offset
   *          the target offset to start appending parameters at
   *
   * @returns the offset of the last parameter appended
   *
   */

  int append(int target_offset, QueryParameters source);

  /**
   * throws an exception if this query parameters is not valid
   */

  QueryParameters validate();

}
