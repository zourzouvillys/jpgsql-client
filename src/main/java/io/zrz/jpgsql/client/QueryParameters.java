package io.zrz.jpgsql.client;

import java.util.Collection;
import java.util.UUID;

import org.postgresql.core.Oid;
import org.postgresql.util.ByteConverter;

/**
 * Parameters passed to execute a query.
 */

public interface QueryParameters {

  QueryParameters setInteger(int pnum, int val);

  DefaultParametersList setLong(int pnum, long val);

  QueryParameters setString(int pnum, String value, int oid);

  /**
   * set a the parameter to an array of strings
   */

  QueryParameters setStringArray(int pnum, Collection<String> value);

  QueryParameters setIntArray(int pnum, int[] array);

  QueryParameters setNull(int pnum, int oid);

  QueryParameters setBytes(int pnum, byte[] bytes, int oid);

  Object getValue(int pnum);

  int getOid(int pnum);

  int count();

  /**
   * sets by guessing from the passed arguments. the number of parameters must
   * match the number provided, otherwise an {@link IllegalArgumentException} will
   * be thrown.
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

  default QueryParameters setUUID(int i, UUID uuid) {
    final byte[] val = new byte[16];
    ByteConverter.int8(val, 0, uuid.getMostSignificantBits());
    ByteConverter.int8(val, 8, uuid.getLeastSignificantBits());
    return setBytes(i, val, Oid.UUID);
  }

}
