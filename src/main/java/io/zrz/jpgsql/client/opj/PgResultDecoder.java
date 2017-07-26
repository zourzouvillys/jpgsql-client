package io.zrz.jpgsql.client.opj;

import java.time.Instant;

import org.postgresql.core.Field;
import org.postgresql.core.Oid;
import org.postgresql.util.ByteConverter;

public class PgResultDecoder {

  public PgResultDecoder() {
  }

  public static long toLong(Field field, byte[] bs) {
    final int oid = field.getOID();

    if (field.getFormat() == Field.TEXT_FORMAT) {
      return Long.parseLong(new String(bs));
    }

    switch (oid) {
      case Oid.INT2:
        return ByteConverter.int2(bs, 0);
      case Oid.INT4:
        return ByteConverter.int4(bs, 0);
      case Oid.INT8:
        return ByteConverter.int8(bs, 0);
    }

    throw new AssertionError(String.format("Can't convert binary field with OID %d to long", oid));
  }

  public static String toString(Field field, byte[] bs) {

    final int oid = field.getOID();

    if (field.getFormat() == Field.BINARY_FORMAT) {

      switch (oid) {
        case Oid.TIMESTAMP:
        case Oid.TIMESTAMPTZ:
          return toInstant(field, bs).toString();
        case Oid.INT2:
        case Oid.INT4:
        case Oid.INT8:
          return Long.toString(toLong(field, bs));
        default:
          throw new AssertionError(String.format("Can't convert binary field with OID %d to long", oid));
      }

    }

    return new String(bs);
  }

  /**
   *
   * @param field
   * @param bs
   * @return
   */

  private static Instant toInstant(Field field, byte[] bytes) {

    final long time = ByteConverter.int8(bytes, 0);

    // long secs = time / 1000000;
    // int nanos = (int) (time - secs * 1000000);

    return Instant.ofEpochMilli(time / 1000);

  }

}
