package io.zrz.jpgsql.client.opj;

import org.postgresql.core.Field;
import org.postgresql.core.Oid;
import org.postgresql.util.ByteConverter;

public class PgResultDecoder {

  public PgResultDecoder() {
  }

  public static long toLong(Field field, byte[] bs) {
    final int oid = field.getOID();
    switch (oid) {
      case Oid.INT2:
        return ByteConverter.int2(bs, 0);
      case Oid.INT4:
        return ByteConverter.int4(bs, 0);
      case Oid.INT8:
        return ByteConverter.int4(bs, 0);
    }
    throw new AssertionError();
  }

  public static String toString(Field field, byte[] bs) {
    if (field.getFormat() == Field.BINARY_FORMAT) {
      return Long.toString(toLong(field, bs));
    }
    return new String(bs);
  }

}
