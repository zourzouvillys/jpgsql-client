package io.zrz.jpgsql.client.opj;

import java.math.BigDecimal;
import java.time.Instant;

import org.postgresql.core.Field;
import org.postgresql.core.Oid;
import org.postgresql.util.ByteConverter;

public class PgResultDecoder {

  public PgResultDecoder() {
  }

  public static long toLong(final Field field, final byte[] bs) {
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

  public static String toString(final Field field, final byte[] bs) {

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
        case Oid.FLOAT4:
          return Double.toString(ByteConverter.float4(bs, 0));
        case Oid.FLOAT8:
          return Double.toString(ByteConverter.float8(bs, 0));
        default:
          throw new AssertionError(String.format("Can't convert binary field with OID %d to string", oid));
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

  public static Instant toInstant(final Field field, final byte[] bytes) {

    final int oid = field.getOID();

    switch (oid) {
      case Oid.TIMESTAMP:
      case Oid.TIMESTAMPTZ: {
        if (field.getFormat() == Field.TEXT_FORMAT) {
          return Instant.parse(new String(bytes));
        }
        final long time = ByteConverter.int8(bytes, 0);
        return Instant.ofEpochMilli(time / 1000);
      }
      case Oid.FLOAT8: {

        final BigDecimal val = (field.getFormat() == Field.TEXT_FORMAT)
            ? new BigDecimal(new String(bytes))
            : BigDecimal.valueOf(ByteConverter.float8(bytes, 0));

        //

        return Instant.ofEpochSecond(
            val.longValue(),
            val.multiply(BigDecimal.valueOf(1000 * 1000 * 1000)).longValue());

      }
      default:
        throw new AssertionError(String.format("Can't convert binary field with OID %d to instant", oid));

    }

  }

  /**
   *
   */

  public static BigDecimal toBigDecimal(final Field field, final byte[] bs) {

    final int oid = field.getOID();

    if (field.getFormat() == Field.TEXT_FORMAT) {
      return new BigDecimal(Double.parseDouble(new String(bs)));
    }

    switch (oid) {
      case Oid.FLOAT4:
        return BigDecimal.valueOf(ByteConverter.float4(bs, 0));
      case Oid.FLOAT8:
        return BigDecimal.valueOf(ByteConverter.float8(bs, 0));
      case Oid.INT2:
        return BigDecimal.valueOf(ByteConverter.int2(bs, 0));
      case Oid.INT4:
        return BigDecimal.valueOf(ByteConverter.int4(bs, 0));
      case Oid.INT8:
        return BigDecimal.valueOf(ByteConverter.int8(bs, 0));
    }

    throw new AssertionError(String.format("Can't convert binary field with OID %d to big decimal", oid));

  }

}
