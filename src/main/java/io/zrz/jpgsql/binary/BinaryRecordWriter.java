package io.zrz.jpgsql.binary;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * writer of postgres binary records.
 * 
 * @author theo
 *
 */

public interface BinaryRecordWriter {

  /**
   * 
   * @param oid
   * @return
   */

  BinaryStreamWriter writeEmptyArray(int oid);

  BinaryStreamWriter writeTextArray(List<String> collect);

  /**
   * 
   */

  BinaryRecordWriter writeNull();

  /**
   * 
   */

  BinaryRecordWriter writeSmallint(int value);

  BinaryRecordWriter writeInt(int value);

  BinaryRecordWriter writeBigint(long value);

  BinaryRecordWriter writeBoolean(boolean value);

  /**
   * 
   */

  BinaryRecordWriter writeJsonb(byte[] data);

  default BinaryRecordWriter writeJsonb(String json) {
    return writeJsonb(json.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * 
   */

  BinaryRecordWriter writeBytea(byte[] data);

  /**
   * 
   */

  BinaryRecordWriter writeText(String data);

  default BinaryRecordWriter writeText(byte[] utf8Bytes) {
    return writeText(new String(utf8Bytes, StandardCharsets.UTF_8));
  }

  /**
   * 
   */

  BinaryRecordWriter writeInet(InetAddress data);

  /**
   * 
   */

  BinaryRecordWriter writeTextSearchVector(Collection<String> data);

  /**
   * write a timestamp using postgres epoch micros
   */

  BinaryRecordWriter writeTimestampPgMicros(long microsSinceMillenium);

  //

  default BinaryRecordWriter writeTimestamp(Date value) {
    return writeTimestampPgMicros(PostgresTimestamp.fromUnixMillis(value.getTime()));
  }

  default BinaryRecordWriter writeTimestamp(LocalDateTime value) {
    return writeTimestampPgMicros(PostgresTimestamp.fromUnixMillis(value.toInstant(ZoneOffset.UTC).toEpochMilli()));
  }

  default BinaryRecordWriter writeTimestamp(Instant value) {
    return writeTimestampPgMicros(PostgresTimestamp.fromUnixMillis(value.toEpochMilli()));
  }

  default BinaryRecordWriter writeTimestampUnixEpochMillis(long epochMillis) {
    return writeTimestampPgMicros(PostgresTimestamp.fromUnixMillis(epochMillis));
  }

}
