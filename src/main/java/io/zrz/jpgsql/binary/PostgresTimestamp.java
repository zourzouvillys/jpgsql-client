package io.zrz.jpgsql.binary;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class PostgresTimestamp {

  public static final long POSTGRES_EPOCH_MILLIS = LocalDateTime.of(2000, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
  public static final long POSTGRES_EPOCH_MICROS = POSTGRES_EPOCH_MILLIS * 1000L;

  public static long fromUnixMillis(long unixEpochMillis) {
    return (unixEpochMillis - POSTGRES_EPOCH_MILLIS) * 1000L;
  }

  public static long fromUnixMicros(long unixEpochMicros) {
    return (unixEpochMicros - POSTGRES_EPOCH_MICROS);
  }

  public static long toUnixMillis(long pgEpochMillis) {
    return (pgEpochMillis + POSTGRES_EPOCH_MILLIS);
  }

  public static long toUnixMicros(long pgEpochMicros) {
    return (pgEpochMicros + POSTGRES_EPOCH_MICROS);
  }

}
