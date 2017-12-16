package io.zrz.jpgsql.proto;

import lombok.Value;

@Value
public class WalPosition
{

  private long index;
  private long sequence;

  public String toString()
  {
    return String.format("%s/%s", Long.toHexString(index), Long.toHexString(sequence));
  }

  public static WalPosition of(long timeline, long bytes)
  {
    return new WalPosition(timeline, bytes);
  }

  public static WalPosition fromString(String string)
  {
    int idx = string.indexOf('/');
    return of(Long.parseLong(string.substring(0, idx), 16), Long.parseLong(string.substring(idx + 1), 16));
  }

}
