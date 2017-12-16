package io.zrz.jpgsql.proto.wire;

import java.util.List;

import lombok.Value;

@Value
public class RowDescription implements PostgreSQLPacket
{

  @Value
  public static final class Entry
  {
    private String name;
  }

  private final List<Entry> rows;

  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor)
  {
    return visitor.visitRowDescription(this);
  }

}
