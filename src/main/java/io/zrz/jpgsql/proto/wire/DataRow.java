package io.zrz.jpgsql.proto.wire;

import java.util.List;

import lombok.Value;

@Value
public class DataRow implements PostgreSQLPacket
{
  
  private final List<String> data;
  
  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor)
  {
    return visitor.visitDataRow(this);
  }

}
