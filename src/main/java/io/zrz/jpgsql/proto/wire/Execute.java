package io.zrz.jpgsql.proto.wire;

import lombok.Value;

@Value
public class Execute implements PostgreSQLPacket
{
  
  private final String command;
  private final int maxRows;
  
  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor)
  {
    return visitor.visitExecute(this);
  }

}
