package io.zrz.jpgsql.proto.wire;

import lombok.Value;

@Value
public class ReadyForQuery implements PostgreSQLPacket
{
  
  private final byte status;
  
  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor)
  {
    return visitor.visitReadyForQuery(this);
  }

}
