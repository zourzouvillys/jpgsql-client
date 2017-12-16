package io.zrz.jpgsql.proto.wire;

import io.zrz.jpgsql.proto.netty.MessageType;
import lombok.Value;

@Value
public class UnknownMessage implements PostgreSQLPacket
{
  
  private final MessageType type;

  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor)
  {
    return visitor.visitUnknownMessage(this);
  }

}
