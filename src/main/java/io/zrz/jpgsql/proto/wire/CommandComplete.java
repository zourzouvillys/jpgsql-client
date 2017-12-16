package io.zrz.jpgsql.proto.wire;

import lombok.Value;

@Value
public class CommandComplete implements PostgreSQLPacket
{
  
  private final String commandTag;

  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor)
  {
    return visitor.visitCommandComplete(this);
  }

}
