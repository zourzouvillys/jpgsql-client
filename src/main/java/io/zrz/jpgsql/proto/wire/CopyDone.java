package io.zrz.jpgsql.proto.wire;

import lombok.Value;

@Value
public class CopyDone implements PostgreSQLPacket
{
  
  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor)
  {
    return visitor.visitCopyDone(this);
  }

}
