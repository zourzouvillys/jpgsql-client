package io.zrz.jpgsql.proto.wire;

import io.netty.buffer.ByteBuf;
import lombok.Value;

@Value
public class CopyData implements PostgreSQLPacket
{

  private ByteBuf data;

  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor)
  {
    return visitor.visitCopyData(this);
  }

}
