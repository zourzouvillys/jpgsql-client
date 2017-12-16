package io.zrz.jpgsql.proto.wire;

import lombok.Value;

@Value
public class BackendKeyData implements PostgreSQLPacket
{

  private final int processId;
  private final int secret;

  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor)
  {
    return visitor.visitBackendKeyData(this);
  }

}
