package io.zrz.jpgsql.proto.wire;

import java.util.Map;

import lombok.Value;

@Value
public class StartupMessage implements PostgreSQLPacket
{

  private int majorVersion;
  private int minorVersion;

  private Map<String, String> parameters;

  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor)
  {
    return visitor.visitStartupMessage(this);
  }

}
