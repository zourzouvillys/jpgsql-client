package io.zrz.jpgsql.proto.wire;

import lombok.Value;

@Value
public class ParameterStatus implements PostgreSQLPacket
{
  
  private String key;
  private String value;

  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor)
  {
    return visitor.visitParameterStatus(this);
  }

}
