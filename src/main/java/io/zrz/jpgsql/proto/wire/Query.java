package io.zrz.jpgsql.proto.wire;

import lombok.Value;

@Value
public class Query implements PostgreSQLPacket
{
  
  private final String query;
  
  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor)
  {
    return visitor.visitQuery(this);
  }

}
