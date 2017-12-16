package io.zrz.jpgsql.proto.wire;

import lombok.Value;

@Value
public class AuthenticationOk implements AuthenticationPacket {

  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor) {
    return visitor.visitAuthenticationOk(this);
  }

}
