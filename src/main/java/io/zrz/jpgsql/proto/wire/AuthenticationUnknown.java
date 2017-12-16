package io.zrz.jpgsql.proto.wire;

import lombok.Getter;

public class AuthenticationUnknown implements AuthenticationPacket {

  @Getter
  private int authType;

  public AuthenticationUnknown(int authType) {
    this.authType = authType;
  }

  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor) {
    return visitor.visitAuthenticationUnknown(this);
  }

  public String toString() {
    return String.format("Authentication(%s)", AuthType.fromInt(authType));
  }

}
