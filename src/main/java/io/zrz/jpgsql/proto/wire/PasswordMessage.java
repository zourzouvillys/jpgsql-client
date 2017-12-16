package io.zrz.jpgsql.proto.wire;

import lombok.Value;

@Value
public class PasswordMessage implements PostgreSQLPacket {

  private byte[] password;

  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor) {
    return visitor.visitPasswordMessage(this);
  }

}
