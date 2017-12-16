package io.zrz.jpgsql.proto.wire;

import org.postgresql.core.Utils;

import lombok.Getter;

public class AuthenticationMD5Password implements AuthenticationPacket {

  @Getter
  private byte[] salt;

  public AuthenticationMD5Password(byte[] salt) {
    this.salt = salt;
  }

  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor) {
    return visitor.visitAuthenticationMD5Password(this);
  }

  public String toString() {
    return String.format("AuthenticationMD5Password(%s)", Utils.toHexString(salt));
  }

}
