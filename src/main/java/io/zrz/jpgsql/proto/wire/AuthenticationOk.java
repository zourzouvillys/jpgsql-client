// Generated by delombok at Tue Sep 22 10:54:18 PDT 2020
package io.zrz.jpgsql.proto.wire;

public final class AuthenticationOk implements AuthenticationPacket {
  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor) {
    return visitor.visitAuthenticationOk(this);
  }

  @java.lang.SuppressWarnings("all")
  public AuthenticationOk() {
  }

  @java.lang.Override
  @java.lang.SuppressWarnings("all")
  public boolean equals(final java.lang.Object o) {
    if (o == this) return true;
    if (!(o instanceof AuthenticationOk)) return false;
    return true;
  }

  @java.lang.Override
  @java.lang.SuppressWarnings("all")
  public int hashCode() {
    final int result = 1;
    return result;
  }

  @java.lang.Override
  @java.lang.SuppressWarnings("all")
  public java.lang.String toString() {
    return "AuthenticationOk()";
  }
}
