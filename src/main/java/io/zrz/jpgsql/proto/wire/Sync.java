// Generated by delombok at Tue Sep 22 10:54:18 PDT 2020
package io.zrz.jpgsql.proto.wire;

public final class Sync implements PostgreSQLPacket {
  @Override
  public <T> T apply(final PostgreSQLPacketVisitor<T> visitor) {
    return visitor.visitSync(this);
  }

  @java.lang.SuppressWarnings("all")
  public Sync() {
  }

  @java.lang.Override
  @java.lang.SuppressWarnings("all")
  public boolean equals(final java.lang.Object o) {
    if (o == this) return true;
    if (!(o instanceof Sync)) return false;
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
    return "Sync()";
  }
}
