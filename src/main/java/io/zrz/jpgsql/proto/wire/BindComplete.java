package io.zrz.jpgsql.proto.wire;

import lombok.Value;

//
final @Value public class BindComplete implements PostgreSQLPacket {

  @Override
  public <T> T apply(final PostgreSQLPacketVisitor<T> visitor) {
    return visitor.visitBindComplete(this);
  }

}
