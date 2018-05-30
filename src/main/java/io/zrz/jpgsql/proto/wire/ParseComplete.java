package io.zrz.jpgsql.proto.wire;

import lombok.Value;

final @Value public class ParseComplete implements PostgreSQLPacket {

  @Override
  public <T> T apply(final PostgreSQLPacketVisitor<T> visitor) {
    return visitor.visitParseComplete(this);
  }

}
