package io.zrz.jpgsql.proto.wire;

import lombok.Value;

@Value
public class Sync implements PostgreSQLPacket {

  @Override
  public <T> T apply(final PostgreSQLPacketVisitor<T> visitor) {
    return visitor.visitSync(this);
  }

}
