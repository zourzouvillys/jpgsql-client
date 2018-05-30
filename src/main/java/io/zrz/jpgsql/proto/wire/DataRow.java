package io.zrz.jpgsql.proto.wire;

import java.util.List;

import lombok.Value;

@Value
public class DataRow implements PostgreSQLPacket {

  private final List<String> data;

  @Override
  public <T> T apply(final PostgreSQLPacketVisitor<T> visitor) {
    return visitor.visitDataRow(this);
  }

  @Override
  public String toString() {

    return "DataRow(" + data.size() + ")";

  }

}
