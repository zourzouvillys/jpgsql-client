package io.zrz.jpgsql.proto.wire;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@AllArgsConstructor
@Builder
@Value
public class CopyBothResponse implements PostgreSQLPacket
{

  public static enum Format
  {
    Text, Binary, Unknown
  }

  private final Format format;

  @Singular
  private final List<Integer> columns;

  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor)
  {
    return visitor.visitCopyBothResponse(this);
  }

}
