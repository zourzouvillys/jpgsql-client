package io.zrz.jpgsql.proto.wire;

import java.util.List;

import lombok.Singular;
import lombok.ToString;
import lombok.Value;

@ToString
@Value
public class ErrorResponse implements PostgreSQLPacket
{

  @Singular
  private final List<String> messages;

  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor)
  {
    return visitor.visitErrorResponse(this);
  }

}
