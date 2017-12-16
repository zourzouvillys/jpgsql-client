package io.zrz.jpgsql.proto.wire;

import java.util.List;

import lombok.Getter;
import lombok.ToString;

@ToString
public class NoticeResponse implements PostgreSQLPacket
{

  @Getter
  private List<String> messages;

  public NoticeResponse(List<String> messages)
  {
    this.messages = messages;
  }

  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor)
  {
    return visitor.visitNoticeResponse(this);
  }

}
