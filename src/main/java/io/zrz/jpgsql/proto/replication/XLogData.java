package io.zrz.jpgsql.proto.replication;

import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class XLogData
{

  private final long startingPoint;
  private final long currentEnd;
  private final long txtime;
  private final ByteBuf byteBuf;

  public XLogData(final long startingPoint, final long currentEnd, final long txtime, final ByteBuf byteBuf)
  {
    this.startingPoint = startingPoint;
    this.currentEnd = currentEnd;
    this.txtime = txtime;
    this.byteBuf = byteBuf;
  }

  public long getStartingPoint()
  {
    return this.startingPoint;
  }

  public long getCurrentEnd()
  {
    return this.currentEnd;
  }

  public long getTxTime()
  {
    return this.txtime;
  }

  public ByteBuf getByteBuf()
  {
    return this.byteBuf;
  }

}
