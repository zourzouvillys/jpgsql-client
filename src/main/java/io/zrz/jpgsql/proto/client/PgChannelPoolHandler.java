package io.zrz.jpgsql.proto.client;

import io.netty.channel.Channel;
import io.netty.channel.pool.AbstractChannelPoolHandler;

public class PgChannelPoolHandler extends AbstractChannelPoolHandler {

  @Override
  public void channelReleased(Channel ch) throws Exception {
    super.channelReleased(ch);
  }

  @Override
  public void channelAcquired(Channel ch) throws Exception {
    super.channelAcquired(ch);
  }

  @Override
  public void channelCreated(Channel ch) throws Exception {
  }

}
