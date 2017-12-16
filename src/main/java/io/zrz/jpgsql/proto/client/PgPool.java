package io.zrz.jpgsql.proto.client;

import java.net.InetSocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;

public class PgPool {

  SimpleChannelPool pool;
  final Bootstrap cb = new Bootstrap();

  public PgPool(EventLoopGroup group) {
    cb.group(group).channel(NioSocketChannel.class);
    this.pool = new SimpleChannelPool(cb.remoteAddress(InetSocketAddress.createUnresolved("127.0.0.1", 5432)), new PgChannelPoolHandler());

  }

}
