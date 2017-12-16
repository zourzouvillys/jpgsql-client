package io.zrz.jpgsql.proto.netty.handler;

import java.util.HashMap;

import javax.net.ssl.SSLEngine;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.zrz.jpgsql.proto.client.PgConnection;
import io.zrz.jpgsql.proto.netty.ProtoUtils;
import io.zrz.jpgsql.proto.wire.PostgreSQLPacket;
import lombok.extern.slf4j.Slf4j;

/**
 * Sends the initial payload when the channel is opened.
 */

@Slf4j
public class PostgreSQLClientTlsNegotiation extends SimpleChannelInboundHandler<ByteBuf> {

  private boolean established = false;
  private SSLEngine sslEngine;
  private HashMap<String, String> params;
  private SimpleChannelInboundHandler<PostgreSQLPacket> handler;
  private String password;

  public PostgreSQLClientTlsNegotiation() {
  }

  public PostgreSQLClientTlsNegotiation(SSLEngine sslEngine, HashMap<String, String> params, SimpleChannelInboundHandler<PostgreSQLPacket> handler,
      String password) {
    this.sslEngine = sslEngine;
    this.params = params;
    this.handler = handler;
    this.password = password;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {

    byte b = msg.readByte();

    switch (b) {
      case 'S':
        SslHandler ssl = new SslHandler(sslEngine);
        ctx.pipeline().replace(this, "tls", ssl);

        ssl.handshakeFuture().addListener(new GenericFutureListener<Future<? super Channel>>() {
          @Override
          public void operationComplete(Future<? super Channel> future) throws Exception {
            ctx.pipeline().addLast(new PostgreSQLDecoder());
            ctx.pipeline().addLast(new PostgreSQLEncoder());
            ctx.pipeline().addLast(new LoggingHandler(PgConnection.class, LogLevel.DEBUG));
            ctx.pipeline().addLast(new PostgreSQLClientNegotiation(params, password));
            ctx.pipeline().addLast(handler);
          }
        });

        return;
    }

    // an error occured.

    log.error("SSL rejected, continuing without ...");

    ctx.pipeline().remove(this);
    ctx.pipeline().addLast(new PostgreSQLDecoder());
    ctx.pipeline().addLast(new PostgreSQLEncoder());
    ctx.pipeline().addLast(new LoggingHandler(PgConnection.class, LogLevel.DEBUG));
    ctx.pipeline().addLast(new PostgreSQLClientNegotiation(params, password));
    ctx.pipeline().addLast(handler);

  }

  private void initialize(ChannelHandlerContext ctx) {
    ByteBuf buf = ctx.alloc().buffer(8);
    buf.writeInt(8);
    buf.writeInt(ProtoUtils.SSL_MAGIC);
    ctx.writeAndFlush(buf);
  }

  private void destroy() {
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    if (ctx.channel().isActive() && ctx.channel().isRegistered()) {
      // channelActvie() event has been fired already, which means this.channelActive() will
      // not be invoked. We have to initialize here instead.
      initialize(ctx);
    }
    else {
      // channelActive() event has not been fired yet. this.channelActive() will be invoked
      // and initialization will occur there.
    }
    super.handlerAdded(ctx);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    destroy();
    super.handlerRemoved(ctx);
  }

  @Override
  public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    // Initialize early if channel is active already.
    if (ctx.channel().isActive()) {
      initialize(ctx);
    }
    super.channelRegistered(ctx);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    initialize(ctx);
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    destroy();
    super.channelInactive(ctx);
  }

}
