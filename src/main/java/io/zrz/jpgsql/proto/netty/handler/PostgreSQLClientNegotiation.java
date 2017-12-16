package io.zrz.jpgsql.proto.netty.handler;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.zrz.jpgsql.proto.wire.AuthenticationMD5Password;
import io.zrz.jpgsql.proto.wire.AuthenticationOk;
import io.zrz.jpgsql.proto.wire.AuthenticationPacket;
import io.zrz.jpgsql.proto.wire.BackendKeyData;
import io.zrz.jpgsql.proto.wire.ParameterStatus;
import io.zrz.jpgsql.proto.wire.PasswordMessage;
import io.zrz.jpgsql.proto.wire.PostgreSQLPacket;
import io.zrz.jpgsql.proto.wire.ReadyForQuery;
import io.zrz.jpgsql.proto.wire.StartupMessage;
import lombok.extern.slf4j.Slf4j;

/**
 * Sends the initial payload when the channel is opened.
 */

@Slf4j
public class PostgreSQLClientNegotiation extends SimpleChannelInboundHandler<PostgreSQLPacket> {

  private BackendKeyData backend = null;
  private Map<String, String> params = new HashMap<>();
  private Map<String, String> properties;
  private boolean established = false;
  private String password;

  public PostgreSQLClientNegotiation(Map<String, String> properties, String password) {
    this.properties = properties;
    this.password = password;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, PostgreSQLPacket msg) throws Exception {

    log.debug("msg: ", msg);

    if (established) {
      ctx.fireChannelRead(msg);
      return;
    }

    if (msg instanceof ReadyForQuery) {
      this.established = true;
      ctx.fireUserEventTriggered(new PostgreSQLHandshakeCompleteEvent(params, backend));
      return;
    }
    else if (msg instanceof BackendKeyData) {
      this.backend = (BackendKeyData) msg;
    }
    else if (msg instanceof ParameterStatus) {
      this.params.put(((ParameterStatus) msg).getKey(), ((ParameterStatus) msg).getValue());
    }
    else if (msg instanceof AuthenticationOk) {

      // nothing to do ... signal we are ready.

    }
    else if (msg instanceof AuthenticationMD5Password) {

      log.warn("authentication type requested", msg);

      Hasher h1 = Hashing.md5().newHasher();

      h1.putString(password, StandardCharsets.UTF_8);
      h1.putString(properties.get("user"), StandardCharsets.UTF_8);

      Hasher h2 = Hashing.md5().newHasher();

      h2.putBytes(h1.hash().toString().getBytes());
      h2.putBytes(((AuthenticationMD5Password) msg).getSalt());

      HashCode hash = h2.hash();

      byte[] pass = new byte[((hash.bits() / 8) * 2) + 3];

      pass[0] = 'm';
      pass[1] = 'd';
      pass[2] = '5';

      System.arraycopy(hash.toString().getBytes(), 0, pass, 3, 32);

      ctx.writeAndFlush(new PasswordMessage(pass));

    }
    else if (msg instanceof AuthenticationPacket) {

      // err, crap.
      log.warn("authentication type {} not supported", msg);

    }
    else {
      log.warn("client negotiation got unexpected message {}", msg);
    }

  }

  private void initialize(ChannelHandlerContext ctx) {
    ctx.writeAndFlush(new StartupMessage(3, 0, this.properties));
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
