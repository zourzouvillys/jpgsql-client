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

  public PostgreSQLClientNegotiation(final Map<String, String> properties, final String password) {
    this.properties = properties;
    this.password = password;
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final PostgreSQLPacket msg) throws Exception {

    log.debug("msg: {}", msg);

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

      log.info("authentication type MD5 requested {}", msg);

      final Hasher h1 = Hashing.md5().newHasher();

      h1.putString(password, StandardCharsets.UTF_8);
      h1.putString(properties.get("user"), StandardCharsets.UTF_8);

      final Hasher h2 = Hashing.md5().newHasher();

      h2.putBytes(h1.hash().toString().getBytes());
      h2.putBytes(((AuthenticationMD5Password) msg).getSalt());

      final HashCode hash = h2.hash();

      final byte[] pass = new byte[((hash.bits() / 8) * 2) + 3];

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

  private void initialize(final ChannelHandlerContext ctx) {
    ctx.writeAndFlush(new StartupMessage(3, 0, this.properties));
  }

  private void destroy() {
  }

  @Override
  public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
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
  public void handlerRemoved(final ChannelHandlerContext ctx) throws Exception {
    destroy();
    super.handlerRemoved(ctx);
  }

  @Override
  public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
    // Initialize early if channel is active already.
    if (ctx.channel().isActive()) {
      initialize(ctx);
    }
    super.channelRegistered(ctx);
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    initialize(ctx);
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
    destroy();
    super.channelInactive(ctx);
  }

}
