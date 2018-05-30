package io.zrz.jpgsql.proto.netty.handler;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.zrz.jpgsql.proto.netty.ProtoUtils;
import io.zrz.jpgsql.proto.wire.AuthenticationMD5Password;
import io.zrz.jpgsql.proto.wire.AuthenticationOk;
import io.zrz.jpgsql.proto.wire.AuthenticationUnknown;
import io.zrz.jpgsql.proto.wire.BackendKeyData;
import io.zrz.jpgsql.proto.wire.Bind;
import io.zrz.jpgsql.proto.wire.BindComplete;
import io.zrz.jpgsql.proto.wire.CommandComplete;
import io.zrz.jpgsql.proto.wire.CopyBothResponse;
import io.zrz.jpgsql.proto.wire.CopyData;
import io.zrz.jpgsql.proto.wire.CopyDone;
import io.zrz.jpgsql.proto.wire.DataRow;
import io.zrz.jpgsql.proto.wire.ErrorResponse;
import io.zrz.jpgsql.proto.wire.Execute;
import io.zrz.jpgsql.proto.wire.Flush;
import io.zrz.jpgsql.proto.wire.NoticeResponse;
import io.zrz.jpgsql.proto.wire.ParameterStatus;
import io.zrz.jpgsql.proto.wire.Parse;
import io.zrz.jpgsql.proto.wire.ParseComplete;
import io.zrz.jpgsql.proto.wire.PasswordMessage;
import io.zrz.jpgsql.proto.wire.PostgreSQLPacket;
import io.zrz.jpgsql.proto.wire.PostgreSQLPacketVisitor;
import io.zrz.jpgsql.proto.wire.Query;
import io.zrz.jpgsql.proto.wire.ReadyForQuery;
import io.zrz.jpgsql.proto.wire.RowDescription;
import io.zrz.jpgsql.proto.wire.SslRequest;
import io.zrz.jpgsql.proto.wire.StartupMessage;
import io.zrz.jpgsql.proto.wire.Sync;
import io.zrz.jpgsql.proto.wire.UnknownMessage;

/**
 * encode PostgreSQL messages.
 */

public class PostgreSQLEncoder extends MessageToByteEncoder<PostgreSQLPacket> {

  @Override
  protected void encode(final ChannelHandlerContext ctx, final PostgreSQLPacket msg, final ByteBuf out) throws Exception {

    msg.apply(new PostgreSQLPacketVisitor<Void>() {

      @Override
      public Void visitAuthenticationOk(final AuthenticationOk pkt) {
        return null;
      }

      @Override
      public Void visitAuthenticationUnknown(final AuthenticationUnknown pkt) {
        return null;
      }

      @Override
      public Void visitBackendKeyData(final BackendKeyData data) {
        return null;
      }

      @Override
      public Void visitCommandComplete(final CommandComplete cmd) {
        return null;
      }

      @Override
      public Void visitUnknownMessage(final UnknownMessage unknownMessage) {
        return null;
      }

      @Override
      public Void visitCopyBothResponse(final CopyBothResponse cmd) {
        return null;
      }

      @Override
      public Void visitCopyData(final CopyData copyData) {
        return null;
      }

      @Override
      public Void visitCopyDone(final CopyDone copyDone) {
        return null;
      }

      @Override
      public Void visitDataRow(final DataRow dataRow) {
        return null;
      }

      @Override
      public Void visitErrorResponse(final ErrorResponse errorResponse) {
        return null;
      }

      @Override
      public Void visitNoticeResponse(final NoticeResponse noticeResponse) {
        return null;
      }

      @Override
      public Void visitParameterStatus(final ParameterStatus parameterStatus) {
        return null;
      }

      @Override
      public Void visitReadyForQuery(final ReadyForQuery readyForQuery) {
        return null;
      }

      @Override
      public Void visitRowDescription(final RowDescription rowDescription) {
        return null;
      }

      @Override
      public Void visitStartupMessage(final StartupMessage msg) {
        final int pos = out.writerIndex();
        out.writeInt(0);
        out.writeInt(ProtoUtils.PROTO_VERSION);
        msg.getParameters().forEach((key, value) -> ProtoUtils.addParam(out, key, value));
        out.writeByte(0);
        out.setInt(pos, out.writerIndex() - pos);
        return null;
      }

      @Override
      public Void visitQuery(final Query query) {
        out.writeByte('Q');
        final int pos = out.writerIndex();
        out.writeInt(0); // update in a bit
        out.writeBytes(query.getQuery().getBytes(StandardCharsets.UTF_8));
        out.writeByte(0);
        out.setInt(pos, out.writerIndex() - pos);
        return null;
      }

      @Override
      public Void visitExecute(final Execute execute) {
        out.writeByte('E');
        final int pos = out.writerIndex();
        out.writeInt(0); // update in a bit
        out.writeBytes(execute.getCommand().getBytes(StandardCharsets.UTF_8));
        out.writeByte(0);
        out.writeInt(execute.getMaxRows());
        out.setInt(pos, out.writerIndex() - pos);
        return null;
      }

      @Override
      public Void visitSslRequest(final SslRequest sslreq) {
        out.writeInt(8);
        out.writeInt(ProtoUtils.SSL_MAGIC);
        return null;
      }

      @Override
      public Void visitAuthenticationMD5Password(final AuthenticationMD5Password authenticationMD5Password) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Void visitPasswordMessage(final PasswordMessage passwordMessage) {
        out.writeByte('p');
        final byte[] digest = passwordMessage.getPassword();
        out.writeInt(4 + digest.length + 1);
        out.writeBytes(digest);
        out.writeByte(0);
        return null;
      }

      @Override
      public Void visitParse(final Parse parse) {

        // Byte1('P')
        // Identifies the message as a Parse command.
        out.writeByte('P');

        // Int32
        // Length of message contents in bytes, including self.
        final int pos = out.writerIndex();

        out.writeInt(0);

        // String
        // The name of the destination prepared statement (an empty string selects the unnamed prepared statement).
        out.writeBytes(parse.getName().getBytes(StandardCharsets.UTF_8));
        out.writeByte(0);

        // String
        // The query string to be parsed.
        out.writeBytes(parse.getQuery().getBytes(StandardCharsets.UTF_8));
        out.writeByte(0);

        // Int16
        // The number of parameter data types specified (can be zero). Note that this is not an indication of the number
        // of parameters that might appear in the query string, only the number that the frontend wants to prespecify
        // types for.
        out.writeShort(parse.getParamOids().size());

        // Then, for each parameter, there is the following:
        // Int32
        // Specifies the object ID of the parameter data type. Placing a zero here is equivalent to leaving the type
        // unspecified.
        for (final int oid : parse.getParamOids()) {
          out.writeInt(oid);
        }

        final int len = out.writerIndex() - pos;
        out.setInt(pos, len);

        return null;

      }

      @Override
      public Void visitFlush(final Flush flush) {
        out.writeByte('H');
        out.writeInt(4);
        return null;
      }

      @Override
      public Void visitParseComplete(final ParseComplete parseComplete) {
        return null;
      }

      @Override
      public Void visitBind(final Bind bind) {
        // Byte1('B')
        // Identifies the message as a Bind command.
        out.writeByte('B');

        // Int32
        // Length of message contents in bytes, including self.
        final int pos = out.writerIndex();
        out.writeInt(0);

        //
        // String
        // The name of the destination portal (an empty string selects the unnamed portal).
        //
        out.writeBytes(bind.getDestinationPortal().getBytes(StandardCharsets.UTF_8));
        out.writeByte(0);

        // String
        // The name of the source prepared statement (an empty string selects the unnamed prepared statement).
        out.writeBytes(bind.getSourcePreparedStatement().getBytes(StandardCharsets.UTF_8));
        out.writeByte(0);
        //
        // Int16
        // The number of parameter format codes that follow (denoted C below). This can be zero to indicate that there
        // are no parameters or that the parameters all use the default format (text); or one, in which case the
        // specified format code is applied to all parameters; or it can equal the actual number of parameters.
        out.writeShort(0);
        //
        // Int16[C]
        // The parameter format codes. Each must presently be zero (text) or one (binary).
        //
        // Int16
        // The number of parameter values that follow (possibly zero). This must match the number of parameters needed
        // by the query.
        out.writeShort(0);
        //
        // Next, the following pair of fields appear for each parameter:
        //
        // Int32
        // The length of the parameter value, in bytes (this count does not include itself). Can be zero. As a special
        // case, -1 indicates a NULL parameter value. No value bytes follow in the NULL case.
        //
        // Byten
        // The value of the parameter, in the format indicated by the associated format code. n is the above length.
        //
        // After the last parameter, the following fields appear:
        //
        // Int16
        // The number of result-column format codes that follow (denoted R below). This can be zero to indicate that
        // there are no
        // result columns or that the result columns should all use the default format (text); or one, in which case the
        // specified format code is applied to all result columns (if any); or it can equal the actual number of result
        // columns of the query.
        out.writeShort(1);
        //
        // Int16[R]
        // The result-column format codes. Each must presently be zero (text) or one (binary).
        out.writeShort(1);

        final int len = out.writerIndex() - pos;
        out.setInt(pos, len);

        return null;

      }

      @Override
      public Void visitBindComplete(final BindComplete bindComplete) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Void visitSync(final Sync sync) {
        out.writeByte('S');
        out.writeInt(4);
        return null;
      }

    });

  }

}
