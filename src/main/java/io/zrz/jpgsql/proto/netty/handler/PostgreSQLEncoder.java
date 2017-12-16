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
import io.zrz.jpgsql.proto.wire.CommandComplete;
import io.zrz.jpgsql.proto.wire.CopyBothResponse;
import io.zrz.jpgsql.proto.wire.CopyData;
import io.zrz.jpgsql.proto.wire.CopyDone;
import io.zrz.jpgsql.proto.wire.DataRow;
import io.zrz.jpgsql.proto.wire.ErrorResponse;
import io.zrz.jpgsql.proto.wire.Execute;
import io.zrz.jpgsql.proto.wire.NoticeResponse;
import io.zrz.jpgsql.proto.wire.ParameterStatus;
import io.zrz.jpgsql.proto.wire.PasswordMessage;
import io.zrz.jpgsql.proto.wire.PostgreSQLPacket;
import io.zrz.jpgsql.proto.wire.PostgreSQLPacketVisitor;
import io.zrz.jpgsql.proto.wire.Query;
import io.zrz.jpgsql.proto.wire.ReadyForQuery;
import io.zrz.jpgsql.proto.wire.RowDescription;
import io.zrz.jpgsql.proto.wire.SslRequest;
import io.zrz.jpgsql.proto.wire.StartupMessage;
import io.zrz.jpgsql.proto.wire.UnknownMessage;

/**
 * encode PostgreSQL messages.
 */

public class PostgreSQLEncoder extends MessageToByteEncoder<PostgreSQLPacket> {

  @Override
  protected void encode(ChannelHandlerContext ctx, PostgreSQLPacket msg, ByteBuf out) throws Exception {

    msg.apply(new PostgreSQLPacketVisitor<Void>() {

      @Override
      public Void visitAuthenticationOk(AuthenticationOk pkt) {
        return null;
      }

      @Override
      public Void visitAuthenticationUnknown(AuthenticationUnknown pkt) {
        return null;
      }

      @Override
      public Void visitBackendKeyData(BackendKeyData data) {
        return null;
      }

      @Override
      public Void visitCommandComplete(CommandComplete cmd) {
        return null;
      }

      @Override
      public Void visitUnknownMessage(UnknownMessage unknownMessage) {
        return null;
      }

      @Override
      public Void visitCopyBothResponse(CopyBothResponse cmd) {
        return null;
      }

      @Override
      public Void visitCopyData(CopyData copyData) {
        return null;
      }

      @Override
      public Void visitCopyDone(CopyDone copyDone) {
        return null;
      }

      @Override
      public Void visitDataRow(DataRow dataRow) {
        return null;
      }

      @Override
      public Void visitErrorResponse(ErrorResponse errorResponse) {
        return null;
      }

      @Override
      public Void visitNoticeResponse(NoticeResponse noticeResponse) {
        return null;
      }

      @Override
      public Void visitParameterStatus(ParameterStatus parameterStatus) {
        return null;
      }

      @Override
      public Void visitReadyForQuery(ReadyForQuery readyForQuery) {
        return null;
      }

      @Override
      public Void visitRowDescription(RowDescription rowDescription) {
        return null;
      }

      @Override
      public Void visitStartupMessage(StartupMessage msg) {
        int pos = out.writerIndex();
        out.writeInt(0);
        out.writeInt(ProtoUtils.PROTO_VERSION);
        msg.getParameters().forEach((key, value) -> ProtoUtils.addParam(out, key, value));
        out.writeByte(0);
        out.setInt(pos, out.writerIndex() - pos);
        return null;
      }

      @Override
      public Void visitQuery(Query query) {
        out.writeByte('Q');
        int pos = out.writerIndex();
        out.writeInt(0); // update in a bit
        out.writeBytes(query.getQuery().getBytes(StandardCharsets.UTF_8));
        out.writeByte(0);
        out.setInt(pos, out.writerIndex() - pos);
        return null;
      }

      @Override
      public Void visitExecute(Execute execute) {
        out.writeByte('E');
        int pos = out.writerIndex();
        out.writeInt(0); // update in a bit
        out.writeBytes(execute.getCommand().getBytes(StandardCharsets.UTF_8));
        out.writeByte(execute.getMaxRows());
        out.setInt(pos, out.writerIndex() - pos);
        return null;
      }

      @Override
      public Void visitSslRequest(SslRequest sslreq) {
        out.writeInt(8);
        out.writeInt(ProtoUtils.SSL_MAGIC);
        return null;
      }

      @Override
      public Void visitAuthenticationMD5Password(AuthenticationMD5Password authenticationMD5Password) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Void visitPasswordMessage(PasswordMessage passwordMessage) {
        out.writeByte('p');
        byte[] digest = passwordMessage.getPassword();
        out.writeInt(4 + digest.length + 1);
        out.writeBytes(digest);
        out.writeByte(0);
        return null;
      }

    });

  }

}
