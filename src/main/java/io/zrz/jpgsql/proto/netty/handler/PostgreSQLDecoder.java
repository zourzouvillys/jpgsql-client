package io.zrz.jpgsql.proto.netty.handler;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.zrz.jpgsql.proto.netty.MessageType;
import io.zrz.jpgsql.proto.netty.ProtoUtils;
import io.zrz.jpgsql.proto.wire.AuthenticationMD5Password;
import io.zrz.jpgsql.proto.wire.AuthenticationOk;
import io.zrz.jpgsql.proto.wire.AuthenticationUnknown;
import io.zrz.jpgsql.proto.wire.BackendKeyData;
import io.zrz.jpgsql.proto.wire.CommandComplete;
import io.zrz.jpgsql.proto.wire.CopyBothResponse;
import io.zrz.jpgsql.proto.wire.CopyBothResponse.Format;
import io.zrz.jpgsql.proto.wire.CopyData;
import io.zrz.jpgsql.proto.wire.CopyDone;
import io.zrz.jpgsql.proto.wire.DataRow;
import io.zrz.jpgsql.proto.wire.ParameterStatus;
import io.zrz.jpgsql.proto.wire.PostgreSQLPacket;
import io.zrz.jpgsql.proto.wire.ReadyForQuery;
import io.zrz.jpgsql.proto.wire.RowDescription;
import io.zrz.jpgsql.proto.wire.UnknownMessage;

/**
 * decode PostgreSQL messages.
 */

public class PostgreSQLDecoder extends ByteToMessageDecoder {

  @Override
  protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) throws Exception {

    while (in.readableBytes() >= 5) {

      final byte type = in.getByte(in.readerIndex());

      final int len = in.getInt(in.readerIndex() + 1);

      if (in.readableBytes() < (len + 1)) {
        return;
      }

      in.skipBytes(5);

      final MessageType mtype = MessageType.getType(type);

      final ByteBuf payload = in.readSlice(len - 4).retain();

      out.add(parse(mtype, payload));

    }

  }

  /**
   * Parse the specified packet.
   * 
   * @param mtype
   * @param payload
   * @return
   */

  private static final PostgreSQLPacket parse(MessageType mtype, ByteBuf payload) {

    switch (mtype) {

      case AuthRequest: {

        int authType = payload.readInt();

        switch (authType) {
          case 0:
            return new AuthenticationOk();
          case 5: {
            byte[] salt = new byte[4];
            payload.readBytes(salt);
            return new AuthenticationMD5Password(salt);
          }
          default:
            return new AuthenticationUnknown(authType);
        }

      }

      case BackendKeyData: {
        int processId = payload.readInt();
        int secret = payload.readInt();
        return new BackendKeyData(processId, secret);
      }

      case CommandComplete: {
        return new CommandComplete(ProtoUtils.parseString(payload));
      }

      case CopyBothResponse: {
        return parseCopyBothResponse(payload);
      }

      case CopyData: {
        return parseCopyData(payload);
      }

      case CopyDone: {
        return new CopyDone();
      }

      case DataRow: {
        return new DataRow(ProtoUtils.parseDataRow(payload));
      }

      case ErrorResponse: {
        return ProtoUtils.parseError(payload);
      }

      case NoticeResponse: {
        return ProtoUtils.parseNotice(payload);
      }

      case ParameterStatus: {
        String key = ProtoUtils.parseString(payload);
        String value = ProtoUtils.parseString(payload);
        return new ParameterStatus(key, value);
      }

      case ReadyForQuery: {
        return new ReadyForQuery(payload.readByte());
      }

      case RowDescription: {
        return new RowDescription(ProtoUtils.parseRowDescription(payload));
      }

    }
    return new UnknownMessage(mtype);
  }

  private static final CopyData parseCopyData(final ByteBuf buffer) {
    return new CopyData(buffer);
  }

  private static final CopyBothResponse parseCopyBothResponse(ByteBuf cbp) {

    final int type = cbp.readByte();

    switch (type) {

      case 0:
        // rtype = ReplicationHandle.Format.Text;
        break;

      case 1:
        // rtype = ReplicationHandle.Format.Binary;
        break;

      default:
        // it's an unknown type. erp.
        // rtype = ReplicationHandle.Format.Unknown;
        break;

    }

    final int len = cbp.readShort();

    List<Integer> formats = new ArrayList<>(len);

    for (int i = 0; i < len; ++i) {
      formats.add((int) cbp.readShort());
    }

    return new CopyBothResponse((type) == 1 ? Format.Text : Format.Binary, formats);

  }

}
