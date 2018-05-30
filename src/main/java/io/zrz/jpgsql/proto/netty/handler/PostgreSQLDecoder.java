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
import io.zrz.jpgsql.proto.wire.BindComplete;
import io.zrz.jpgsql.proto.wire.CommandComplete;
import io.zrz.jpgsql.proto.wire.CopyBothResponse;
import io.zrz.jpgsql.proto.wire.CopyBothResponse.Format;
import io.zrz.jpgsql.proto.wire.CopyData;
import io.zrz.jpgsql.proto.wire.CopyDone;
import io.zrz.jpgsql.proto.wire.DataRow;
import io.zrz.jpgsql.proto.wire.ParameterStatus;
import io.zrz.jpgsql.proto.wire.ParseComplete;
import io.zrz.jpgsql.proto.wire.PostgreSQLPacket;
import io.zrz.jpgsql.proto.wire.ReadyForQuery;
import io.zrz.jpgsql.proto.wire.RowDescription;
import io.zrz.jpgsql.proto.wire.TransactionStatus;
import io.zrz.jpgsql.proto.wire.UnknownMessage;
import lombok.extern.slf4j.Slf4j;

/**
 * decode PostgreSQL messages.
 */

@Slf4j
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

  private static final PostgreSQLPacket parse(final MessageType mtype, final ByteBuf payload) {

    switch (mtype) {

      case AuthRequest: {

        final int authType = payload.readInt();

        switch (authType) {
          case 0:
            return new AuthenticationOk();
          case 5: {
            final byte[] salt = new byte[4];
            payload.readBytes(salt);
            return new AuthenticationMD5Password(salt);
          }
          default:
            return new AuthenticationUnknown(authType);
        }

      }

      case BackendKeyData: {
        final int processId = payload.readInt();
        final int secret = payload.readInt();
        return new BackendKeyData(processId, secret);
      }

      case BindComplete: {
        return new BindComplete();
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
        final String key = ProtoUtils.parseString(payload);
        final String value = ProtoUtils.parseString(payload);
        return new ParameterStatus(key, value);
      }

      case ParseComplete: {
        return new ParseComplete();
      }

      case ReadyForQuery: {

        final byte status = payload.readByte();

        // Current backend transaction status indicator. Possible values are 'I' if idle (not in a transaction block);
        // 'T' if in a transaction block; or 'E' if in a failed transaction block (queries will be rejected until block
        // is ended).
        switch (status) {
          case 'I':
            return new ReadyForQuery(TransactionStatus.Idle);
          case 'T':
            return new ReadyForQuery(TransactionStatus.Transaction);
          case 'E':
            return new ReadyForQuery(TransactionStatus.Error);
          default:
            throw new IllegalArgumentException("unknown transaction status '" + status + "'");
        }

      }

      case RowDescription: {
        return new RowDescription(ProtoUtils.parseRowDescription(payload));
      }

    }

    log.warn("unknown type: {}", mtype);

    return new UnknownMessage(mtype);
  }

  private static final CopyData parseCopyData(final ByteBuf buffer) {
    return new CopyData(buffer);
  }

  private static final CopyBothResponse parseCopyBothResponse(final ByteBuf cbp) {

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

    final List<Integer> formats = new ArrayList<>(len);

    for (int i = 0; i < len; ++i) {
      formats.add((int) cbp.readShort());
    }

    return new CopyBothResponse((type) == 1 ? Format.Text : Format.Binary, formats);

  }

}
