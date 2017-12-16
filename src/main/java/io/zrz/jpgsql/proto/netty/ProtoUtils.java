package io.zrz.jpgsql.proto.netty;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;
import io.zrz.jpgsql.proto.wire.ErrorResponse;
import io.zrz.jpgsql.proto.wire.NoticeResponse;
import io.zrz.jpgsql.proto.wire.RowDescription;

public class ProtoUtils {

  public static final int PROTO_VERSION = 196608;
  public static final int SSL_MAGIC = 80877103;

  public static void addParam(final ByteBuf buf, final String key, final String value) {
    buf.writeBytes(key.getBytes(StandardCharsets.UTF_8));
    buf.writeByte(0);
    buf.writeBytes(value.getBytes(StandardCharsets.UTF_8));
    buf.writeByte(0);
  }

  public static String identifier(final String ident) {
    return String.format("\"%s\"", ident);
  }

  public static String value(final String value) {
    return String.format("'%s'", value);
  }

  public static String parseString(ByteBuf payload) {
    final int pos = payload.forEachByte(ByteBufProcessor.FIND_NUL);
    final byte[] posa = new byte[(pos - payload.readerIndex()) + 1];
    payload.readBytes(posa);
    return new String(posa, StandardCharsets.UTF_8);
  }

  /**
   *
   */

  public static ErrorResponse parseError(final ByteBuf payload) {

    final int len = payload.readInt();

    final List<String> messages = new LinkedList<>();

    while (payload.readableBytes() > 0) {
      final int pos = payload.forEachByte(ByteBufProcessor.FIND_NUL);
      final byte[] posa = new byte[(pos - payload.readerIndex()) + 1];
      payload.readBytes(posa);
      messages.add(new String(posa));
    }

    return new ErrorResponse(messages);

  }

  public static List<RowDescription.Entry> parseRowDescription(final ByteBuf payload) {

    final int rows = payload.readShort();

    final ArrayList<RowDescription.Entry> descs = new ArrayList<>(rows);

    for (int i = 0; i < rows; ++i) {

      final int pos = payload.forEachByte(ByteBufProcessor.FIND_NUL);

      final byte[] posa = new byte[(pos - payload.readerIndex())];

      payload.readBytes(posa);

      // the null char.
      payload.readByte();

      final String name = new String(posa, StandardCharsets.UTF_8);

      final int table = payload.readInt();
      final int col = payload.readShort();

      final int type = payload.readInt();
      final int typlen = payload.readShort();
      final int typmod = payload.readInt();
      final int typfmt = payload.readShort();

      descs.add(new RowDescription.Entry(name));

      // System.err.println(String.format("name = %s, table = %d, col = %d, type = %d, len = %d, mod = %d, fmt = %d",
      // name,
      // table,
      // col,
      // type,
      // typlen,
      // typmod,
      // typfmt
      //
      // ));

    }

    return descs;

  }

  public static List<String> parseDataRow(final ByteBuf payload) {

    final int rows = payload.readShort();

    final ArrayList<String> results = new ArrayList<>(rows);

    for (int i = 0; i < rows; ++i) {

      final int len = payload.readInt();

      if (len == -1) {
        results.add(null);
        // field is NULL.
        continue;
      }
      else if (len == 0) {
        results.add(null);
        // empty length
        continue;
      }

      final byte[] data = new byte[len];
      payload.readBytes(data);
      results.add(new String(data));

    }

    return results;

  }

  /**
   * Parse a NOTICE.
   *
   * @param payload
   * @return
   */

  public static NoticeResponse parseNotice(final ByteBuf payload) {

    final int len = payload.readInt();

    final List<String> messages = new LinkedList<>();

    while (payload.readableBytes() > 1) {

      final byte type = payload.readByte();

      final int pos = payload.forEachByte(ByteBufProcessor.FIND_NUL);

      if (pos == -1) {
        final byte[] posa = new byte[payload.readableBytes()];
        payload.readBytes(posa);
        messages.add(new String(posa));
      }
      else {
        final byte[] posa = new byte[(pos - payload.readerIndex()) + 1];
        payload.readBytes(posa);
        messages.add(new String(posa));
      }

    }

    return new NoticeResponse(messages);

  }

}
