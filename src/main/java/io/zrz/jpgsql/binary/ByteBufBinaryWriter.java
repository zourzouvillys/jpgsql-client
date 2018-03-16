package io.zrz.jpgsql.binary;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.postgresql.core.Oid;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import io.netty.buffer.ByteBuf;
import lombok.SneakyThrows;

public class ByteBufBinaryWriter implements BinaryStreamWriter {

  private ByteBuf buf;

  public ByteBufBinaryWriter(ByteBuf buf) {
    this.buf = buf;
  }

  public ByteBuf buffer() {
    return this.buf;
  }

  public ByteBuf buffer(ByteBuf buffer) {
    ByteBuf old = this.buf;
    this.buf = buffer;
    return old;
  }

  @Override
  public ByteBufBinaryWriter writeStartRecord(int nfields) {
    buf.writeShort(nfields);
    return this;
  }

  public ByteBufBinaryWriter writeBytes(byte[] data) {
    buf.writeInt(data.length);
    buf.writeBytes(data);
    return this;
  }

  public ByteBufBinaryWriter writeBytes(ByteBuf data) {
    buf.writeInt(data.readableBytes());
    buf.writeBytes(data);
    return this;
  }

  public ByteBufBinaryWriter writeBytes(ByteBuffer data) {
    buf.writeInt(data.remaining());
    buf.writeBytes(data);
    return this;
  }

  @Override
  public ByteBufBinaryWriter writeNull() {
    buf.writeInt(-1);
    return this;
  }

  @Override
  public ByteBufBinaryWriter writeText(String data) {

    //
    int pos = buf.writerIndex();
    buf.writeInt(0);

    buf.writeBytes(data.getBytes(StandardCharsets.UTF_8));

    int len = buf.writerIndex() - pos - 4;

    buf.setInt(pos, len);

    return this;

  }

  public ByteBufBinaryWriter writeText(ByteBuffer data) {
    buf.writeInt(buf.readableBytes());
    buf.writeBytes(data);
    return this;
  }

  @Override
  public ByteBufBinaryWriter writeTextSearchVector(Collection<String> data) {

    int pos = buf.writerIndex();

    buf.writeInt(0);

    //
    List<String> words = data
        .stream()
        .filter(x -> x != null)
        .flatMap(word -> Splitter.on(" ").splitToList(word).stream())
        .map(x -> x.trim())
        .filter(x -> x != null && !x.isEmpty())
        .map(x -> x.toLowerCase())
        .sorted()
        .distinct()
        .collect(Collectors.toList());

    buf.writeInt(words.size());

    words.forEach(lexeme -> {

      buf.writeBytes(lexeme.getBytes(StandardCharsets.UTF_8));
      buf.writeByte(0);

      // position
      buf.writeShort(0);

    });

    buf.setInt(pos, buf.writerIndex() - pos - 4);
    return this;

  }

  public ByteBufBinaryWriter writeText(Consumer<ByteBuf> writer) {

    //

    int pos = buf.writerIndex();
    buf.writeInt(0);

    writer.accept(buf);

    int len = buf.writerIndex() - pos - 4;

    buf.setInt(pos, len);
    return this;

  }

  @Override
  public ByteBufBinaryWriter writeInt(int value) {
    Preconditions.checkArgument(value <= 2147483647 && value >= -2147483648, value);
    buf.writeInt(4);
    buf.writeInt(value);
    return this;
  }

  @Override
  public ByteBufBinaryWriter writeBigint(long value) {
    buf.writeInt(8);
    buf.writeLong(value);
    return this;
  }

  @Override
  public ByteBufBinaryWriter writeBoolean(boolean value) {
    buf.writeInt(1);
    buf.writeByte(value ? 1 : 0);
    return this;

  }

  /**
   * write the timestamp, which is provided in *microseconds* since postgres epoch (2000-01-01 00:00:00 UTC).
   */

  @Override
  public ByteBufBinaryWriter writeTimestampPgMicros(long value) {
    // length is 8
    buf.writeInt(8);
    buf.writeLong(value);
    return this;
  }

  @SneakyThrows
  public BinaryRecordWriter writeJsonb(ByteBuffer buf) {
    if (!buf.isDirect()) {
      return writeJsonb(buf.array(), buf.remaining());
    }
    byte[] in = new byte[buf.remaining()];
    buf.get(in);
    return writeJsonb(in, in.length);
  }

  @Override
  public BinaryRecordWriter writeJsonb(byte[] data) {
    return writeJsonb(data, data.length);
  }

  public BinaryRecordWriter writeJsonb(byte[] data, int length) {

    if (length >= 6) {
      for (int i = 0; i < length - 6; ++i) {
        if (data[i] == '\\' && data[i + 1] == 'u' && data[i + 2] == '0' && data[i + 3] == '0' && data[i + 4] == '0' && data[i + 5] == '0') {
          throw new IllegalArgumentException("\\u0000 not allowed in JSONB");
        }
      }
    }

    buf.writeInt(length + 1);
    buf.writeByte(1);
    buf.writeBytes(data);
    return this;
  }

  @Override
  public BinaryRecordWriter writeBytea(byte[] data) {
    buf.writeInt(data.length);
    buf.writeBytes(data);
    return this;
  }

  @Override
  public BinaryRecordWriter writeSmallint(int value) {
    buf.writeInt(2);
    buf.writeShort(value);
    return this;
  }

  @Override
  public BinaryRecordWriter writeInet(InetAddress addr) {

    byte[] bytes = addr.getAddress();

    if (bytes.length == 4) {

      buf.writeInt(bytes.length + 4);
      buf.writeByte(2); // PGSQL_AF_INET
      buf.writeByte(32);
      buf.writeByte(0);
      buf.writeByte(4);
      buf.writeBytes(bytes);

    }
    else if (bytes.length == 16) {

      buf.writeInt(bytes.length + 4);
      // family
      buf.writeByte(3); // PGSQL_AF_INET6
      // bits
      buf.writeByte(128);
      // type, always 0 without CIDR.
      buf.writeByte(0);
      // nb_bytes
      buf.writeByte(16);
      // payload
      buf.writeBytes(bytes);

    }
    else {
      System.err.println("Invalid IP address, len = " + bytes.length + " (" + addr + ")");
      writeNull();
    }

    return this;

  }

  @Override
  public BinaryStreamWriter writeEmptyArray(int oid) {

    buf.writeInt(12);

    // ndim
    buf.writeInt(0);

    // flags
    buf.writeInt(0);

    // OID
    buf.writeInt(oid);

    return this;

  }

  @Override
  public BinaryStreamWriter writeTextArray(List<String> collect) {

    if (collect.isEmpty()) {
      return this.writeEmptyArray(Oid.TEXT);
    }

    int bytes = collect.stream().mapToInt(x -> x.getBytes(StandardCharsets.UTF_8).length).sum();

    buf.writeInt(20 + (bytes + (collect.size() * 4)));

    // ndim
    buf.writeInt(1);

    // flags
    buf.writeInt(0);

    // OID
    buf.writeInt(Oid.TEXT);

    {

      // nelts
      buf.writeInt(collect.size());

      // index to start
      buf.writeInt(1);

    }

    for (String value : collect) {
      byte[] data = value.getBytes(StandardCharsets.UTF_8);
      buf.writeInt(data.length);
      buf.writeBytes(data);
    }

    return this;

  }

  @Override
  public void writeRawField(int oid, byte[] data) {
    buf.writeInt(data.length);
    buf.writeBytes(data);
  }

  @Override
  public void writeOid(int oid) {
    buf.writeInt(oid);
  }

}
