package io.zrz.jpgsql.binary;

import java.io.DataOutput;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Splitter;

import lombok.SneakyThrows;

/**
 * converts a {@link UserUpdate} into it's raw table form.
 */

public class BinaryOutputStreamWriter implements BinaryStreamWriter {

  private DataOutput out;

  public BinaryOutputStreamWriter(DataOutput out) {
    this.out = out;
  }

  @SneakyThrows
  @Override
  public BinaryRecordWriter writeNull() {
    out.writeByte(-1);
    return this;
  }

  @SneakyThrows
  @Override
  public BinaryRecordWriter writeInt(int value) {
    out.writeInt(4);
    out.writeInt(value);
    return this;
  }

  @SneakyThrows
  @Override
  public BinaryRecordWriter writeBigint(long value) {
    out.writeInt(8);
    out.writeLong(value);
    return this;
  }

  @SneakyThrows
  @Override
  public BinaryRecordWriter writeBoolean(boolean value) {
    out.writeInt(1);
    out.writeByte(value ? 1 : 0);
    return this;
  }

  @SneakyThrows
  @Override
  public BinaryRecordWriter writeJsonb(byte[] data) {
    out.writeInt(data.length + 1);
    out.writeByte(1);
    out.write(data);
    return this;
  }

  @SneakyThrows
  @Override
  public BinaryRecordWriter writeBytea(byte[] data) {
    // data = PGbytea.toBytes(data);
    out.writeInt(data.length);
    out.write(data);
    return this;
  }

  @SneakyThrows
  @Override
  public BinaryRecordWriter writeText(String data) {
    byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length);
    out.write(bytes);
    return this;
  }

  @SneakyThrows
  @Override
  public BinaryRecordWriter writeTextSearchVector(Collection<String> data) {

    List<byte[]> words = data
        .stream()
        .filter(x -> x != null)
        .flatMap(word -> Splitter.on(" ").splitToList(word).stream())
        .map(x -> x.trim())
        .filter(x -> x != null && !x.isEmpty())
        .map(x -> x.toLowerCase())
        .sorted()
        .distinct()
        .map(word -> word.getBytes(StandardCharsets.UTF_8))
        .collect(Collectors.toList());

    out.writeInt(4 + words.stream().mapToInt(lexeme -> lexeme.length + 3).sum());

    out.writeInt(words.size());

    words.forEach(lexeme -> {

      try {
        out.write(lexeme);
        out.writeByte(0);
        // position
        out.writeShort(0);
      }
      catch (Exception ex) {
        throw new RuntimeException(ex);
      }

    });

    return this;

  }

  @SneakyThrows
  @Override
  public BinaryRecordWriter writeTimestampPgMicros(long value) {
    out.writeInt(8);
    out.writeLong(value);
    return this;
  }

  @SneakyThrows
  @Override
  public BinaryStreamWriter writeStartRecord(int numfields) {
    out.writeShort(numfields);
    return this;
  }

  @SneakyThrows
  @Override
  public BinaryRecordWriter writeSmallint(int value) {
    out.writeInt(2);
    out.writeShort(value);
    return this;
  }

  @SneakyThrows
  @Override
  public BinaryRecordWriter writeInet(InetAddress addr) {
    byte[] bytes = addr.getAddress();
    out.writeInt(bytes.length);
    out.write(bytes);
    return this;
  }

  @Override
  public BinaryStreamWriter writeEmptyArray(int oid) {
    throw new IllegalArgumentException();
  }

  @Override
  public BinaryStreamWriter writeTextArray(List<String> collect) {
    throw new IllegalArgumentException();
  }

  @SneakyThrows
  @Override
  public void writeRawField(int oid, byte[] data) {
    out.writeInt(data.length);
    out.write(data);
  }

  @SneakyThrows
  @Override
  public void writeOid(int oid) {
    out.writeInt(oid);
  }

}
