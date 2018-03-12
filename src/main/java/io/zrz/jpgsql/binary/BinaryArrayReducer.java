package io.zrz.jpgsql.binary;

import java.util.function.BiConsumer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.reactivex.functions.Function;
import io.zrz.jpgsql.client.DefaultParametersList;
import io.zrz.jpgsql.client.opj.BinaryParamValue;

public class BinaryArrayReducer {

  private ByteBuf buffer;
  private int count;

  public BinaryArrayReducer() {
    this.buffer = Unpooled.buffer();
  }

  public BinaryArrayReducer write(ByteBuf rb) {
    buffer.writeInt(rb.readableBytes());
    buffer.writeBytes(rb);
    rb.release();
    this.count++;
    return this;
  }

  public BinaryParamValue toArray(int arrayType, int recordType) {
    byte[] data = new byte[buffer.readableBytes()];
    buffer.readBytes(data);
    buffer.release();
    return new ArrayBinaryParam(arrayType, recordType, count, data);
  }

  public static Function<? super BinaryArrayReducer, DefaultParametersList> toParamList(int arrayType, int recordType) {
    return in -> {
      DefaultParametersList p = new DefaultParametersList(1);
      p.set(1, in.toArray(arrayType, recordType));
      return p;
    };
  }

  public DefaultParametersList toParameterList(int arrayType, int recordType) {
    DefaultParametersList p = new DefaultParametersList(1);
    p.set(1, this.toArray(arrayType, recordType));
    return p;
  }

  public <T> BinaryArrayReducer apply(T e, BiConsumer<ByteBufBinaryWriter, T> extractor) {
    ByteBuf rb = Unpooled.buffer();
    ByteBufBinaryWriter w = new ByteBufBinaryWriter(rb);
    extractor.accept(w, e);
    buffer.writeInt(rb.readableBytes());
    buffer.writeBytes(rb);
    rb.release();
    this.count++;
    return this;
  }

  public <T> BinaryArrayReducer raw(T e, BiConsumer<ByteBufBinaryWriter, T> extractor) {
    ByteBuf rb = Unpooled.buffer();
    ByteBufBinaryWriter w = new ByteBufBinaryWriter(rb);
    extractor.accept(w, e);
    buffer.writeBytes(rb);
    rb.release();
    this.count++;
    return this;
  }

}
