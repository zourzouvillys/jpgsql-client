package io.zrz.jpgsql.binary;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import io.zrz.jpgsql.client.opj.BinaryParamValue;
import lombok.SneakyThrows;

class ArrayBinaryParam implements BinaryParamValue {

  private int oid;
  private int inner;
  private int nelts;
  private byte[] data;

  public ArrayBinaryParam(int oid, int inner, int nelts, byte[] data) {
    this.oid = oid;
    this.inner = inner;
    this.nelts = nelts;
    this.data = data;
  }

  @SneakyThrows
  @Override
  public byte[] toByteArray() {

    ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
    DataOutputStream arrayOutput = new DataOutputStream(byteArrayOutput);

    if (nelts == 0) {

      arrayOutput.writeInt(0); // Dimensions
      arrayOutput.writeInt(1); // options
      arrayOutput.writeInt(inner); // opd

    }
    else {

      arrayOutput.writeInt(1); // Dimensions

      arrayOutput.writeInt(1); // options, 1 == allow null
      arrayOutput.writeInt(inner); // inner type

      //
      arrayOutput.writeInt(nelts); // number of elements
      arrayOutput.writeInt(1); // lower bound. always 1.

      // now the records ...
      arrayOutput.write(this.data);

    }

    byte[] buf = byteArrayOutput.toByteArray();

    return buf;

  }

  @Override
  public int getOid() {
    return oid;
  }

  public String toString() {
    return "array count=" + nelts + ", outer oid=" + oid + " inner=" + inner + ", bytes=" + this.data.length;
  }

}
