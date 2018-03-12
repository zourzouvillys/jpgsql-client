package io.zrz.jpgsql.binary;

/**
 * writer of postgres binary streams
 * 
 * @author theo
 *
 */

public interface BinaryStreamWriter extends BinaryRecordWriter {

  BinaryStreamWriter writeStartRecord(int numfields);

  void writeRaw(int oid);

}
