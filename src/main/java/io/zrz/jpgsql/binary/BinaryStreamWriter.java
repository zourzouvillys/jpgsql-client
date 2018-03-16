package io.zrz.jpgsql.binary;

/**
 * writer of postgres binary streams
 * 
 * @author theo
 *
 */

public interface BinaryStreamWriter extends BinaryRecordWriter {

  BinaryStreamWriter writeStartRecord(int numfields);

  /**
   * write a raw record out.
   * 
   * @param oid
   * @param data
   * 
   */

  void writeRawField(int oid, byte[] data);

  /**
   * writes a single INT4.
   * 
   * @param oid
   */

  void writeOid(int oid);

}
