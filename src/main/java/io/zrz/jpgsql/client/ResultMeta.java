package io.zrz.jpgsql.client;

public interface ResultMeta {

  /**
   * fetch information about a specific field.
   *
   * @param index
   *          The field number. starts at zero.
   * 
   * @return
   */

  ResultField field(int index);

}
