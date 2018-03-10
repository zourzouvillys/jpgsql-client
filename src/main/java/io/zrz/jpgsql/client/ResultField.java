package io.zrz.jpgsql.client;

public interface ResultField {

  int oid();

  int modifier();

  int tableoid();

  int format();

  int position();

  String label();

  int length();

  int column();

}
