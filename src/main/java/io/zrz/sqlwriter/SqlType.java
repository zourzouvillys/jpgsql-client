package io.zrz.sqlwriter;

import io.zrz.sqlwriter.SqlWriter.SqlGenerator;

public interface SqlType {

  DbIdent ident();

  default SqlGenerator literal(String value) {
    return SqlWriters.literal(value, this);
  }

}
