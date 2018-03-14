package io.zrz.sqlwriter;

import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import io.zrz.sqlwriter.SqlWriter.SqlGenerator;
import lombok.Getter;

public class DbIdent implements SqlGenerator {

  @Getter
  private ImmutableList<String> names;

  public DbIdent(ImmutableList<String> vals) {
    this.names = vals;
  }

  public static DbIdent of(String ident, String... strings) {
    return new DbIdent(ImmutableList.<String>builder().add(ident).add(strings).build());
  }

  @Override
  public String toString() {
    return names.stream().map(ident -> SqlWriter.ident(ident).asString()).collect(Collectors.joining("."));
  }

  public String getSimpleName() {
    return this.names.get(this.names.size() - 1);
  }

  @Override
  public void write(SqlWriter w) {
    w.writeIdent(this);
  }

}
