package io.zrz.sqlwriter;

import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import lombok.Getter;

public class DbIdent {

  @Getter
  private ImmutableList<String> names;

  public DbIdent(ImmutableList<String> vals) {
    this.names = vals;
  }

  public static DbIdent of(String ident, String... strings) {
    return new DbIdent(ImmutableList.<String>builder().add(ident).add(strings).build());
  }

  public String toString() {
    return names.stream().map(ident -> ident).collect(Collectors.joining("."));
  }

}
