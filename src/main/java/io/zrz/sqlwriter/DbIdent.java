package io.zrz.sqlwriter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import io.zrz.sqlwriter.SqlWriter.SqlGenerator;

public class DbIdent implements SqlGenerator {

  private final ImmutableList<String> names;

  public DbIdent(final ImmutableList<String> vals) {
    this.names = vals;
  }

  public static DbIdent of(final String ident, final String... strings) {
    return new DbIdent(ImmutableList.<String>builder().add(ident).add(strings).build());
  }

  public ImmutableList<String> getNames() {
    return this.names;
  }

  @Override
  public String toString() {
    return names.stream().map(ident -> SqlWriter.ident(ident).asString()).collect(Collectors.joining("."));
  }

  public String getSimpleName() {
    return this.names.get(this.names.size() - 1);
  }

  @Override
  public void write(final SqlWriter w) {
    w.writeIdent(this);
  }

  public String getNamespaceName() {
    return this.names.get(0);
  }

  public DbIdent withReplacedSimpleName(final UnaryOperator<String> operator) {
    final List<String> parts = new ArrayList<String>(this.names);
    parts.set(parts.size() - 1, operator.apply(parts.get(parts.size() - 1)));
    return new DbIdent(ImmutableList.copyOf(parts));
  }

  public DbIdent withColumn(final String ident) {
    final List<String> parts = new ArrayList<String>(this.names);
    parts.add(ident);
    return new DbIdent(ImmutableList.copyOf(parts));
  }

  public DbIdent withoutLast() {
    final List<String> parts = new ArrayList<String>(this.names);
    parts.remove(parts.size() - 1);
    return new DbIdent(ImmutableList.copyOf(parts));
  }

}
