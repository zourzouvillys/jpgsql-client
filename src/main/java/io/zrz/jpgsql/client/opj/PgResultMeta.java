package io.zrz.jpgsql.client.opj;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.postgresql.core.Field;

import io.zrz.jpgsql.client.ResultMeta;

public class PgResultMeta implements ResultMeta {

  private final List<PgResultField> fields;

  public PgResultMeta(Field[] fields) {
    this.fields = IntStream.range(0, fields.length)
        .mapToObj(i -> new PgResultField(i, fields[i]))
        .collect(Collectors.toList());
  }

  @Override
  public PgResultField field(int index) {
    return this.fields.get(index);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(this.fields.stream().map(PgResultField::toString).collect(Collectors.joining(", ", "[ ", " ]")));
    return sb.toString();
  }

  public int count() {
    return this.fields.size();
  }

}
