package io.zrz.jpgsql.client.opj;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.postgresql.core.Field;

import io.zrz.jpgsql.client.ResultField;
import io.zrz.jpgsql.client.ResultMeta;

public class PgResultMeta implements ResultMeta {

  private final List<PgResultField> fields;
  private Map<String, PgResultField> names;

  public PgResultMeta(Field[] fields) {

    this.fields = IntStream.range(0, fields.length)
        .mapToObj(i -> new PgResultField(i, fields[i]))
        .collect(Collectors.toList());

    this.names = IntStream.range(0, this.fields.size())
        .mapToObj(x -> this.fields.get(x))
        .collect(Collectors.toMap(x -> x.label(), x -> x));

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

  public ResultField field(String label) {
    return this.names.get(label);
  }

}
