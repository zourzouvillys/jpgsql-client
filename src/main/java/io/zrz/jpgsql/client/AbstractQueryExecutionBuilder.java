package io.zrz.jpgsql.client;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import io.zrz.sqlwriter.Tuple;

/**
 * helper class for generating queries.
 *
 * @author theo
 *
 */

public abstract class AbstractQueryExecutionBuilder<T> {

  private final List<Tuple> queries = new LinkedList<>();
  protected final PostgresQueryProcessor client;

  protected abstract T result(int i, Tuple tuple);

  protected AbstractQueryExecutionBuilder(final PostgresQueryProcessor client) {
    this.client = client;
  }

  public boolean isEmpty() {
    return queries.isEmpty();
  }

  public void clear() {
    queries.clear();
  }

  public int size() {
    return this.queries.size();
  }

  public T begin() {
    return this.add("BEGIN");
  }

  public T beginReadOnly() {
    return this.add("BEGIN READ ONLY");
  }

  public T commit() {
    return this.add("COMMIT");
  }

  public T rollback() {
    return this.add("ROLLBACK");
  }

  public T setLocal(final String key, final long value) {
    final Tuple tuple = Tuple.of(this.client.createQuery(String.format("SET LOCAL %s = %d", key, value)), null);
    this.queries.add(tuple);
    return this.result(this.queries.size() - 1, tuple);
  }

  public T setLocal(final String key, final double value) {
    final Tuple tuple = Tuple.of(this.client.createQuery(String.format("SET LOCAL %s = %f", key, value)), null);
    this.queries.add(tuple);
    return this.result(this.queries.size() - 1, tuple);
  }

  public T set(final String key, final long value) {
    final Tuple tuple = Tuple.of(this.client.createQuery(String.format("SET %s = %d", key, value)), null);
    this.queries.add(tuple);
    return this.result(this.queries.size() - 1, tuple);
  }

  public T setLocal(final String key, final String value) {
    final Tuple tuple = Tuple.of(this.client.createQuery(String.format("SET LOCAL %s = %s", key, value)), null);
    this.queries.add(tuple);
    return this.result(this.queries.size() - 1, tuple);
  }

  public T set(final String key, final String value) {
    final Tuple tuple = Tuple.of(this.client.createQuery(String.format("SET %s = %s", key, value)), null);
    this.queries.add(tuple);
    return this.result(this.queries.size() - 1, tuple);
  }

  public T add(final String sql, Object... params) {
    return this.add(this.client.createQuery(sql, params.length), params);
  }

  public T add(final String sql, QueryParameters params) {
    Tuple tuple = Tuple.of(client.createQuery(sql, params.count()), params);
    this.queries.add(tuple);
    return this.result(this.queries.size() - 1, tuple);
  }

  public T add(final Query sql, final Object... params) {
    final Query query = this.client.createQuery(sql);
    final Tuple tuple = Tuple.of(query, query.createParameters().setFrom(params).validate());
    this.queries.add(tuple);
    return this.result(this.queries.size() - 1, tuple);
  }

  public T add(Tuple tuple) {
    this.queries.add(tuple);
    return this.result(this.queries.size() - 1, tuple);
  }

  protected Tuple buildQuery() {

    final Query query = this.client.createQuery(this.queries.stream().map(t -> t.query()).collect(Collectors.toList()));

    final QueryParameters params = query.createParameters();

    this.queries.stream()
      .filter(t -> t.params() != null)
      .sequential()
      .reduce(1, (result, element) -> params.append(result, element.params()), (id, x) -> id);

    return Tuple.of(query, params);

  }

  @Override
  public String toString() {
    return this.queries.stream().map(t -> t.query().toString()).collect(Collectors.joining(";\n"));
  }

}
