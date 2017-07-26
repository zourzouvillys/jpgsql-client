package io.zrz.jpgsql.client;

import java.util.LinkedList;
import java.util.List;

/**
 * helper class for generating queries.
 *
 * @author theo
 *
 */

public abstract class AbstractPostgresQueryBuilder<T> {

  private final PostgresQueryProcessor client;
  private final List<Query> queries = new LinkedList<>();

  public AbstractPostgresQueryBuilder(PostgresQueryProcessor client) {
    this.client = client;
  }

  protected abstract T result(int index, Query q);

  public T set(String key, int value) {
    final Query q = this.client.createQuery(String.format("SET %s = %d", key, value));
    this.queries.add(q);
    return this.result(this.queries.size(), this.queries.get(this.queries.size() - 1));
  }

  public T set(String key, String value) {
    this.queries.add(this.client.createQuery(String.format("SET %s = %s", key, value)));
    return this.result(this.queries.size(), this.queries.get(this.queries.size() - 1));
  }

  public T add(String sql) {
    this.queries.add(this.client.createQuery(sql));
    return this.result(this.queries.size(), this.queries.get(this.queries.size() - 1));
  }

  public T add(Query q) {
    this.queries.add(q);
    return this.result(this.queries.size(), this.queries.get(this.queries.size() - 1));
  }

  public T add(String sql, int params) {
    this.queries.add(this.client.createQuery(sql, params));
    return this.result(this.queries.size(), this.queries.get(this.queries.size() - 1));
  }

  public T add(List<String> statements) {
    statements.forEach(sql -> this.queries.add(this.client.createQuery(sql)));
    return this.result(this.queries.size(), this.queries.get(this.queries.size() - 1));
  }

  public Query buildQuery() {
    if (this.queries.size() == 1) {
      return this.queries.get(0);
    }
    return this.client.createQuery(this.queries);
  }

}
