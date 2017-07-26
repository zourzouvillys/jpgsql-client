package io.zrz.jpgsql.client;

public class PostgresQueryBuilder extends AbstractPostgresQueryBuilder<PostgresQueryBuilder> {

  public PostgresQueryBuilder(PostgresQueryProcessor client) {
    super(client);
  }

  @Override
  protected PostgresQueryBuilder result(int index, Query q) {
    return this;
  }

  public static AbstractPostgresQueryBuilder<PostgresQueryBuilder> with(PostgresQueryProcessor ds) {
    return new PostgresQueryBuilder(ds);
  }

  public Query build() {
    return super.buildQuery();
  }

}
