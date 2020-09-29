package io.zrz.jpgsql.client.catalog;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.zrz.jpgsql.client.PostgresQueryProcessor;
import io.zrz.jpgsql.client.PostgresUtils;
import io.zrz.sqlwriter.DbIdent;
import io.zrz.sqlwriter.QueryGenerator;
import io.zrz.sqlwriter.SqlWriters;

public class PgCatalog {

  private PostgresQueryProcessor pg;

  private Flowable<PgClassRecord> catalog = Flowable.defer(() ->

  QueryGenerator.from(DbIdent.of("pg_catalog", "pg_class"))
      .innerJoin(
          DbIdent.of("pg_catalog", "pg_namespace"),
          SqlWriters.eq(DbIdent.of("pg_namespace", "oid"), DbIdent.of("pg_class", "relnamespace")))
      .submitTo(pg))
      .flatMap(PostgresUtils.rowMapper()).map(row -> new PgClassRecord(row)).cache();

  public PgCatalog(PostgresQueryProcessor pg) {
    this.pg = pg;
  }

  public Completable loadCatalog() {
    return catalog.ignoreElements();
  }

  public Single<Boolean> exists(DbIdent klass) {
    return catalog
        .filter(x -> x.getNamespaceName().equals(klass.getNamespaceName()) && x.getSimpleName().equals(klass.getSimpleName()))
        .map(x -> true)
        .single(false);
  }

  public Flowable<PgClassRecord> catalog() {
    return this.catalog;
  }

}
