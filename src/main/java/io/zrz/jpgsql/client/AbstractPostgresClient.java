package io.zrz.jpgsql.client;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import lombok.SneakyThrows;

public abstract class AbstractPostgresClient implements PostgresClient {

  private final Cache<String, Query> cache = CacheBuilder.newBuilder()
      .maximumSize(1000)
      .expireAfterWrite(5, TimeUnit.MINUTES)
      .build();

  private final Cache<List<Query>, CombinedQuery> combineCache = CacheBuilder.newBuilder()
      .maximumSize(1000)
      .expireAfterWrite(5, TimeUnit.MINUTES)
      .build();

  @Override
  @SneakyThrows
  public Query createQuery(String sql, int paramcount) {
    return this.cache.get(sql, () -> new SimpleQuery(sql, paramcount));
  }

  @Override
  @SneakyThrows
  public Query createQuery(List<Query> combine) {
    return this.combineCache.get(combine, () -> new CombinedQuery(combine.stream().flatMap(q -> q.getSubqueries().stream()).collect(Collectors.toList())));
  }

}
