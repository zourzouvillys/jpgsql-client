package io.zrz.jpgsql.binary;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import io.zrz.jpgsql.client.PostgresQueryProcessor;
import io.zrz.jpgsql.client.Query;
import io.zrz.jpgsql.client.QueryParameters;
import io.zrz.sqlwriter.Tuple;

public class TupleReducer {

  private List<Tuple> tuples = new LinkedList<>();

  public TupleReducer add(final Tuple val) {

    this.tuples.add(val);

    return this;

  }

  public Tuple build(final PostgresQueryProcessor pg) {

    final Query query = pg.createQuery(this.tuples.stream().sequential().map(t -> t.query()).collect(Collectors.toList()));

    final QueryParameters params = query.createParameters();

    this.tuples.stream()
      .filter(t -> t.params() != null)
      .sequential()
      .reduce(1, (result, element) -> params.append(result, element.params()), (id, x) -> id);

    return Tuple.of(query, params);

  }

}
