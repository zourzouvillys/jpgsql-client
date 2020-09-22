package io.zrz.sqlwriter;

import io.reactivex.Flowable;
import io.zrz.jpgsql.client.ResultRow;

public class PostgresQueryBatch {

  public Flowable<ResultRow> withRows(Tuple indexIoStats) {
    return Flowable.empty();
  }

}
