package io.zrz.sqlwriter;

import io.reactivex.rxjava3.core.Flowable;
import io.zrz.jpgsql.client.ResultRow;

public class PostgresQueryBatch {

  public Flowable<ResultRow> withRows(Tuple indexIoStats) {
    return Flowable.empty();
  }

}
