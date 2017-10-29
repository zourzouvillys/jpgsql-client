package io.zrz.jpgsql.client;

import java.util.ArrayList;
import java.util.List;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

public class PostgresUtils {

  public static Function<QueryResult, Single<CommandStatus>> statusMapper() {
    return (res) -> {
      if (res instanceof CommandStatus) {
        return Single.just((CommandStatus) res);
      }
      return Single.error(new RuntimeException(res.toString()));
    };
  }

  public static Function<QueryResult, Flowable<PgResultRow>> rowMapper() {

    return (res) -> {

      if (res instanceof RowBuffer) {

        final RowBuffer rows = (RowBuffer) res;

        final List<PgResultRow> r = new ArrayList<>(rows.count());

        for (int i = 0; i < rows.count(); ++i) {
          r.add(new PgResultRow(rows, i));
        }

        return Flowable.fromIterable(r);

      }

      return Flowable.error(new RuntimeException(res.toString()));

    };

  }

}
