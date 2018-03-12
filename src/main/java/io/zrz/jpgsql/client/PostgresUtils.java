package io.zrz.jpgsql.client;

import java.util.ArrayList;
import java.util.List;

import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostgresUtils {

  public static Function<QueryResult, Single<CommandStatus>> statusMapper() {
    return (res) -> {
      if (res instanceof CommandStatus) {
        return Single.just((CommandStatus) res);
      }
      return Single.error(new RuntimeException(res.toString()));
    };
  }

  public static Function<QueryResult, Maybe<CommandStatus>> commandStatusMapper() {
    return (res) -> {
      if (res instanceof CommandStatus) {
        return Maybe.just((CommandStatus) res);
      }
      return Maybe.error(new RuntimeException(res.toString()));
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
      else if (res instanceof CommandStatus) {
        return Flowable.empty();
      }
      else if (res instanceof SecureProgress) {
        return Flowable.empty();
      }
      else if (res instanceof WarningResult) {
        log.info("info: {}", res);
        return Flowable.empty();
      }
      else if (res instanceof ErrorResult) {
        return Flowable.error((ErrorResult) res);
      }

      return Flowable.error(new RuntimeException(res.getClass().getSimpleName()));

    };

  }

}
