package io.zrz.jpgsql.client;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.reactivex.rxjava3.core.Flowable;
import io.zrz.jpgsql.client.opj.PgResultMeta;

public interface RowBuffer extends QueryResult, Publisher<ResultRow> {

  /**
   * the statement number in this query that this result is for. For each statement provided, the
   * statement number increments by one, regardless of it being a SELECT or command.
   */

  @Override
  int statementId();

  /**
   * The number of rows in this batch.
   */

  int count();

  /**
   * if there might be more {@link RowBuffer} instances to follow.
   *
   * there is guarantee that there are more. listen for the end using the CommandStatus message, or
   * the stream being marked as completed.
   *
   */

  boolean maybeMore();

  /**
   * fetch info about a field in this result batch.
   */

  ResultField field(int index);

  /**
   * the query that these results are for.
   */

  Query query();

  /**
   * returns an unboxed integer value.
   *
   * if the requested value is null, it throws a {@link NullPointerException}.
   *
   */

  int intval(int row, int col);

  /**
   * returns an unboxed integer value.
   *
   * if the field is null, returns the defaultValue instead.
   *
   */

  int intval(int row, int col, int defaultValue);

  /**
   * returns an unboxed integer value.
   *
   * if the requested value is null, it throws a {@link NullPointerException}.
   *
   */

  long longval(int row, int col);

  /**
   * returns an unboxed integer value.
   *
   * if the field is null, returns the defaultValue instead.
   *
   */

  long longval(int row, int col, long defaultValue);

  /**
   * the raw byte value for this field.
   *
   * may be null.
   *
   * also, note that a field may be returned in text or binary form, and performign the same query
   * multiple times ay result in different format.
   *
   */

  byte[] bytes(int row, int col);

  /**
   * the number of fields in the row. always the same in a single buffer.
   */

  int fields();

  /**
   * the given field value as a string.
   */

  String strval(int row, int col);

  /**
   * a numeric type with precision, e.g float4. float8, etc.
   */

  BigDecimal decimal(int row, int col);

  default boolean empty() {
    return count() == 0;
  }

  @Override
  default QueryResultKind getKind() {
    return QueryResultKind.RESULTS;
  }

  Instant instant(int row, int col);

  @Override
  default void subscribe(final Subscriber<? super ResultRow> s) {
    Flowable.fromIterable(IntStream.range(0, count())
      .mapToObj(this::row)
      .collect(Collectors.toList()))
      .subscribe(s);
  }

  default void forEach(Consumer<? super ResultRow> fe) {
    IntStream.range(0, count())
      .mapToObj(this::row)
      .forEach(fe);
  }

  ResultRow row(int offset);

  ResultField field(String label);

  boolean boolval(int row, int field);

  byte[] bytea(int row, int i);

  PgResultMeta meta();

  int[] int2vector(int row, int column);

  Collection<String> textArray(int row, int column);

}
