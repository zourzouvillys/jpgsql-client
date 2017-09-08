package io.zrz.jpgsql.client;

import io.zrz.visitors.annotations.Visitable;

@Visitable.Type
public interface RowBuffer extends QueryResult {

  /**
   * the statement number in this query that this result is for. For each
   * statement provided, the statement number increments by one, regardless of it
   * being a SELECT or command.
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
   * there is guarantee that there are more. listen for the end using the
   * CommandStatus message, or the stream being marked as completed.
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
   * also, note that a field may be returned in text or binary form, and
   * performign the same query multiple times ay result in different format.
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

  default boolean empty() {
    return count() == 0;
  }

}
