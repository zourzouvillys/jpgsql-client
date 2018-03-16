package io.zrz.sqlwriter;

import static io.zrz.sqlwriter.SqlWriters.ident;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.OptionalInt;

import io.zrz.sqlwriter.SqlWriter.SqlGenerator;

public class QueryGenerator implements SqlGenerator {

  private DbIdent table;
  private List<SqlGenerator> columns = new LinkedList<>();
  private List<SqlGenerator> joins = new LinkedList<>();
  private List<SqlGenerator> where = new LinkedList<>();
  private List<SqlGenerator> groupBy = new LinkedList<>();
  private List<SqlGenerator> orderBy = new LinkedList<>();
  private OptionalInt limit = OptionalInt.empty();

  public QueryGenerator(DbIdent table) {
    this.table = table;
  }

  public static QueryGenerator from(DbIdent table) {
    return new QueryGenerator(table);
  }

  public static QueryGenerator fromIdent(String tableName) {
    return from(DbIdent.of(tableName));
  }

  public QueryGenerator selectIdent(String columnName) {
    return select(SqlWriter.ident(columnName));
  }

  public QueryGenerator select(SqlGenerator expr) {
    this.columns.add(expr);
    return this;
  }

  public QueryGenerator select(SqlGenerator expr, String label) {
    this.columns.add(SqlWriters.as(expr, label));
    return this;
  }

  public QueryGenerator select(SqlGenerator... columns) {
    Arrays.stream(columns).forEach(this.columns::add);
    return this;
  }

  public QueryGenerator where(SqlGenerator filter) {
    this.where.add(filter);
    return this;
  }

  public QueryGenerator orderBy(SqlGenerator expr, SqlDirection direction, SqlNulls nulls) {
    this.orderBy.add(SqlWriters.orderByExpr(expr, direction, nulls));
    return this;
  }

  public QueryGenerator orderByIdent(String field) {
    return orderBy(ident(field), null, null);
  }

  public QueryGenerator orderBy(SqlGenerator expr) {
    return orderBy(expr, null, null);
  }

  public QueryGenerator orderByDesc(SqlGenerator expr) {
    return orderBy(expr, SqlDirection.DESC, null);
  }

  public QueryGenerator limit(int count) {
    this.limit = OptionalInt.of(count);
    return this;
  }

  @Override
  public void write(SqlWriter w) {

    w.writeKeyword(SqlKeyword.SELECT);

    if (this.columns.isEmpty())
      w.writeStar();
    else
      w.writeList(SqlWriter.comma(), this.columns);

    w.writeKeyword(SqlKeyword.FROM);
    w.writeIdent(table);

    this.joins.forEach(gen -> w.write(gen));

    if (!this.where.isEmpty()) {
      w.writeKeyword(SqlKeyword.WHERE);
      w.writeList(SqlKeyword.AND, this.where);
    }

    if (!this.groupBy.isEmpty()) {
      w.writeKeyword(SqlKeyword.GROUP, SqlKeyword.BY);
      w.writeList(SqlWriter.comma(), this.groupBy);
    }

    if (!this.orderBy.isEmpty()) {
      w.writeKeyword(SqlKeyword.ORDER, SqlKeyword.BY);
      w.writeList(SqlWriter.comma(), this.orderBy);
    }

    this.limit.ifPresent(count -> {

      w.writeKeyword(SqlKeyword.LIMIT);
      w.writeLiteral(count);

    });

  }

  public QueryGenerator innerJoin(DbIdent of, SqlGenerator on) {
    this.joins.add(w -> {
      w.writeKeyword(SqlKeyword.INNER);
      w.writeKeyword(SqlKeyword.JOIN);
      w.writeIdent(of);
      w.writeKeyword(SqlKeyword.ON);
      w.writeExprList(on);
    });
    return this;
  }

  public QueryGenerator selectCount() {
    return select(SqlWriters.count());
  }

  public SqlGenerator groupBy(SqlGenerator expr) {
    this.groupBy.add(expr);
    return this;
  }
}
