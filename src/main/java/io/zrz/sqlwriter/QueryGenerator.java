package io.zrz.sqlwriter;

import static io.zrz.sqlwriter.SqlWriters.ident;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import com.google.common.base.Verify;

import io.zrz.sqlwriter.SqlWriter.SqlGenerator;

public class QueryGenerator implements SqlGenerator {

  private Map<String, SqlGenerator> withs = new HashMap<>();

  private SqlGenerator table;

  private List<SqlGenerator> columns = new LinkedList<>();
  private List<SqlGenerator> joins = new LinkedList<>();
  private List<SqlGenerator> where = new LinkedList<>();
  private List<SqlGenerator> groupBy = new LinkedList<>();
  private List<SqlGenerator> orderBy = new LinkedList<>();

  private OptionalInt limit = OptionalInt.empty();
  private String tableAlias;
  private boolean forUpdate;

  public QueryGenerator(SqlGenerator table) {
    this(table, null);
  }

  public QueryGenerator(SqlGenerator table, String alias) {
    this.table = table;
    this.tableAlias = alias;
  }

  public static QueryGenerator from(SqlGenerator table, String alias) {
    return new QueryGenerator(table, alias);
  }

  public static QueryGenerator from(SqlGenerator table) {
    return new QueryGenerator(table, null);
  }

  public static QueryGenerator fromIdent(String tableName) {
    return from(DbIdent.of(tableName));
  }

  public QueryGenerator with(String alias, SqlGenerator select) {
    Verify.verify(this.withs.put(alias, select) == null, alias);
    return this;
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

    if (!this.withs.isEmpty()) {
      w.write(SqlKeyword.WITH);
      int count = 0;
      for (Map.Entry<String, SqlGenerator> gen : this.withs.entrySet()) {
        if (count++ > 0) {
          w.writeComma();
        }
        w.writeNewline(true);
        w.writeIdent(gen.getKey());
        w.writeKeyword(SqlKeyword.AS);
        w.writeStartExpr();
        w.write(gen.getValue());
        w.writeEndExpr();
      }
      w.writeNewline(false);
    }

    w.writeKeyword(SqlKeyword.SELECT);

    if (this.columns.isEmpty()) {
      w.writeStar();
    }
    else {
      w.writeList(SqlWriter.comma(), this.columns);
    }

    w.writeNewline(false);
    w.writeKeyword(SqlKeyword.FROM);
    w.write(table);

    if (tableAlias != null) {
      w.writeKeyword(SqlKeyword.AS);
      w.writeIdent(this.tableAlias);
    }

    this.joins.forEach(gen -> {
      w.writeNewline(false);
      w.write(gen);
    });

    if (!this.where.isEmpty()) {
      w.writeNewline(false);
      w.writeKeyword(SqlKeyword.WHERE);
      w.writeList(SqlKeyword.AND, this.where);
    }

    if (!this.groupBy.isEmpty()) {
      w.writeNewline(false);
      w.writeKeyword(SqlKeyword.GROUP, SqlKeyword.BY);
      w.writeList(SqlWriter.comma(), this.groupBy);
    }

    if (!this.orderBy.isEmpty()) {
      w.writeNewline(false);
      w.writeKeyword(SqlKeyword.ORDER, SqlKeyword.BY);
      w.writeList(SqlWriter.comma(), this.orderBy);
    }

    this.limit.ifPresent(count -> {

      w.writeNewline(false);
      w.writeKeyword(SqlKeyword.LIMIT);
      w.writeLiteral(count);

    });

    if (this.forUpdate) {
      w.writeNewline(false);
      w.writeKeyword(SqlKeyword.FOR);
      w.writeKeyword(SqlKeyword.UPDATE);
    }

  }

  public QueryGenerator innerJoin(SqlGenerator of, SqlGenerator on) {
    return innerJoin(null, of, on);
  }

  public QueryGenerator innerJoin(String innerAlias, SqlGenerator of, SqlGenerator on) {
    this.joins.add(w -> {
      w.writeKeyword(SqlKeyword.INNER);
      w.writeKeyword(SqlKeyword.JOIN);
      w.write(of);
      if (tableAlias != null) {
        w.writeKeyword(SqlKeyword.AS);
        w.writeIdent(innerAlias);
      }
      w.writeKeyword(SqlKeyword.ON);
      w.writeExprList(on);
    });
    return this;
  }

  public QueryGenerator addFrom(SqlGenerator from) {
    this.joins.add(from);
    return this;
  }

  public QueryGenerator selectCount() {
    return select(SqlWriters.count());
  }

  public QueryGenerator groupBy(SqlGenerator expr) {
    this.groupBy.add(expr);
    return this;
  }

  public QueryGenerator forUpdate() {
    this.forUpdate = true;
    return this;
  }

}
