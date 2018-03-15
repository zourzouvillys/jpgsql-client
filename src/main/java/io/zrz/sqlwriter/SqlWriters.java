package io.zrz.sqlwriter;

import static io.zrz.sqlwriter.SqlKeyword.ALL;
import static io.zrz.sqlwriter.SqlKeyword.ALTER;
import static io.zrz.sqlwriter.SqlKeyword.AND;
import static io.zrz.sqlwriter.SqlKeyword.ATTACH;
import static io.zrz.sqlwriter.SqlKeyword.CREATE;
import static io.zrz.sqlwriter.SqlKeyword.DETACH;
import static io.zrz.sqlwriter.SqlKeyword.DROP;
import static io.zrz.sqlwriter.SqlKeyword.EXISTS;
import static io.zrz.sqlwriter.SqlKeyword.FOR;
import static io.zrz.sqlwriter.SqlKeyword.GRANT;
import static io.zrz.sqlwriter.SqlKeyword.IF;
import static io.zrz.sqlwriter.SqlKeyword.IN;
import static io.zrz.sqlwriter.SqlKeyword.INDEX;
import static io.zrz.sqlwriter.SqlKeyword.LOGGED;
import static io.zrz.sqlwriter.SqlKeyword.NOT;
import static io.zrz.sqlwriter.SqlKeyword.ON;
import static io.zrz.sqlwriter.SqlKeyword.OR;
import static io.zrz.sqlwriter.SqlKeyword.OWNER;
import static io.zrz.sqlwriter.SqlKeyword.PARTITION;
import static io.zrz.sqlwriter.SqlKeyword.RENAME;
import static io.zrz.sqlwriter.SqlKeyword.SCHEMA;
import static io.zrz.sqlwriter.SqlKeyword.SET;
import static io.zrz.sqlwriter.SqlKeyword.TABLE;
import static io.zrz.sqlwriter.SqlKeyword.TABLES;
import static io.zrz.sqlwriter.SqlKeyword.TABLESPACE;
import static io.zrz.sqlwriter.SqlKeyword.TO;
import static io.zrz.sqlwriter.SqlKeyword.TYPE;
import static io.zrz.sqlwriter.SqlKeyword.UNIQUE;
import static io.zrz.sqlwriter.SqlKeyword.UNLOGGED;
import static io.zrz.sqlwriter.SqlKeyword.USING;
import static io.zrz.sqlwriter.SqlKeyword.VALUES;
import static io.zrz.sqlwriter.SqlKeyword.VIEW;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import io.zrz.sqlwriter.SqlWriter.SqlGenerator;

public class SqlWriters {

  public static SqlWriter.SqlGenerator createSchema(String schemaName) {
    return (w) -> {
      w.writeKeyword(CREATE, SCHEMA);
      w.writeIdent(schemaName);
    };
  }

  public static SqlWriter.SqlGenerator createSchemaIfNotExists(String schemaName) {
    return (w) -> {
      w.writeKeyword(CREATE, SCHEMA, IF, NOT, EXISTS);
      w.writeIdent(schemaName);
    };
  }

  public static SqlWriter.SqlGenerator columnIdent(DbIdent ident) {
    return (w) -> {

      List<String> idents = Lists.newArrayList(ident.getNames());

      String columnName = idents.remove(idents.size() - 1);

      w.writeKeyword(SqlKeyword.REFERENCES);

      w.writeIdent(idents);
      w.writeStartExpr();
      w.writeIdent(columnName);
      w.writeEndExpr();

    };
  }

  public static SqlGenerator deleteFrom(DbIdent table, SqlGenerator where) {
    return w -> {
      w.writeKeyword(SqlKeyword.DELETE);
      w.writeKeyword(SqlKeyword.FROM);
      w.writeIdent(table);
      w.writeKeyword(SqlKeyword.WHERE);
      w.write(where);
    };
  }

  public static SqlGenerator insertInto(DbIdent target, SqlGenerator select) {
    return w -> {
      w.writeKeyword(SqlKeyword.INSERT);
      w.writeKeyword(SqlKeyword.INTO);
      w.writeIdent(target);
      w.write(select);
    };
  }

  public static SqlWriter.SqlGenerator copyBinaryFromStdin(DbIdent tableName, String... columns) {

    return w -> {

      w.writeKeyword(SqlKeyword.COPY);
      w.writeIdent(tableName);

      if (columns.length > 0) {
        w.writeExprList(SqlWriters.idents(columns));
      }

      w.writeKeyword(SqlKeyword.FROM);
      w.writeKeyword(SqlKeyword.STDIN);
      w.writeKeyword(SqlKeyword.WITH);
      w.writeKeyword(SqlKeyword.BINARY);

    };

  }

  public static SqlWriter.SqlGenerator createTable(String schemaName, String tableName) {
    return (w) -> {
      w.writeKeyword(CREATE, TABLE, IF, NOT, EXISTS);
      w.writeIdent(schemaName, tableName);
      w.writeStartExpr();
      w.writeEndExpr();
    };
  }

  public static SqlWriter.SqlGenerator indexItem(String columnName, String opclass) {
    return (w) -> {
      w.writeIdent(columnName);
      if (opclass != null)
        w.writeIdent(opclass);
    };

  }

  public static SqlWriter.SqlGenerator indexItem(String columnName, String opclass, SqlDirection direction, SqlNulls nulls) {
    return (w) -> {
      w.writeIdent(columnName);
      if (opclass != null)
        w.writeIdent(opclass);
      if (direction != null) {
        w.writeKeyword(direction == SqlDirection.ASC ? SqlKeyword.ASC : SqlKeyword.DESC);
      }
      if (nulls != null) {
        w.writeKeyword(SqlKeyword.NULLS);
        w.writeKeyword(nulls == SqlNulls.FIRST ? SqlKeyword.FIRST : SqlKeyword.LAST);
      }
    };
  }

  public static SqlWriter.SqlGenerator indexItem(SqlGenerator columnName, String opclass, SqlDirection direction, SqlNulls nulls) {
    return (w) -> {
      w.write(columnName);
      if (opclass != null)
        w.writeIdent(opclass);
      if (direction != null) {
        w.writeKeyword(direction == SqlDirection.ASC ? SqlKeyword.ASC : SqlKeyword.DESC);
      }
      if (nulls != null) {
        w.writeKeyword(SqlKeyword.NULLS);
        w.writeKeyword(nulls == SqlNulls.FIRST ? SqlKeyword.FIRST : SqlKeyword.LAST);
      }
    };
  }

  public static SqlWriter.SqlGenerator indexItem(String columnName, SqlDirection direction, SqlNulls nulls) {
    return (w) -> {
      w.writeIdent(columnName);
      if (direction != null) {
        w.writeKeyword(direction == SqlDirection.ASC ? SqlKeyword.ASC : SqlKeyword.DESC);
      }
      if (nulls != null) {
        w.writeKeyword(SqlKeyword.NULLS);
        w.writeKeyword(nulls == SqlNulls.FIRST ? SqlKeyword.FIRST : SqlKeyword.LAST);
      }
    };
  }

  // { column_name | ( expression ) } [ COLLATE collation ] [ opclass ] [ ASC | DESC ] [ NULLS { FIRST | LAST } ]

  public static SqlWriter.SqlGenerator createUniqueIndex(String idxname, String type, DbIdent tblname, SqlGenerator... indexItems) {
    return (w) -> {

      w.writeKeyword(CREATE);
      w.writeKeyword(UNIQUE);
      w.writeKeyword(INDEX);
      w.writeKeyword(IF, NOT, EXISTS);
      w.writeIdent(idxname);
      w.writeKeyword(ON);
      w.writeIdent(tblname);

      w.writeKeyword(USING);
      w.writeIdent(type);

      w.writeExprList(indexItems);

      // w.writeKeyword(WHERE);
      // w.writeLiteral(true);

    };
  }

  public static SqlWriter.SqlGenerator createIndex(String idxname, String type, DbIdent tblname, SqlGenerator... indexItems) {
    return (w) -> {

      w.writeKeyword(CREATE);
      w.writeKeyword(INDEX);
      w.writeKeyword(IF, NOT, EXISTS);
      w.writeIdent(idxname);
      w.writeKeyword(ON);
      w.writeIdent(tblname);

      w.writeKeyword(USING);
      w.writeIdent(type);

      w.writeExprList(indexItems);

    };

  }

  public static SqlWriter.SqlGenerator createBtreeIndex(String id, DbIdent tblname, SqlGenerator... indexItems) {
    return (w) -> {
      w.writeKeyword(CREATE);
      w.writeKeyword(INDEX);
      w.writeKeyword(IF, NOT, EXISTS);
      w.writeIdent(id);
      w.writeKeyword(ON);
      w.writeIdent(tblname);
      w.writeExprList(indexItems);
    };

  }

  public static SqlGenerator now() {
    return w -> {
      w.writeKeyword(SqlKeyword.NOW);
      w.writeStartExpr();
      w.writeEndExpr();
    };
  }

  public static SqlGenerator attachTable(DbIdent master, DbIdent partition, SqlGenerator... values) {
    return attachTable(master, partition, ImmutableList.copyOf(values));
  }

  public static SqlGenerator attachTable(DbIdent master, DbIdent partition, Collection<SqlGenerator> values) {

    return w -> {

      // ALTER TABLE users ATTACH PARTITION "xxx".yyy FOR VALUES IN ( 1, 2 );

      w.writeKeyword(ALTER, TABLE);
      w.writeIdent(master);

      w.writeKeyword(ATTACH, PARTITION);
      w.writeIdent(partition);

      w.writeKeyword(FOR, VALUES, IN);

      w.writeStartExpr();
      w.writeList(SqlWriter.comma(), values);
      w.writeEndExpr();

    };

  }

  public static SqlGenerator detachTable(DbIdent master, DbIdent partition) {

    return w -> {

      // ALTER TABLE users DETACH PARTITION "xxx".yyy

      w.writeKeyword(ALTER, TABLE);
      w.writeIdent(master);

      w.writeKeyword(DETACH, PARTITION);
      w.writeIdent(partition);

    };

  }

  public static SqlGenerator alterTablespace(DbIdent master, String tablespace) {

    return w -> {

      // ALTER TABLE users SET TABLESPACE 'xxx';

      w.writeKeyword(ALTER, TABLE);
      w.writeIdent(master);

      w.writeKeyword(SET, TABLESPACE);
      w.writeQuotedString(tablespace);

    };

  }

  public static SqlGenerator setLogged(DbIdent master, boolean logged) {

    return w -> {

      // ALTER TABLE users SET LOGGED | UNLOGGED;

      w.writeKeyword(ALTER, TABLE);
      w.writeIdent(master);
      w.writeKeyword(SET);

      w.writeKeyword(logged ? LOGGED : UNLOGGED);

    };

  }

  public static SqlGenerator literal(int i) {
    return w -> w.writeLiteral(i);
  }

  public static SqlGenerator literal(String value) {
    Objects.requireNonNull(value);
    return w -> w.writeQuotedString(value);
  }

  public static SqlGenerator literal(boolean value) {
    return w -> w.writeLiteral(value);
  }

  public static SqlGenerator literal(LocalDateTime value) {
    return w -> {

      w.writeQuotedString(value.toString());
      w.writeOperator("::timestamp");

    };
  }

  public static SqlGenerator[] idents(String... columns) {

    SqlGenerator[] res = new SqlGenerator[columns.length];

    for (int i = 0; i < columns.length; ++i) {
      String value = columns[i];
      res[i] = w -> w.writeIdent(value);
    }

    return res;

  }

  public static SqlGenerator select(DbIdent table, SqlGenerator filter, String... columns) {
    return select(table, filter, Arrays.stream(columns).map(SqlWriters::ident).toArray(SqlGenerator[]::new));
  }

  public static SqlGenerator select(DbIdent table, SqlGenerator filter, SqlGenerator... columns) {
    return w -> {
      w.writeKeyword(SqlKeyword.SELECT);
      w.writeList(SqlWriter.comma(), Arrays.asList(columns));
      w.writeNewline();
      w.writeKeyword(SqlKeyword.FROM);
      w.writeIdent(table);
      w.writeNewline();
      w.writeKeyword(SqlKeyword.WHERE);
      w.write(filter);
    };
  }

  public static SqlGenerator select(DbIdent table, Optional<SqlGenerator> filter, SqlGenerator... columns) {
    return w -> {
      w.writeKeyword(SqlKeyword.SELECT);
      w.writeList(SqlWriter.comma(), Arrays.asList(columns));
      w.writeNewline();
      w.writeKeyword(SqlKeyword.FROM);
      w.writeIdent(table);
      filter.ifPresent(f -> {
        w.writeNewline();
        w.writeKeyword(SqlKeyword.WHERE);
        w.write(f);
      });
    };
  }

  public static SqlGenerator selectGroupBy(DbIdent table, SqlGenerator groupBy, SqlGenerator... columns) {
    return w -> {
      w.writeKeyword(SqlKeyword.SELECT);
      w.writeList(SqlWriter.comma(), Arrays.asList(columns));
      w.writeNewline();
      w.writeKeyword(SqlKeyword.FROM);
      w.writeIdent(table);
      w.writeNewline();
      w.writeKeyword(SqlKeyword.GROUP);
      w.writeKeyword(SqlKeyword.BY);
      w.write(groupBy);
    };
  }

  public static SqlGenerator alterSchemaOwner(String schemaName, String owner) {
    // ALTER SCHEMA xxx OWNER TO "yyy";
    return w -> {
      w.writeKeyword(ALTER, SCHEMA);
      w.writeIdent(schemaName);
      w.writeKeyword(OWNER, TO);
      w.writeIdent(owner);
    };
  }

  public static SqlGenerator grantUsageOnSchema(String schemaName, String whom) {
    // GRANT SELECT ON ALL TABLES IN SCHEMA ccc TO ddd;
    return w -> {
      w.writeKeyword(GRANT);
      w.writeKeyword(SqlKeyword.USAGE);
      w.writeKeyword(ON);
      w.writeKeyword(SCHEMA);
      w.writeIdent(schemaName);
      w.writeKeyword(TO);
      w.writeIdent(whom);
    };

  }

  public static SqlGenerator grantTable(SqlKeyword what, DbIdent table, String whom) {
    return w -> {
      w.writeKeyword(GRANT);
      w.writeKeyword(what);
      w.writeKeyword(ON);
      w.writeKeyword(TABLE);
      w.writeIdent(table);
      w.writeKeyword(TO);
      w.writeIdent(whom);
    };
  }

  public static SqlGenerator grantOnAllTables(String schemaName, String whom, SqlKeyword... what) {
    // GRANT SELECT ON ALL TABLES IN SCHEMA yyy TO xxx;
    return w -> {
      w.writeKeyword(GRANT);
      w.writeKeyword(what);
      w.writeKeyword(ON, ALL, TABLES, IN, SCHEMA);
      w.writeIdent(schemaName);
      w.writeKeyword(TO);
      w.writeIdent(whom);
    };

  }

  public static SqlGenerator identStartsWith(String ident, String value) {
    return w -> {
      w.writeIdent(ident);
      w.writeKeyword(SqlKeyword.LIKE);
      w.writeQuotedString(value + "%");
    };
  }

  public static SqlGenerator identLike(String ident, String value) {
    return w -> {
      w.writeIdent(ident);
      w.writeOperator("=");
      w.writeQuotedString(value);
    };
  }

  public static SqlGenerator eq(SqlGenerator ident, String value) {
    return w -> {
      w.write(ident);
      w.writeOperator("=");
      w.writeQuotedString(value);
    };
  }

  public static SqlGenerator eq(SqlGenerator ident, SqlGenerator value) {
    return w -> {
      w.write(ident);
      w.writeOperator("=");
      w.write(value);
    };
  }

  public static SqlGenerator eq(String ident, int value) {
    return w -> {
      w.write(SqlWriter.ident(ident));
      w.writeOperator("=");
      w.writeLiteral(value);
    };
  }

  public static SqlGenerator eq(String ident, String value) {
    return w -> {
      w.write(SqlWriter.ident(ident));
      w.writeOperator("=");
      w.writeQuotedString(value);
    };
  }

  public static SqlGenerator eq(String ident, SqlGenerator value) {
    return w -> {
      w.write(SqlWriter.ident(ident));
      w.writeOperator("=");
      w.write(value);
    };
  }

  public static SqlGenerator exprList(SqlGenerator... fields) {
    return w -> {
      w.writeExprList(fields);
    };
  }

  public static SqlGenerator list(SqlGenerator seperator, Collection<SqlGenerator> exprs) {
    return list(seperator, exprs.stream());
  }

  public static SqlGenerator list(SqlGenerator seperator, Stream<SqlGenerator> exprs) {
    return list(seperator, exprs.toArray(SqlGenerator[]::new));
  }

  public static SqlGenerator list(SqlGenerator seperator, SqlGenerator... fields) {
    Arrays.stream(fields).forEach(Preconditions::checkNotNull);
    return w -> {
      for (int i = 0; i < fields.length; ++i) {
        if (i > 0) {
          w.write(seperator);
        }
        w.writeExprList(fields[i]);
      }
    };
  }

  public static SqlGenerator and(SqlGenerator... exprs) {
    return w -> {
      w.writeStartExpr();
      w.write(list(AND, exprs));
      w.writeEndExpr();
    };
  }

  public static SqlGenerator or(SqlGenerator... exprs) {
    return w -> {
      w.writeStartExpr();
      w.write(list(OR, exprs));
      w.writeEndExpr();
    };
  }

  public static SqlGenerator and(Collection<SqlGenerator> exprs) {
    return w -> {
      w.writeStartExpr();
      w.write(list(AND, exprs));
      w.writeEndExpr();
    };
  }

  public static SqlGenerator or(Collection<SqlGenerator> exprs) {
    return w -> {
      w.writeStartExpr();
      w.write(list(OR, exprs));
      w.writeEndExpr();
    };
  }

  public static SqlGenerator max(SqlGenerator field) {
    return w -> {
      w.writeFunction("max", field);
    };
  }

  public static SqlGenerator function(String name, SqlGenerator field) {
    return w -> {
      w.writeFunction(name, field);
    };
  }

  public static SqlGenerator function(String name, SqlGenerator... fields) {
    return w -> {
      w.writeFunction(name, fields);
    };
  }

  public static SqlGenerator function(String name, String... strvals) {
    return w -> {
      w.writeFunction(name, Arrays.stream(strvals).map(SqlWriter::quotedString).toArray(SqlGenerator[]::new));
    };
  }

  public static SqlGenerator dropTableIfExists(DbIdent ident) {
    return w -> {
      w.writeKeyword(DROP, TABLE, IF, EXISTS);
      w.writeIdent(ident);
    };
  }

  public static SqlGenerator dropTypesIfExistsCascade(DbIdent... types) {
    return w -> {
      w.writeKeyword(DROP, TYPE, IF, EXISTS);
      w.writeList(SqlWriter.comma(), types);
      w.writeKeyword(SqlKeyword.CASCADE);
    };
  }

  public static SqlGenerator dropViewIfExists(DbIdent ident) {
    return w -> {
      w.writeKeyword(DROP, VIEW, IF, EXISTS);
      w.writeIdent(ident);
    };
  }

  public static String toString(SqlGenerator gen) {
    SqlWriter w = new SqlWriter(true);
    gen.write(w);
    return w.toString();
  }

  public static SqlGenerator renameTable(DbIdent currentName, DbIdent targetName) {
    return w -> {
      w.writeKeyword(ALTER, TABLE);
      w.writeIdent(currentName);
      w.writeKeyword(RENAME, TO);
      ImmutableList<String> endpart = targetName.getNames();
      w.writeIdent(endpart.get(endpart.size() - 1));
    };
  }

  public static SqlGenerator vacuumAnalyze(DbIdent ident) {
    return w -> {
      w.writeKeyword(SqlKeyword.VACUUM);
      w.writeKeyword(SqlKeyword.ANALYZE);
      w.writeIdent(ident);
    };
  }

  public static SqlGenerator vacuum(DbIdent ident) {
    return w -> {
      w.writeKeyword(SqlKeyword.VACUUM);
      w.writeIdent(ident);
    };
  }

  public static SqlGenerator setLocal(String key, SqlGenerator value) {
    return w -> {
      w.writeKeyword(SqlKeyword.SET);
      w.writeKeyword(SqlKeyword.LOCAL);
      w.writeIdent(key);
      w.writeKeyword(SqlKeyword.TO);
      w.write(value);
    };
  }

  public static SqlGenerator setLocal(String key, String value) {
    return setLocal(key, SqlWriter.quotedString(value));
  }

  public static SqlGenerator show(String key) {
    return w -> {
      w.writeKeyword(SqlKeyword.SHOW);
      w.writeIdent(key);
    };
  }

  public static SqlGenerator not(SqlWriter.SqlGenerator expr) {

    return w -> {
      w.writeKeyword(NOT);
      w.write(expr);
    };

  }

  public static SqlGenerator inAnyArray(SqlGenerator field, SqlGenerator... items) {
    return w -> {
      w.write(field);
      w.writeOperator("=");
      w.writeKeyword(SqlKeyword.ANY);
      w.writeStartExpr();
      w.writeKeyword(SqlKeyword.ARRAY);
      w.writeOperator("[");
      w.writeList(SqlWriter.comma(), items);
      w.writeOperator("]");
      w.writeEndExpr();
    };
  }

  public static SqlGenerator orderBy(String columnName, SqlDirection direction) {
    return w -> {
      w.writeKeyword(SqlKeyword.ORDER, SqlKeyword.BY);
      w.writeIdent(columnName);
      switch (direction) {
        case ASC:
          w.writeKeyword(SqlKeyword.ASC);
          break;
        case DESC:
          w.writeKeyword(SqlKeyword.DESC);
          break;
        default:
          throw new IllegalArgumentException();
      }
    };
  }

  public static SqlGenerator orderBy(String columnName, SqlDirection direction, SqlNulls nulls) {
    return orderBy(SqlWriter.ident(columnName), direction, nulls);
  }

  public static SqlGenerator orderBy(SqlGenerator expr, SqlDirection direction, SqlNulls nulls) {
    return w -> {
      w.writeKeyword(SqlKeyword.ORDER, SqlKeyword.BY);
      w.write(orderByExpr(expr, direction, nulls));
    };
  }

  public static SqlGenerator orderByExpr(SqlGenerator expr, SqlDirection direction, SqlNulls nulls) {

    return w -> {

      w.write(expr);

      if (direction != null) {
        switch (direction) {
          case ASC:
            w.writeKeyword(SqlKeyword.ASC);
            break;
          case DESC:
            w.writeKeyword(SqlKeyword.DESC);
            break;
          default:
            throw new IllegalArgumentException();
        }
      }

      if (nulls == SqlNulls.LAST) {
        w.writeKeyword(SqlKeyword.NULLS);
        w.writeKeyword(SqlKeyword.LAST);
      }

    };

  }

  public static SqlGenerator array(String... items) {
    return array(Arrays.stream(items).map(SqlWriters::literal).toArray(SqlGenerator[]::new));
  }

  public static SqlGenerator array(SqlGenerator... items) {
    return w -> {
      w.writeKeyword(SqlKeyword.ARRAY);
      w.writeOperator("[");
      w.writeList(SqlWriter.comma(), items);
      w.writeOperator("]");
    };
  }

  public static SqlGenerator cast(SqlGenerator expr, String type) {
    return w -> {
      w.write(expr);
      w.writeOperator("::");
      w.writeTypename(type);
    };
  }

  public static SqlGenerator binaryExpression(String operator, SqlGenerator left, SqlGenerator right) {
    return w -> {
      w.write(left);
      w.writeOperator(operator);
      w.write(right);
    };
  }

  public static SqlGenerator binaryExpression(String operator, SqlGenerator left, int right) {
    return binaryExpression(operator, left, literal(right));
  }

  public static SqlGenerator binaryExpression(String operator, SqlGenerator left, String right) {
    return w -> {
      w.write(left);
      w.writeOperator(operator);
      w.writeQuotedString(right);
    };
  }

  public static SqlGenerator binaryExpression(String operator, String fieldName, SqlGenerator right) {
    return binaryExpression(operator, SqlWriter.ident(fieldName), right);
  }

  public static SqlGenerator lower(SqlGenerator arg) {
    return function("lower", arg);
  }

  public static SqlGenerator lowerIdent(String arg) {
    return function("lower", ident(arg));
  }

  public static SqlGenerator between(SqlGenerator left, String lower, String upper) {
    return between(left, literal(lower), literal(upper));
  }

  public static SqlGenerator between(SqlGenerator left, int lower, int upper) {
    return between(left, literal(lower), literal(upper));
  }

  public static SqlGenerator between(SqlGenerator left, SqlGenerator lower, SqlGenerator upper) {
    return w -> {
      w.write(left);
      w.writeKeyword(SqlKeyword.BETWEEN);
      w.write(lower);
      w.writeKeyword(SqlKeyword.AND);
      w.write(upper);
    };
  }

  public static SqlGenerator int4range(int lower, int upper) {
    return function("int4range", literal(lower), literal(upper));
  }

  public static SqlGenerator isNull(SqlGenerator expr) {
    return w -> {
      w.write(expr);
      w.writeKeyword(SqlKeyword.IS);
      w.writeKeyword(SqlKeyword.NULL);
    };
  }

  public static SqlGenerator isNotNull(SqlGenerator expr) {
    return w -> {
      w.write(expr);
      w.writeKeyword(SqlKeyword.IS);
      w.writeKeyword(SqlKeyword.NOT);
      w.writeKeyword(SqlKeyword.NULL);
    };
  }

  public static Collector<SqlGenerator, ImmutableList.Builder<SqlGenerator>, SqlGenerator> toList(SqlKeyword joiner) {

    return new Collector<SqlWriter.SqlGenerator, ImmutableList.Builder<SqlGenerator>, SqlWriter.SqlGenerator>() {

      @Override
      public Supplier<ImmutableList.Builder<SqlGenerator>> supplier() {
        return () -> ImmutableList.builder();
      }

      @Override
      public BiConsumer<ImmutableList.Builder<SqlGenerator>, SqlGenerator> accumulator() {
        return (a, b) -> {
          a.add(b);
        };
      }

      @Override
      public BinaryOperator<ImmutableList.Builder<SqlGenerator>> combiner() {
        return (a, b) -> a.addAll(b.build());
      }

      @Override
      public Function<ImmutableList.Builder<SqlGenerator>, SqlGenerator> finisher() {
        return (a) -> SqlWriters.list(joiner, a.build());
      }

      @Override
      public Set<Characteristics> characteristics() {
        return ImmutableSet.of(Characteristics.CONCURRENT);
      }

    };

  }

  public static SqlGenerator dropIndexIfExists(String indexName) {
    return w -> {
      w.writeKeyword(DROP);
      w.writeKeyword(INDEX);
      w.writeKeyword(IF, EXISTS);
      w.writeIdent(indexName);
    };
  }

  public static SqlGenerator createExtensionIfNotExists(String extensionName, String schema) {
    return w -> {
      w.writeKeyword(SqlKeyword.CREATE, SqlKeyword.EXTENSION, SqlKeyword.IF, SqlKeyword.NOT, SqlKeyword.EXISTS);
      w.writeIdent(extensionName);
      w.writeKeyword(SqlKeyword.WITH, SqlKeyword.SCHEMA);
      w.writeIdent(schema);
    };
  }

  public static SqlGenerator ident(String... idents) {
    return SqlWriter.ident(idents);
  }

  public static SqlGenerator substring(SqlGenerator ident, int start, int end) {
    return function("substring", ident, literal(start), literal(end));
  }

  public static SqlGenerator ne(String ident, int value) {
    return w -> {
      w.writeIdent(ident);
      w.writeOperator("<>");
      w.writeLiteral(value);
    };
  }

  public static SqlGenerator multiply(SqlGenerator left, int right) {
    return binaryExpression("*", left, right);
  }

  public static SqlGenerator coalesce(SqlGenerator... exprs) {
    return function("coalesce", exprs);
  }

  public static SqlGenerator extractEpochFrom(SqlGenerator expr) {
    return function("extract", (SqlWriter w) -> {
      w.writeKeyword(SqlKeyword.EPOCH);
      w.writeKeyword(SqlKeyword.FROM);
      w.write(expr);
    });
  }

  public static SqlGenerator coalesce(SqlGenerator val1, int val2) {
    return coalesce(val1, literal(val2));
  }

  public static SqlGenerator as(SqlGenerator expr, String label) {
    return w -> {
      w.write(expr);
      w.writeKeyword(SqlKeyword.AS);
      w.writeIdent(label);
    };
  }

  public static SqlGenerator notify(String channel) {
    return w -> {
      w.writeKeyword(SqlKeyword.NOTIFY);
      w.writeIdent(channel);
    };
  }

  public static SqlGenerator notify(String channel, String payload) {
    return w -> {
      w.writeKeyword(SqlKeyword.NOTIFY);
      w.writeIdent(channel);
      if (payload != null) {
        w.writeComma();
        w.writeQuotedString(payload);
      }
    };
  }

  public static SqlGenerator gte(SqlGenerator left, SqlGenerator right) {
    return binaryExpression(">= ", left, right);
  }

  public static SqlGenerator lt(SqlGenerator left, SqlGenerator right) {
    return binaryExpression("< ", left, right);
  }

  public static SqlGenerator left(SqlGenerator ident, int i) {
    return function("left", ident, literal(i));
  }

  public static SqlGenerator star() {
    return w -> {
      w.writeStar();
    };
  }

  public static SqlGenerator alterTableAddPrimaryKey(DbIdent tableName, SqlGenerator... items) {
    return w -> {

      w.writeKeyword(SqlKeyword.ALTER);
      w.writeKeyword(SqlKeyword.TABLE);
      w.writeIdent(tableName);
      w.writeKeyword(SqlKeyword.ADD);
      w.writeKeyword(SqlKeyword.PRIMARY);
      w.writeKeyword(SqlKeyword.KEY);

      w.writeExprList(items);

    };
  }

}
