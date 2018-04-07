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

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.zrz.sqlwriter.SqlWriter.SqlGenerator;

public class SqlWriters {

  public static SqlWriter.SqlGenerator createSchema(final String schemaName) {
    return (w) -> {
      w.writeKeyword(CREATE, SCHEMA);
      w.writeIdent(schemaName);
    };
  }

  public static SqlWriter.SqlGenerator createSchemaIfNotExists(final String schemaName) {
    return (w) -> {
      w.writeKeyword(CREATE, SCHEMA, IF, NOT, EXISTS);
      w.writeIdent(schemaName);
    };
  }

  public static SqlWriter.SqlGenerator columnIdent(final DbIdent ident) {
    return (w) -> {

      final List<String> idents = Lists.newArrayList(ident.getNames());

      final String columnName = idents.remove(idents.size() - 1);

      w.writeKeyword(SqlKeyword.REFERENCES);

      w.writeIdent(idents);
      w.writeStartExpr();
      w.writeIdent(columnName);
      w.writeEndExpr();

    };
  }

  public static SqlGenerator deleteFrom(final DbIdent table, final SqlGenerator where) {
    return w -> {
      w.writeKeyword(SqlKeyword.DELETE);
      w.writeKeyword(SqlKeyword.FROM);
      w.writeIdent(table);
      w.writeKeyword(SqlKeyword.WHERE);
      w.write(where);
    };
  }

  public static SqlGenerator deleteFrom(final DbIdent table, final SqlGenerator where, final SqlGenerator... returning) {
    return w -> {
      w.writeKeyword(SqlKeyword.DELETE);
      w.writeKeyword(SqlKeyword.FROM);
      w.writeIdent(table);
      w.writeKeyword(SqlKeyword.WHERE);
      w.write(where);
      if (returning != null && returning.length > 0) {
        w.writeKeyword(SqlKeyword.RETURNING);
        w.writeList(SqlWriter.comma(), returning);
      }
    };
  }

  public static SqlGenerator insertInto(final DbIdent target, final SqlGenerator select) {
    return w -> {
      w.writeKeyword(SqlKeyword.INSERT);
      w.writeKeyword(SqlKeyword.INTO);
      w.writeIdent(target);
      w.write(select);
    };
  }

  public static SqlGenerator insertInto(final DbIdent target, final List<String> fields, final SqlGenerator select) {
    return w -> {
      w.writeKeyword(SqlKeyword.INSERT);
      w.writeKeyword(SqlKeyword.INTO);
      w.writeIdent(target);
      w.writeExprList(fields.toArray(new String[0]));
      w.write(select);
    };
  }

  public static SqlGenerator merge(final DbIdent target, final List<String> fields, final SqlGenerator values, final Map<String, SqlGenerator> mergeFields,
      final SqlGenerator... returning) {
    return w -> {

      w.writeKeyword(SqlKeyword.INSERT);
      w.writeKeyword(SqlKeyword.INTO);

      w.writeIdent(target);

      w.writeExprList(fields.toArray(new String[0]));

      w.write(values);

      w.writeKeyword(SqlKeyword.ON);
      w.writeKeyword(SqlKeyword.CONFLICT);

      w.writeExprList(Sets.difference(ImmutableSet.copyOf(fields), mergeFields.keySet()).stream().map(SqlWriters::ident));

      w.writeKeyword(SqlKeyword.DO);
      w.writeKeyword(SqlKeyword.UPDATE);
      w.writeKeyword(SqlKeyword.SET);

      final List<SqlGenerator> mergeKeys = new LinkedList<>();
      final List<SqlGenerator> mergeValues = new LinkedList<>();

      mergeFields.forEach((k, v) -> {

        mergeKeys.add(ident(k));
        mergeValues.add(v);

      });

      w.writeExprList(mergeKeys);
      w.writeOperator("=");
      w.writeExprList(mergeValues);

      if (returning.length > 0) {
        w.writeKeyword(SqlKeyword.RETURNING);
        w.writeList(comma(), returning);
      }

    };
  }

  public static SqlWriter.SqlGenerator copyBinaryFromStdin(final DbIdent tableName, final String... columns) {

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

  public static SqlWriter.SqlGenerator copyBinaryFromStdin(final DbIdent tableName, final Collection<String> columns) {

    return w -> {

      w.writeKeyword(SqlKeyword.COPY);
      w.writeIdent(tableName);

      if (!columns.isEmpty()) {
        w.writeExprList(SqlWriters.idents(columns));
      }

      w.writeKeyword(SqlKeyword.FROM);
      w.writeKeyword(SqlKeyword.STDIN);
      w.writeKeyword(SqlKeyword.WITH);
      w.writeKeyword(SqlKeyword.BINARY);

    };

  }

  public static SqlWriter.SqlGenerator createTable(final String schemaName, final String tableName) {
    return (w) -> {
      w.writeKeyword(CREATE, TABLE, IF, NOT, EXISTS);
      w.writeIdent(schemaName, tableName);
      w.writeStartExpr();
      w.writeEndExpr();
    };
  }

  public static SqlWriter.SqlGenerator indexItem(final String columnName, final String opclass) {
    return (w) -> {
      w.writeIdent(columnName);
      if (opclass != null)
        w.writeIdent(opclass);
    };

  }

  public static SqlWriter.SqlGenerator indexItem(final String columnName, final String opclass, final SqlDirection direction, final SqlNulls nulls) {
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

  public static SqlWriter.SqlGenerator indexItem(final SqlGenerator columnName, final String opclass, final SqlDirection direction, final SqlNulls nulls) {
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

  public static SqlWriter.SqlGenerator indexItem(final String columnName, final SqlDirection direction, final SqlNulls nulls) {
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

  public static SqlWriter.SqlGenerator createUniqueIndex(final String idxname, final String type, final DbIdent tblname, final SqlGenerator... indexItems) {
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

  public static SqlWriter.SqlGenerator createIndex(final String idxname, final String type, final DbIdent tblname, final SqlGenerator... indexItems) {
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

  public static SqlWriter.SqlGenerator createBtreeIndex(final String id, final DbIdent tblname, final SqlGenerator... indexItems) {
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

  public static SqlGenerator now(final ZoneOffset offset) {
    return w -> {
      w.writeKeyword(SqlKeyword.NOW);
      w.writeStartExpr();
      w.writeEndExpr();
      w.writeKeyword(SqlKeyword.AT);
      w.writeKeyword(SqlKeyword.TIME);
      w.writeKeyword(SqlKeyword.ZONE);
      w.writeQuotedString(offset.getId());

    };
  }

  public static SqlGenerator attachTable(final DbIdent master, final DbIdent partition, final SqlGenerator... values) {
    return attachTable(master, partition, ImmutableList.copyOf(values));
  }

  public static SqlGenerator attachTable(final DbIdent master, final DbIdent partition, final Collection<SqlGenerator> values) {

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

  public static SqlGenerator detachTable(final DbIdent master, final DbIdent partition) {

    return w -> {

      // ALTER TABLE users DETACH PARTITION "xxx".yyy

      w.writeKeyword(ALTER, TABLE);
      w.writeIdent(master);

      w.writeKeyword(DETACH, PARTITION);
      w.writeIdent(partition);

    };

  }

  public static SqlGenerator alterTablespace(final DbIdent master, final String tablespace) {

    return w -> {

      // ALTER TABLE users SET TABLESPACE 'xxx';

      w.writeKeyword(ALTER, TABLE);
      w.writeIdent(master);

      w.writeKeyword(SET, TABLESPACE);
      w.writeQuotedString(tablespace);

    };

  }

  public static SqlGenerator setLogged(final DbIdent master, final boolean logged) {

    return w -> {

      // ALTER TABLE users SET LOGGED | UNLOGGED;

      w.writeKeyword(ALTER, TABLE);
      w.writeIdent(master);
      w.writeKeyword(SET);

      w.writeKeyword(logged ? LOGGED : UNLOGGED);

    };

  }

  public static SqlGenerator literal(final Set<String> values) {
    return w -> w.write(array(values.stream().map(SqlWriters::literal)));
  }

  public static SqlGenerator literal(final Duration internal) {
    return cast(literal(SqlUtils.toSqlString(internal)), PgTypes.INTERVAL);
  }

  public static SqlGenerator literal(final Instant time) {
    return cast(literal(time.toString()), PgTypes.TIMESTAMPTZ);
  }

  public static SqlGenerator literal(final int i) {
    return w -> w.writeLiteral(i);
  }

  public static SqlGenerator literal(final byte[] bytea) {
    return w -> w.writeByteArray(bytea);
  }

  public static SqlGenerator literal(final long i) {
    return w -> w.writeLiteral(i);
  }

  public static SqlGenerator literal(final double i) {
    return w -> w.writeLiteral(i);
  }

  public static SqlGenerator literal(final String value) {
    Objects.requireNonNull(value);
    return w -> w.writeQuotedString(value);
  }

  public static SqlGenerator literal(final String value, final DbIdent type) {
    return cast(literal(value), type, 0);
  }

  public static SqlGenerator literal(final String value, final DbIdent type, final int dims) {
    return cast(literal(value), type, dims);
  }

  public static SqlGenerator literal(final boolean value) {
    return w -> w.writeLiteral(value);
  }

  public static SqlGenerator literal(final LocalDateTime value) {
    return w -> {

      w.writeQuotedString(value.toString());
      w.writeOperator("::timestamp");

    };
  }

  public static SqlGenerator[] idents(final String... columns) {

    final SqlGenerator[] res = new SqlGenerator[columns.length];

    for (int i = 0; i < columns.length; ++i) {
      final String value = columns[i];
      res[i] = w -> w.writeIdent(value);
    }

    return res;

  }

  public static SqlGenerator[] idents(final Collection<String> columns) {
    return columns.stream().map(x -> ident(x)).toArray(SqlGenerator[]::new);
  }

  public static SqlGenerator select(final DbIdent table, final SqlGenerator filter, final String... columns) {
    return select(table, filter, Arrays.stream(columns).map(SqlWriters::ident).toArray(SqlGenerator[]::new));
  }

  public static SqlGenerator select(final DbIdent table, final SqlGenerator filter, final SqlGenerator... columns) {
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

  public static SqlGenerator select(final DbIdent table, final Optional<SqlGenerator> filter, final SqlGenerator... columns) {
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

  public static SqlGenerator selectGroupBy(final DbIdent table, final SqlGenerator groupBy, final SqlGenerator... columns) {
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

  public static SqlGenerator alterSchemaOwner(final String schemaName, final String owner) {
    // ALTER SCHEMA xxx OWNER TO "yyy";
    return w -> {
      w.writeKeyword(ALTER, SCHEMA);
      w.writeIdent(schemaName);
      w.writeKeyword(OWNER, TO);
      w.writeIdent(owner);
    };
  }

  public static SqlGenerator grantUsageOnSchema(final String schemaName, final String whom) {
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

  public static SqlGenerator grantTable(final SqlKeyword what, final DbIdent table, final String whom) {
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

  public static SqlGenerator grantOnAllTables(final String schemaName, final String whom, final SqlKeyword... what) {
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

  public static SqlGenerator identStartsWith(final String ident, final String value) {
    return w -> {
      w.writeIdent(ident);
      w.writeKeyword(SqlKeyword.LIKE);
      w.writeQuotedString(value + "%");
    };
  }

  public static SqlGenerator identLike(final String ident, final String value) {
    return w -> {
      w.writeIdent(ident);
      w.writeOperator("=");
      w.writeQuotedString(value);
    };
  }

  public static SqlGenerator eq(final SqlGenerator ident, final String value) {
    return w -> {
      w.write(ident);
      w.writeOperator("=");
      w.writeQuotedString(value);
    };
  }

  public static SqlGenerator eq(final SqlGenerator ident, final SqlGenerator value) {
    return w -> {
      w.write(ident);
      w.writeOperator("=");
      w.write(value);
    };
  }

  public static SqlGenerator eq(final String ident, final int value) {
    return w -> {
      w.write(SqlWriter.ident(ident));
      w.writeOperator("=");
      w.writeLiteral(value);
    };
  }

  public static SqlGenerator eq(final String ident, final String value) {
    return w -> {
      w.write(SqlWriter.ident(ident));
      w.writeOperator("=");
      w.writeQuotedString(value);
    };
  }

  public static SqlGenerator eq(final String ident, final SqlGenerator value) {
    return w -> {
      w.write(SqlWriter.ident(ident));
      w.writeOperator("=");
      w.write(value);
    };
  }

  public static SqlGenerator exprList(final SqlGenerator... fields) {
    return w -> {
      w.writeExprList(fields);
    };
  }

  public static SqlGenerator list(final SqlGenerator seperator, final Collection<SqlGenerator> exprs) {
    return list(seperator, exprs.stream());
  }

  public static SqlGenerator list(final SqlGenerator seperator, final Stream<SqlGenerator> exprs) {
    return list(seperator, exprs.toArray(SqlGenerator[]::new));
  }

  public static SqlGenerator list(final SqlGenerator seperator, final SqlGenerator... fields) {
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

  public static SqlGenerator and(final SqlGenerator... exprs) {
    return w -> {
      w.writeStartExpr();
      w.write(list(AND, exprs));
      w.writeEndExpr();
    };
  }

  public static SqlGenerator or(final SqlGenerator... exprs) {
    return w -> {
      w.writeStartExpr();
      w.write(list(OR, exprs));
      w.writeEndExpr();
    };
  }

  public static SqlGenerator and(final Collection<SqlGenerator> exprs) {
    return w -> {
      w.writeStartExpr();
      w.write(list(AND, exprs));
      w.writeEndExpr();
    };
  }

  public static SqlGenerator or(final Collection<SqlGenerator> exprs) {
    return w -> {
      w.writeStartExpr();
      w.write(list(OR, exprs));
      w.writeEndExpr();
    };
  }

  public static SqlGenerator max(final SqlGenerator field) {
    return w -> {
      w.writeFunction("max", field);
    };
  }

  public static SqlGenerator function(final String name, final SqlGenerator field) {
    return w -> {
      w.writeFunction(name, field);
    };
  }

  public static SqlGenerator function(final String name, final SqlGenerator... fields) {
    return w -> {
      w.writeFunction(name, fields);
    };
  }

  public static SqlGenerator function(final String name, final String... strvals) {
    return w -> {
      w.writeFunction(name, Arrays.stream(strvals).map(SqlWriter::quotedString).toArray(SqlGenerator[]::new));
    };
  }

  public static SqlGenerator dropTableIfExists(final DbIdent ident) {
    return w -> {
      w.writeKeyword(DROP, TABLE, IF, EXISTS);
      w.writeIdent(ident);
    };
  }

  public static SqlGenerator dropTypesIfExistsCascade(final DbIdent... types) {
    return w -> {
      w.writeKeyword(DROP, TYPE, IF, EXISTS);
      w.writeList(SqlWriter.comma(), types);
      w.writeKeyword(SqlKeyword.CASCADE);
    };
  }

  public static SqlGenerator dropViewIfExists(final DbIdent ident) {
    return w -> {
      w.writeKeyword(DROP, VIEW, IF, EXISTS);
      w.writeIdent(ident);
    };
  }

  public static String toString(final SqlGenerator gen) {
    final SqlWriter w = new SqlWriter(true);
    gen.write(w);
    return w.toString();
  }

  public static SqlGenerator renameTable(final DbIdent currentName, final DbIdent targetName) {
    return w -> {
      w.writeKeyword(ALTER, TABLE);
      w.writeIdent(currentName);
      w.writeKeyword(RENAME, TO);
      final ImmutableList<String> endpart = targetName.getNames();
      w.writeIdent(endpart.get(endpart.size() - 1));
    };
  }

  public static SqlGenerator vacuumAnalyze(final DbIdent ident) {
    return w -> {
      w.writeKeyword(SqlKeyword.VACUUM);
      w.writeKeyword(SqlKeyword.ANALYZE);
      w.writeIdent(ident);
    };
  }

  public static SqlGenerator vacuum(final DbIdent ident) {
    return w -> {
      w.writeKeyword(SqlKeyword.VACUUM);
      w.writeIdent(ident);
    };
  }

  public static SqlGenerator setLocal(final String key, final SqlGenerator value) {
    return w -> {
      w.writeKeyword(SqlKeyword.SET);
      w.writeKeyword(SqlKeyword.LOCAL);
      w.writeIdent(key);
      w.writeKeyword(SqlKeyword.TO);
      w.write(value);
    };
  }

  public static SqlGenerator set(final String key, final SqlGenerator value) {
    return w -> {
      w.writeKeyword(SqlKeyword.SET);
      w.writeIdent(key);
      w.writeKeyword(SqlKeyword.TO);
      w.write(value);
    };
  }

  public static SqlGenerator setLocal(final String key, final String value) {
    return setLocal(key, SqlWriter.quotedString(value));
  }

  public static SqlGenerator show(final String key) {
    return w -> {
      w.writeKeyword(SqlKeyword.SHOW);
      w.writeIdent(key);
    };
  }

  public static SqlGenerator not(final SqlWriter.SqlGenerator expr) {

    return w -> {
      w.writeKeyword(NOT);
      w.write(expr);
    };

  }

  public static SqlGenerator inAnyArray(final SqlGenerator field, final SqlGenerator... items) {
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

  public static SqlGenerator inAnyArray(final SqlGenerator field, final IntStream values) {
    return w -> {
      w.write(field);
      w.writeOperator("=");
      w.writeKeyword(SqlKeyword.ANY);
      w.writeStartExpr();
      w.writeKeyword(SqlKeyword.ARRAY);
      w.writeOperator("[");
      w.writeList(SqlWriter.comma(), values.mapToObj(SqlWriters::literal));
      w.writeOperator("]");
      w.writeEndExpr();
    };
  }

  public static SqlGenerator orderBy(final String columnName, final SqlDirection direction) {
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

  public static SqlGenerator orderBy(final String columnName, final SqlDirection direction, final SqlNulls nulls) {
    return orderBy(SqlWriter.ident(columnName), direction, nulls);
  }

  public static SqlGenerator orderBy(final SqlGenerator expr, final SqlDirection direction, final SqlNulls nulls) {
    return w -> {
      w.writeKeyword(SqlKeyword.ORDER, SqlKeyword.BY);
      w.write(orderByExpr(expr, direction, nulls));
    };
  }

  public static SqlGenerator orderByExpr(final SqlGenerator expr, final SqlDirection direction, final SqlNulls nulls) {

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

  public static SqlGenerator array(final String... items) {
    return cast(array(Arrays.stream(items).map(SqlWriters::literal).toArray(SqlGenerator[]::new)), "text", 1);
  }

  public static SqlGenerator array(final Stream<SqlGenerator> stream) {
    return array(stream.toArray(SqlGenerator[]::new));
  }

  public static SqlGenerator array(final SqlGenerator... items) {
    return w -> {
      w.writeKeyword(SqlKeyword.ARRAY);
      w.writeOperator("[");
      w.writeList(SqlWriter.comma(), items);
      w.writeOperator("]");
    };
  }

  public static SqlGenerator cast(final SqlGenerator expr, final SqlType type) {
    return w -> {
      w.write(expr);
      w.writeOperator("::");
      w.writeTypename(type.ident(), 0);
    };
  }

  public static SqlGenerator cast(final SqlGenerator expr, final String type, final int dims) {
    return w -> {
      w.write(expr);
      w.writeOperator("::");
      w.writeTypename(type, dims);
    };
  }

  public static SqlGenerator cast(final SqlGenerator expr, final DbIdent type, final int dims) {
    return w -> {
      w.write(expr);
      w.writeOperator("::");
      w.writeTypename(type, dims);
    };
  }

  public static SqlGenerator binaryExpression(final String operator, final SqlGenerator left, final SqlGenerator right) {
    return w -> {
      w.write(left);
      w.writeOperator(operator);
      w.write(right);
    };
  }

  public static SqlGenerator binaryExpression(final String operator, final SqlGenerator left, final int right) {
    return binaryExpression(operator, left, literal(right));
  }

  public static SqlGenerator binaryExpression(final String operator, final SqlGenerator left, final String right) {
    return w -> {
      w.write(left);
      w.writeOperator(operator);
      w.writeQuotedString(right);
    };
  }

  public static SqlGenerator binaryExpression(final String operator, final String fieldName, final SqlGenerator right) {
    return binaryExpression(operator, SqlWriter.ident(fieldName), right);
  }

  public static SqlGenerator lower(final SqlGenerator arg) {
    return function("lower", arg);
  }

  public static SqlGenerator lowerIdent(final String arg) {
    return function("lower", ident(arg));
  }

  public static SqlGenerator between(final SqlGenerator left, final String lower, final String upper) {
    return between(left, literal(lower), literal(upper));
  }

  public static SqlGenerator between(final SqlGenerator left, final int lower, final int upper) {
    return between(left, literal(lower), literal(upper));
  }

  public static SqlGenerator between(final SqlGenerator left, final SqlGenerator lower, final SqlGenerator upper) {
    return w -> {
      w.write(left);
      w.writeKeyword(SqlKeyword.BETWEEN);
      w.write(lower);
      w.writeKeyword(SqlKeyword.AND);
      w.write(upper);
    };
  }

  public static SqlGenerator int4range(final int lower, final int upper) {
    return function("int4range", literal(lower), literal(upper));
  }

  public static SqlGenerator isNull(final SqlGenerator expr) {
    return w -> {
      w.write(expr);
      w.writeKeyword(SqlKeyword.IS);
      w.writeKeyword(SqlKeyword.NULL);
    };
  }

  public static SqlGenerator isNotNull(final SqlGenerator expr) {
    return w -> {
      w.write(expr);
      w.writeKeyword(SqlKeyword.IS);
      w.writeKeyword(SqlKeyword.NOT);
      w.writeKeyword(SqlKeyword.NULL);
    };
  }

  public static Collector<SqlGenerator, ImmutableList.Builder<SqlGenerator>, SqlGenerator> toList(final SqlKeyword joiner) {

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

  public static SqlGenerator dropIndexIfExists(final String indexName) {
    return w -> {
      w.writeKeyword(DROP);
      w.writeKeyword(INDEX);
      w.writeKeyword(IF, EXISTS);
      w.writeIdent(indexName);
    };
  }

  public static SqlGenerator createExtensionIfNotExists(final String extensionName, final String schema) {
    return w -> {
      w.writeKeyword(SqlKeyword.CREATE, SqlKeyword.EXTENSION, SqlKeyword.IF, SqlKeyword.NOT, SqlKeyword.EXISTS);
      w.writeIdent(extensionName);
      w.writeKeyword(SqlKeyword.WITH, SqlKeyword.SCHEMA);
      w.writeIdent(schema);
    };
  }

  public static SqlGenerator ident(final String... idents) {
    return SqlWriter.ident(idents);
  }

  public static SqlGenerator substring(final SqlGenerator ident, final int start, final int end) {
    return function("substring", ident, literal(start), literal(end));
  }

  public static SqlGenerator ne(final String ident, final int value) {
    return w -> {
      w.writeIdent(ident);
      w.writeOperator("<>");
      w.writeLiteral(value);
    };
  }

  public static SqlGenerator multiply(final SqlGenerator left, final int right) {
    return parenthisize(binaryExpression("*", left, right));
  }

  private static SqlGenerator parenthisize(final SqlGenerator expr) {
    return w -> {
      w.writeStartExpr();
      w.write(expr);
      w.writeEndExpr();
    };
  }

  public static SqlGenerator divide(final SqlGenerator left, final SqlGenerator right) {
    return binaryExpression("/", left, right);
  }

  public static SqlGenerator coalesce(final SqlGenerator... exprs) {
    return function("coalesce", exprs);
  }

  public static SqlGenerator extractEpochFrom(final SqlGenerator expr) {
    return function("extract", (final SqlWriter w) -> {
      w.writeKeyword(SqlKeyword.EPOCH);
      w.writeKeyword(SqlKeyword.FROM);
      w.write(expr);
    });
  }

  public static SqlGenerator extractEpochMillisFrom(final SqlGenerator expr) {
    return cast(multiply(extractEpochFrom(expr), 1000), PgTypes.INT8);
  }

  public static SqlGenerator coalesce(final SqlGenerator val1, final int val2) {
    return coalesce(val1, literal(val2));
  }

  public static SqlGenerator as(final SqlGenerator expr, final String label) {
    return w -> {
      w.write(expr);
      w.writeKeyword(SqlKeyword.AS);
      w.writeIdent(label);
    };
  }

  public static SqlGenerator notify(final String channel) {
    return w -> {
      w.writeKeyword(SqlKeyword.NOTIFY);
      w.writeIdent(channel);
    };
  }

  public static SqlGenerator notify(final String channel, final String payload) {
    return w -> {
      w.writeKeyword(SqlKeyword.NOTIFY);
      w.writeIdent(channel);
      if (payload != null) {
        w.writeComma();
        w.writeQuotedString(payload);
      }
    };
  }

  public static SqlGenerator gte(final SqlGenerator left, final SqlGenerator right) {
    return binaryExpression(">= ", left, right);
  }

  public static SqlGenerator lt(final SqlGenerator left, final SqlGenerator right) {
    return binaryExpression("< ", left, right);
  }

  public static SqlGenerator lt(final SqlGenerator left, final long right) {
    return binaryExpression("< ", left, literal(right));
  }

  public static SqlGenerator left(final SqlGenerator ident, final int i) {
    return function("left", ident, literal(i));
  }

  public static SqlGenerator star() {
    return w -> {
      w.writeStar();
    };
  }

  public static SqlGenerator alterTableAddPrimaryKey(final DbIdent tableName, final SqlGenerator... items) {
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

  public static SqlGenerator selectCount(final DbIdent table) {
    return w -> {
      w.writeKeyword(SqlKeyword.SELECT);
      w.writeKeyword(SqlKeyword.COUNT);
      w.writeStartExpr();
      w.writeStar();
      w.writeEndExpr();
      w.writeKeyword(SqlKeyword.FROM);
      w.writeIdent(table);
    };
  }

  public static SqlGenerator selectOne(final DbIdent table, final SqlGenerator... fields) {
    return select(table, 1, fields);
  }

  public static SqlGenerator select(final DbIdent table, final int limit, final SqlGenerator... fields) {
    return w -> {
      w.writeKeyword(SqlKeyword.SELECT);
      w.write(SqlWriters.list(SqlWriter.comma(), fields));
      w.writeKeyword(SqlKeyword.FROM);
      w.writeIdent(table);
      w.writeKeyword(SqlKeyword.LIMIT);
      w.writeLiteral(limit);
    };
  }

  public static SqlGenerator with(final ImmutableMap<String, SqlGenerator> withers, final SqlGenerator cmd) {
    return w -> {

      w.writeKeyword(SqlKeyword.WITH);

      w.writeList(comma(true), withers.entrySet().stream().map(val -> {
        return cw -> {
          cw.writeIdent(val.getKey());
          cw.writeKeyword(SqlKeyword.AS);
          cw.writeStartExpr();
          cw.writeNewline(true);
          cw.write(val.getValue());
          cw.writeEndExpr();
        };
      }));

      w.writeNewline(false);

      w.write(cmd);

    };

  }

  private static final SqlGenerator _comma = (w) -> w.writeComma();
  private static final SqlGenerator _commaAndNewLine = (w) -> {
    w.writeComma();
    w.writeNewline();
  };

  private static SqlGenerator comma() {
    return _comma;
  }

  private static SqlGenerator comma(final boolean newline) {
    return newline ? _commaAndNewLine : _comma;
  }

  public static SqlGenerator subselect(final QueryGenerator statement) {
    return w -> {
      w.writeStartExpr();
      w.write(statement);
      w.writeEndExpr();
    };
  }

  public static SqlGenerator any(final SqlGenerator arrayValue) {
    return w -> {
      w.writeKeyword(SqlKeyword.ANY);
      w.writeStartExpr();
      w.write(arrayValue);
      w.writeEndExpr();
    };
  }

  public static SqlGenerator count() {
    return w -> {
      w.writeKeyword(SqlKeyword.COUNT);
      w.writeStartExpr();
      w.writeStar();
      w.writeEndExpr();
    };
  }

  public static SqlGenerator update(final DbIdent updateTable, final ImmutableMap<String, SqlGenerator> updates, final DbIdent fromTable,
      final SqlGenerator where) {
    return w -> {
      w.writeKeyword(SqlKeyword.UPDATE);
      w.writeIdent(updateTable);
      w.writeKeyword(SqlKeyword.SET);
      w.writeList(comma(false), updates.entrySet().stream().map(e -> eq(ident(e.getKey()), e.getValue())));
      if (fromTable != null) {
        w.writeKeyword(SqlKeyword.FROM);
        w.writeIdent(fromTable);
      }
      if (where != null) {
        w.writeKeyword(SqlKeyword.WHERE);
        w.write(where);
      }
    };
  }

  public static SqlGenerator begin() {
    return w -> {
      w.writeKeyword(SqlKeyword.BEGIN);
    };
  }

  public static SqlGenerator commit() {
    return w -> {
      w.writeKeyword(SqlKeyword.COMMIT);
    };
  }

  public static SqlGenerator rollback() {
    return w -> {
      w.writeKeyword(SqlKeyword.ROLLBACK);
    };
  }

  public static SqlGenerator defaultValues() {
    return w -> w.writeKeyword(SqlKeyword.DEFAULT, SqlKeyword.VALUES);
  }

  public static SqlGenerator values(final Stream<SqlGenerator> values) {
    return w -> {
      w.writeKeyword(SqlKeyword.VALUES);
      w.writeList(SqlWriters.comma(true), values);
    };
  }

  public static SqlGenerator values(final SqlGenerator... values) {
    return w -> {
      w.writeKeyword(SqlKeyword.VALUES);
      w.writeList(SqlWriters.comma(true), values);
    };
  }

  public static SqlGenerator onConflictDoUpdate(final String... idents) {
    return w -> {

      w.writeKeyword(SqlKeyword.ON);
      w.writeKeyword(SqlKeyword.CONFLICT);
      w.writeKeyword(SqlKeyword.DO);
      w.writeKeyword(SqlKeyword.UPDATE);
      w.writeKeyword(SqlKeyword.SET);

      w.writeExprList(Arrays.stream(idents).map(ident -> ident(ident)));
      w.writeOperator("=");
      w.writeExprList(Arrays.stream(idents).map(ident -> ident("excluded", ident)));

    };
  }

  public static SqlGenerator excluded(final String name) {
    return w -> {
      w.writeKeyword(SqlKeyword.EXCLUDED);
      w.writeOperator(".");
      w.writeIdent(name);
    };
  }

  public static SqlGenerator plus(final SqlGenerator left, final int right) {
    return binaryExpression("+", left, right);
  }

  public static SqlGenerator plus(final SqlGenerator left, final SqlGenerator right) {
    return binaryExpression("+", left, right);
  }

  public static SqlGenerator listen(final String channel) {
    return w -> {
      w.writeKeyword(SqlKeyword.LISTEN);
      w.writeIdent(channel);
    };
  }

  public static SqlGenerator storageParameterEntry(final String key, final SqlGenerator value) {
    return w -> {
      w.writeStorageParameterKey(key);
      w.writeOperator("=");
      w.write(value);

    };
  }

  public static SqlGenerator alterTableProperties(final DbIdent table, final ImmutableMap<String, SqlGenerator> values) {
    return w -> {

      w.writeKeyword(SqlKeyword.ALTER);
      w.writeKeyword(SqlKeyword.TABLE);
      w.writeIdent(table);
      w.writeKeyword(SqlKeyword.SET);

      w.writeExprList(values.entrySet().stream().map(e -> storageParameterEntry(e.getKey(), e.getValue())));

    };
  }

  public static SqlGenerator sum(final DbIdent field) {

    return w -> {

      w.writeKeyword(SqlKeyword.SELECT);
      w.writeKeyword(SqlKeyword.SUM);
      w.writeStartExpr();
      w.writeIdent(field.getSimpleName());
      w.writeEndExpr();
      w.writeKeyword(SqlKeyword.FROM);
      w.write(field.withoutLast());

    };

  }

}
