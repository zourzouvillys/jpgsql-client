package io.zrz.sqlwriter;

import static io.zrz.sqlwriter.SqlKeyword.BY;
import static io.zrz.sqlwriter.SqlKeyword.COMMIT;
import static io.zrz.sqlwriter.SqlKeyword.CREATE;
import static io.zrz.sqlwriter.SqlKeyword.DROP;
import static io.zrz.sqlwriter.SqlKeyword.EXISTS;
import static io.zrz.sqlwriter.SqlKeyword.IF;
import static io.zrz.sqlwriter.SqlKeyword.LIKE;
import static io.zrz.sqlwriter.SqlKeyword.LIST;
import static io.zrz.sqlwriter.SqlKeyword.LOCAL;
import static io.zrz.sqlwriter.SqlKeyword.NOT;
import static io.zrz.sqlwriter.SqlKeyword.ON;
import static io.zrz.sqlwriter.SqlKeyword.PARTITION;
import static io.zrz.sqlwriter.SqlKeyword.TABLE;
import static io.zrz.sqlwriter.SqlKeyword.TEMP;
import static io.zrz.sqlwriter.SqlKeyword.UNLOGGED;
import static io.zrz.sqlwriter.SqlKeyword.WITH;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.zrz.sqlwriter.SqlWriter.SqlGenerator;
import lombok.Getter;

public class TableBuilder {

  @Getter
  private DbIdent tableName;

  @Getter
  private List<SqlGenerator> columns = new LinkedList<>();
  private List<SqlGenerator> checks = new LinkedList<>();
  private Map<String, SqlGenerator> storageParameters = new HashMap<>();

  private boolean unlogged;

  private String partitionList;

  private boolean ifNotExists;

  private boolean dropOnCommit;

  private DbIdent like;

  public TableBuilder(DbIdent tableName) {
    this.tableName = tableName;
    this.ifNotExists = true;
  }

  public TableBuilder(String ident, String... idents) {
    this(DbIdent.of(ident, idents));
  }

  public TableBuilder withColumn(SqlGenerator gen) {
    this.columns.add(gen);
    return this;
  }

  public TableBuilder addJsonbColumn(String columnName) {
    return withColumn(ColumnGenerator.withName(columnName, "jsonb").build());
  }

  public TableBuilder addTextColumn(String columnName) {
    return withColumn(ColumnGenerator.withName(columnName, "text").build());
  }

  public TableBuilder addIntColumn(String columnName) {
    return withColumn(ColumnGenerator.withName(columnName, "int").build());
  }

  public TableBuilder addBoolColumn(String columnName) {
    return withColumn(ColumnGenerator.withName(columnName, "bool").build());
  }

  public SqlGenerator build() {
    return (w) -> {

      w.writeKeyword(CREATE);

      if (this.unlogged) {
        w.writeKeyword(UNLOGGED);
      }
      else if (this.dropOnCommit) {
        w.writeKeyword(LOCAL);
        w.writeKeyword(TEMP);
      }

      w.writeKeyword(TABLE);

      if (ifNotExists) {
        w.writeKeyword(IF, NOT, EXISTS);
      }

      w.writeIdent(tableName);

      w.writeStartExpr();
      w.writeNewline();

      if (this.like != null) {
        w.writeKeyword(LIKE);
        w.writeIdent(this.like);
        if (!columns.isEmpty()) {
          w.writeComma();
        }
      }

      w.writeList(xw -> {
        xw.writeComma();
        xw.writeNewline();
      }, columns);

      if (!checks.isEmpty()) {

        w.writeComma();
        w.writeNewline();
        w.writeList(SqlWriter.comma(),
            this.checks.stream().map(check -> (SqlWriter xw) -> {

              xw.writeKeyword(SqlKeyword.CHECK);
              xw.writeStartExpr();
              xw.write(check);
              xw.writeEndExpr();

            }));

      }

      w.writeNewline();

      w.writeEndExpr();

      if (this.partitionList != null) {

        w.writeKeyword(PARTITION);
        w.writeKeyword(BY);
        w.writeKeyword(LIST);
        w.writeStartExpr();
        w.writeIdent(partitionList);
        w.writeEndExpr();

        w.writeNewline(false);

      }

      if (!this.storageParameters.isEmpty()) {

        w.writeKeyword(WITH);

        w.writeExprList(storageParameters.entrySet().stream().map(x -> {

          return xw -> {
            w.writeIdent(x.getKey());
            w.writeOperator("=");
            w.write(x.getValue());

          };

        }));

        w.writeNewline(false);

      }

      if (this.dropOnCommit) {
        w.writeKeyword(ON);
        w.writeKeyword(COMMIT);
        w.writeKeyword(DROP);
      }

    };
  }

  public TableBuilder addColumn(ColumnGenerator gb) {
    this.columns.add(gb.build());
    return this;
  }

  public TableBuilder unlogged(boolean b) {
    this.unlogged = b;
    return this;
  }

  public TableBuilder ifNotExists(boolean b) {
    this.ifNotExists = b;
    return this;
  }

  // ALTER TABLE moo ATTACH PARTITION users FOR VALUES IN ( 1 );

  public TableBuilder partitionedByList(String column) {
    this.partitionList = column;
    return this;
  }

  public TableBuilder dropOnCommit() {
    this.dropOnCommit = true;
    return this;
  }

  public TableBuilder like(DbIdent like) {
    this.like = like;
    return this;
  }

  public TableBuilder addCheck(SqlGenerator check) {
    this.checks.add(check);
    return this;
  }

  public TableBuilder storageParameter(String name, SqlGenerator value) {
    this.storageParameters.put(name, value);
    return this;
  }

  public TableBuilder storageParameter(String name, int value) {
    this.storageParameters.put(name, SqlWriters.literal(value));
    return this;
  }

  public TableBuilder storageParameter(String name, boolean value) {
    this.storageParameters.put(name, SqlWriters.literal(value));
    return this;
  }

}
