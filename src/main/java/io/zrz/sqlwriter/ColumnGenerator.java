package io.zrz.sqlwriter;

import java.util.LinkedList;
import java.util.List;

import io.zrz.sqlwriter.SqlWriter.SqlGenerator;

public class ColumnGenerator {

  private String columnName;
  private String dataType;
  private boolean notNull = false;
  private SqlGenerator defaultValue;
  private DbIdent references;
  private int dim;
  private boolean unique;
  private Integer primaryKey;
  private List<SqlGenerator> checks = new LinkedList<>();

  public ColumnGenerator(String columnName, String dataType) {
    this.columnName = columnName;
    this.dataType = dataType;
    this.dim = 0;
  }

  public ColumnGenerator(String columnName, String dataType, int dim) {
    this.columnName = columnName;
    this.dataType = dataType;
    this.dim = dim;
  }

  public static ColumnGenerator withName(String columnName, String dataType, int dim) {
    return new ColumnGenerator(columnName, dataType, dim);
  }

  public static ColumnGenerator withName(String columnName, String dataType) {
    return withName(columnName, dataType, 0);
  }

  public static ColumnGenerator byteArrayColumn(String columnName) {
    return withName(columnName, "bytea");
  }

  public static ColumnGenerator textColumn(String columnName) {
    return withName(columnName, "text");
  }

  public static ColumnGenerator bigintColumn(String columnName) {
    return withName(columnName, "bigint");
  }

  public static ColumnGenerator intColumn(String columnName) {
    return withName(columnName, "int");
  }

  public static ColumnGenerator identColumn(String columnName) {
    return withName(columnName, "serial");
  }

  public static ColumnGenerator boolColumn(String columnName) {
    return withName(columnName, "boolean");
  }

  public static ColumnGenerator jsonbColumn(String columnName) {
    return withName(columnName, "jsonb");
  }

  public ColumnGenerator notNull() {
    return notNull(true);
  }

  public ColumnGenerator defaultValue(SqlGenerator expr) {
    this.defaultValue = expr;
    return this;
  }

  public ColumnGenerator defaultValue(boolean expr) {
    this.defaultValue = SqlWriters.literal(expr);
    return this;
  }

  public ColumnGenerator notNull(boolean isNotNull) {
    this.notNull = isNotNull;
    return this;
  }

  public SqlGenerator build() {
    return new SqlGenerator() {
      @Override
      public void write(SqlWriter w) {

        w.writeIdent(columnName);
        w.writeIdent(dataType);

        for (int i = 0; i < dim; ++i) {
          w.writeOperator("[]");
        }

        if (notNull) {
          w.writeKeyword(SqlKeyword.NOT, SqlKeyword.NULL);
        }

        if (unique) {
          w.writeKeyword(SqlKeyword.UNIQUE);
        }

        if (primaryKey != null) {
          w.writeKeyword(SqlKeyword.PRIMARY, SqlKeyword.KEY);
          if (primaryKey != 100) {
            w.writeKeyword(SqlKeyword.WITH);
            w.writeStartExpr();
            w.writeKeyword(SqlKeyword.FILLFACTOR);
            w.writeOperator("=");
            w.writeLiteral(primaryKey);
            w.writeEndExpr();
          }
        }

        if (defaultValue != null) {
          w.writeKeyword(SqlKeyword.DEFAULT);
          w.write(defaultValue);
        }

        if (references != null)
          w.write(SqlWriters.columnIdent(references));

        if (!checks.isEmpty()) {

          w.writeList(SqlWriter.comma(),
              checks.stream().map(check -> (SqlWriter xw) -> {
                xw.writeKeyword(SqlKeyword.CHECK);
                xw.writeStartExpr();
                xw.write(check);
                xw.writeEndExpr();

              }));

        }

      }

    };
  }

  public static ColumnGenerator timestampColumn(String columnName) {
    return withName(columnName, "timestamp");
  }

  public static ColumnGenerator inetColumn(String columnName) {
    return withName(columnName, "inet");
  }

  public static ColumnGenerator tsvectorColumn(String columnName) {
    return withName(columnName, "tsvector");
  }

  public static ColumnGenerator smallintColumn(String columnName) {
    return withName(columnName, "smallint");
  }

  public ColumnGenerator references(String ident, String... idents) {
    this.references = DbIdent.of(ident, idents);
    return this;
  }

  public ColumnGenerator defaultValue(int i) {
    return this.defaultValue(w -> w.writeLiteral(i));
  }

  public ColumnGenerator unique() {
    this.unique = true;
    return this;
  }

  public ColumnGenerator primaryKey(int fillFactor) {
    this.primaryKey = fillFactor;
    return this;
  }

  public ColumnGenerator check(SqlGenerator check) {
    this.checks.add(check);
    return this;
  }

}
