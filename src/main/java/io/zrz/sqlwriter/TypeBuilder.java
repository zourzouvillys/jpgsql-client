package io.zrz.sqlwriter;

import static io.zrz.sqlwriter.SqlKeyword.AS;
import static io.zrz.sqlwriter.SqlKeyword.CREATE;
import static io.zrz.sqlwriter.SqlKeyword.TYPE;

import java.util.LinkedList;
import java.util.List;

import io.zrz.sqlwriter.SqlWriter.SqlGenerator;
import lombok.Getter;

public class TypeBuilder implements SqlGenerator {

  @Getter
  private DbIdent tableName;

  @Getter
  private List<SqlGenerator> columns = new LinkedList<>();

  public TypeBuilder(DbIdent tableName) {
    this.tableName = tableName;
  }

  public TypeBuilder(String ident, String... idents) {
    this(DbIdent.of(ident, idents));
  }

  public TypeBuilder withColumn(SqlGenerator gen) {
    this.columns.add(gen);
    return this;
  }

  public TypeBuilder addJsonbColumn(String columnName) {
    return withColumn(ColumnGenerator.withName(columnName, "jsonb").build());
  }

  public TypeBuilder addTextColumn(String columnName) {
    return withColumn(ColumnGenerator.withName(columnName, "text").build());
  }

  public TypeBuilder addIntColumn(String columnName) {
    return withColumn(ColumnGenerator.withName(columnName, "int").build());
  }

  public TypeBuilder addBoolColumn(String columnName) {
    return withColumn(ColumnGenerator.withName(columnName, "bool").build());
  }

  public SqlGenerator build() {
    return (w) -> {

      w.writeKeyword(CREATE);
      w.writeKeyword(TYPE);
      w.writeIdent(tableName);
      w.writeKeyword(AS);
      w.writeStartExpr();
      w.writeNewline();

      w.writeList(xw -> {
        xw.writeComma();
        xw.writeNewline();
      }, columns);

      w.writeNewline();
      w.writeEndExpr();

    };

  }

  public TypeBuilder addColumn(ColumnGenerator gb) {
    this.columns.add(gb.build());
    return this;
  }

  @Override
  public void write(SqlWriter w) {
    w.write(build());
  }

}
