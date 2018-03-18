package io.zrz.sqlwriter;

import static io.zrz.sqlwriter.SqlWriters.eq;
import static io.zrz.sqlwriter.SqlWriters.ident;

import java.util.LinkedList;
import java.util.List;

import io.zrz.sqlwriter.SqlWriter.SqlGenerator;

public class EnumTypeGenerator implements SqlGenerator {

  private DbIdent enumTypeName;
  private List<String> labels = new LinkedList<>();

  public EnumTypeGenerator(DbIdent enumTypeName) {
    this.enumTypeName = enumTypeName;
  }

  @Override
  public void write(SqlWriter w) {

    // DO $$
    // BEGIN
    // IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'my_type') THEN
    // CREATE TYPE xxx AS
    // ( ... );
    // END IF;
    // END$$;

    w.writeKeyword(SqlKeyword.DO);

    w.writeQuotedString(create().asString());

  }

  private SqlGenerator create() {

    return inner -> {

      inner.writeKeyword(SqlKeyword.BEGIN);
      inner.writeKeyword(SqlKeyword.IF);
      inner.writeKeyword(SqlKeyword.NOT);
      inner.writeKeyword(SqlKeyword.EXISTS);
      inner.writeStartExpr();

      inner.write(createCheck());

      inner.writeEndExpr();

      inner.writeKeyword(SqlKeyword.THEN);

      inner.write(generateCreate());

      inner.writeOperator(";");

      inner.writeKeyword(SqlKeyword.END);
      inner.writeKeyword(SqlKeyword.IF);
      inner.writeOperator(";");

      inner.writeKeyword(SqlKeyword.END);

    };

  }

  private QueryGenerator createCheck() {
    return QueryGenerator.from(DbIdent.of("pg_catalog", "pg_type"), "t")
        .select(SqlWriters.literal(1))
        .innerJoin("n", DbIdent.of("pg_catalog", "pg_namespace"), eq(DbIdent.of("t", "typnamespace"), DbIdent.of("n", "oid")))
        .where(eq(ident("t", "typname"), this.enumTypeName.getSimpleName()))
        .where(eq(ident("n", "nspname"), this.enumTypeName.getNamespaceName()));
  }

  private SqlGenerator createLabelCheck() {

    return QueryGenerator.from(DbIdent.of("pg_catalog", "pg_enum"), "e")
        .selectIdent("enumlabel")
        .innerJoin("t", DbIdent.of("pg_catalog", "pg_type"), eq(DbIdent.of("e", "enumtypid"), DbIdent.of("t", "oid")))
        .innerJoin("n", DbIdent.of("pg_catalog", "pg_namespace"), eq(DbIdent.of("t", "typnamespace"), DbIdent.of("n", "oid")))
        .where(eq(ident("t", "typname"), "$bufferedEventType"))
        .where(eq(ident("n", "nspname"), "public"));

  }

  private SqlGenerator generateCreate() {

    return w -> {

      // CREATE TYPE "$bufferedEventType" AS ENUM ('userCreate', 'userUpdate', 'userDelete', 'connectionDelete',
      // 'tenantPurge');

      w.writeKeyword(SqlKeyword.CREATE);
      w.writeKeyword(SqlKeyword.TYPE);
      w.writeIdent(this.enumTypeName);

      w.writeKeyword(SqlKeyword.AS);
      w.writeKeyword(SqlKeyword.ENUM);

      w.writeStartExpr();
      w.writeList(SqlWriter.comma(), this.labels.stream().map(SqlWriters::literal));
      w.writeEndExpr();

    };

  }

  public EnumTypeGenerator addLabel(String label) {
    this.labels.add(label);
    return this;
  }

}
