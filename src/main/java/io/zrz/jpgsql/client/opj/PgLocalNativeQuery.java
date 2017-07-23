package io.zrz.jpgsql.client.opj;

import org.postgresql.core.NativeQuery;
import org.postgresql.core.ParameterList;
import org.postgresql.core.SqlCommand;
import org.postgresql.core.SqlCommandType;

/**
 * avoid overwriting of parameters when displaying the string, as we don't use
 * JDBC form.
 *
 * @author theo
 *
 */
public class PgLocalNativeQuery extends NativeQuery {

  private static final SqlCommand TYPE_INFO = SqlCommand.createStatementTypeInfo(SqlCommandType.BLANK);

  private PgLocalNativeQuery(String nativeSql, int params, boolean multiStatement, SqlCommand dml) {
    super(nativeSql, new int[params], multiStatement, dml);
  }

  @Override
  public String toString(ParameterList parameters) {
    return this.nativeSql;
  }

  public static PgLocalNativeQuery create(String sql, int paramcount) {
    return new PgLocalNativeQuery(sql, paramcount, false, TYPE_INFO);
  }

  public static PgLocalNativeQuery create(String sql) {
    return create(sql, 0);
  }

}
