package io.zrz.jpgsql.client;

import java.util.Arrays;
import java.util.List;

/**
 * common client API for interacting with PostgreSQL.
 *
 * This interface is shared by the org.postgresql JDBC driver (both threaded and
 * direct), as well as my netty async implementation.
 *
 * @author theo
 *
 */

public interface PostgresClient extends PostgresQueryProcessor {

  /**
   * Create a new query to be executed.
   *
   * @param sql
   *          The SQL statement to execute, without all the JDBC parsing
   *          bullshit. Uses PostgreSQL's native form for placeholders ($1, $2,
   *          etc).
   *
   * @param paramcount
   *          The number of parameters in this query. May be 0.
   *
   * @return A reference for the query which can be used to submit for
   *         processing by PostgreSQL.
   */

  Query createQuery(String sql, int paramcount);

  /**
   * An SQL statement which takes no parameters.
   *
   * @see #createQuery(String, int).
   */

  default Query createQuery(String sql) {
    return createQuery(sql, 0);
  }

  /**
   * Creates a query which will be sent as a single batch.
   *
   * @param combine
   *          The queries to batch together.
   *
   * @return A reference for the query which can be used to submit for
   *         processing by PostgreSQL.
   */

  Query createQuery(List<Query> combine);

  /**
   * @see #createQuery(List)
   */

  default Query createQuery(Query... combine) {
    return this.createQuery(Arrays.asList(combine));
  }

  /**
   * open a {@link TransactionalSession} to perform multiple request/response
   * interactions within a single transaction. only should be used for
   * transactions which span multiple round trips.
   */

  TransactionalSession open();

}
