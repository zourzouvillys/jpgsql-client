/**
 * A non JDBC interface to PostgreSQL, with multiple protocol implementations.
 *
 * The implementation uses rxjava2, although the API does not depend on it (it
 * uses CompletableFuture and reactive-streams, so the JAR can be shaded if it
 * conflicts with things.
 *
 * @author Theo Zourzouvillys
 *
 */
package io.zrz.jpgsql.client;