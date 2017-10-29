package io.zrz.jpgsql.client;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import io.zrz.visitors.annotations.Visitable;

/**
 * when a query is executed, a sequence of instances of this interface will be
 * delivered.
 */

@Visitable.Base(visitors = {
    @Visitable.Visitor(value = BiFunction.class),
    @Visitable.Visitor(value = Function.class),
    @Visitable.Visitor(value = Consumer.class)
})
public interface QueryResult {

  /**
   * the kind of result
   */

  QueryResultKind getKind();

  /**
   * the statement ID that is associated with this result.
   */

  int statementId();

}
