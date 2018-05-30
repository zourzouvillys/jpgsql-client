package io.zrz.jpgsql.client.opj;

import java.util.Optional;

import io.zrz.jpgsql.client.ErrorResult;
import io.zrz.jpgsql.client.Query;
import io.zrz.jpgsql.client.SimpleQuery;
import lombok.Getter;

public class PostgresQueryException extends RuntimeException {

  @Getter
  private ErrorResult serverError;
  private Query query;

  PostgresQueryException(final Query query) {
    this.query = query;
  }

  public Optional<ErrorResult> getErrorResult() {
    return Optional.ofNullable(serverError);
  }

  @Override
  public String getMessage() {

    final StringBuilder sb = new StringBuilder();

    if (getErrorResult().isPresent()) {

      sb.append(getErrorResult().get().toString());

    }
    else if (getCause() != null) {

      sb.append(getCause().getMessage());

    }

    getAssociatedQuery().ifPresent(q -> {

      sb.append("\n").append(q);

    });

    return sb.toString();
  }

  public Optional<SimpleQuery> getAssociatedQuery() {
    if (serverError != null && serverError.statementId() != -1) {
      return Optional.ofNullable(query.statement(serverError.statementId()));
    }
    return Optional.empty();
  }

  public void setErrorResult(final ErrorResult err) {
    this.serverError = err;
  }

}
