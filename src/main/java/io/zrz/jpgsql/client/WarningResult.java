package io.zrz.jpgsql.client;

import org.postgresql.util.ServerErrorMessage;

public final class WarningResult implements QueryResult {

  private final int statementId;
  private final ServerErrorMessage servermsg;

  public WarningResult(final int statementId, final ServerErrorMessage servermsg) {
    this.statementId = statementId;
    this.servermsg = servermsg;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    if (this.getDetail() != null) {
      sb.append("detail=").append(this.serverErrorMessage().getDetail()).append(" ");
    }
    if (this.getWhere() != null) {
      sb.append("where=").append(this.serverErrorMessage().getWhere()).append(" ");
    }
    if (this.getSchema() != null) {
      sb.append("schema=").append(this.serverErrorMessage().getSchema()).append(" ");
    }
    if (this.getTable() != null) {
      sb.append("table=").append(this.serverErrorMessage().getTable()).append(" ");
    }
    if (this.getColumn() != null) {
      sb.append("column=").append(this.serverErrorMessage().getColumn()).append(" ");
    }
    if (this.getDatatype() != null) {
      sb.append("datatype=").append(this.serverErrorMessage().getDatatype()).append(" ");
    }
    if (this.getConstraint() != null) {
      sb.append("constraint=").append(this.serverErrorMessage().getConstraint()).append(" ");
    }
    if (this.getInternalQuery() != null) {
      sb.append("internalQuery=").append(this.serverErrorMessage().getInternalQuery()).append(" ");
    }
    if (this.getInternalPosition() != 0) {
      sb.append("internalPosition=").append(this.serverErrorMessage().getInternalPosition()).append(" ");
    }
    if (this.getPosition() != 0) {
      sb.append("position=").append(this.serverErrorMessage().getPosition()).append(" ");
    }
    if (this.getFile() != null) {
      sb.append("file=").append(this.serverErrorMessage().getFile()).append(" ");
    }
    if (this.getLine() != 0) {
      sb.append("line=").append(this.serverErrorMessage().getLine()).append(" ");
    }
    if (this.getHint() != null) {
      sb.append("hint=").append(this.serverErrorMessage().getHint()).append(" ");
    }

    if (this.getRoutine() != null) {
      sb.append("routine=").append(this.serverErrorMessage().getRoutine()).append(" ");
    }
    if (this.getSeverity() != null) {
      sb.append("severity=").append(this.serverErrorMessage().getSeverity()).append(" ");
    }
    if (this.getSQLState() != null) {
      sb.append("sqlstate=").append(this.serverErrorMessage().getSQLState()).append(" ");
    }
    if (this.getMessage() != null) {
      sb.append("message=").append(this.serverErrorMessage().getMessage()).append(" ");
    }
    return sb.toString();
  }

  @Override
  public int statementId() {
    return this.statementId;
  }

  public ServerErrorMessage serverErrorMessage() {
    return this.servermsg;
  }

  @Override
  public QueryResultKind getKind() {
    return QueryResultKind.WARNING;
  }

  public String getDetail() {
    return this.serverErrorMessage().getDetail();
  }

  public String getWhere() {
    return this.serverErrorMessage().getWhere();
  }

  public String getSchema() {
    return this.serverErrorMessage().getSchema();
  }

  public String getTable() {
    return this.serverErrorMessage().getTable();
  }

  public String getColumn() {
    return this.serverErrorMessage().getColumn();
  }

  public String getDatatype() {
    return this.serverErrorMessage().getDatatype();
  }

  public String getConstraint() {
    return this.serverErrorMessage().getConstraint();
  }

  public int getLine() {
    return this.serverErrorMessage().getLine();
  }

  public String getInternalQuery() {
    return this.serverErrorMessage().getInternalQuery();
  }

  public int getInternalPosition() {
    return this.serverErrorMessage().getInternalPosition();
  }

  public String getFile() {
    return this.serverErrorMessage().getFile();
  }

  public String getHint() {
    return this.serverErrorMessage().getHint();
  }

  public String getMessage() {
    return this.serverErrorMessage().getMessage();
  }

  public int getPosition() {
    return this.serverErrorMessage().getPosition();
  }

  public String getRoutine() {
    return this.serverErrorMessage().getRoutine();
  }

  public String getSeverity() {
    return this.serverErrorMessage().getSeverity();
  }

  public String getSQLState() {
    return this.serverErrorMessage().getSQLState();
  }

}
