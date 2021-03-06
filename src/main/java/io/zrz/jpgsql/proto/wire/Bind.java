// Generated by delombok at Tue Sep 22 10:54:18 PDT 2020
package io.zrz.jpgsql.proto.wire;

import java.util.ArrayList;
import java.util.List;
//
//Byte1('B')
//Identifies the message as a Bind command.
//
//Int32
//Length of message contents in bytes, including self.
//
//String
//The name of the destination portal (an empty string selects the unnamed portal).
//
//String
//The name of the source prepared statement (an empty string selects the unnamed prepared statement).
//
//Int16
//The number of parameter format codes that follow (denoted C below). This can be zero to indicate that there are no parameters or that the parameters all use the default format (text); or one, in which case the specified format code is applied to all parameters; or it can equal the actual number of parameters.
//
//Int16[C]
//The parameter format codes. Each must presently be zero (text) or one (binary).
//
//Int16
//The number of parameter values that follow (possibly zero). This must match the number of parameters needed by the query.
//
//Next, the following pair of fields appear for each parameter:
//
//Int32
//The length of the parameter value, in bytes (this count does not include itself). Can be zero. As a special case, -1 indicates a NULL parameter value. No value bytes follow in the NULL case.
//
//Byten
//The value of the parameter, in the format indicated by the associated format code. n is the above length.
//
//After the last parameter, the following fields appear:
//
//Int16
//The number of result-column format codes that follow (denoted R below). This can be zero to indicate that there are no
// result columns or that the result columns should all use the default format (text); or one, in which case the
// specified format code is applied to all result columns (if any); or it can equal the actual number of result columns of the query.
//
//Int16[R]
//The result-column format codes. Each must presently be zero (text) or one (binary).
public final class Bind implements PostgreSQLPacket {
  private final String destinationPortal;
  private final String sourcePreparedStatement;
  private final List<Integer> parameterFormats = new ArrayList<>();
  private final List<Integer> parameterValues = new ArrayList<>();
  private final List<Integer> resultFormats = new ArrayList<>();

  @Override
  public <T> T apply(final PostgreSQLPacketVisitor<T> visitor) {
    return visitor.visitBind(this);
  }

  @java.lang.SuppressWarnings("all")
  public Bind(final String destinationPortal, final String sourcePreparedStatement) {
    this.destinationPortal = destinationPortal;
    this.sourcePreparedStatement = sourcePreparedStatement;
  }

  @java.lang.SuppressWarnings("all")
  public String getDestinationPortal() {
    return this.destinationPortal;
  }

  @java.lang.SuppressWarnings("all")
  public String getSourcePreparedStatement() {
    return this.sourcePreparedStatement;
  }

  @java.lang.SuppressWarnings("all")
  public List<Integer> getParameterFormats() {
    return this.parameterFormats;
  }

  @java.lang.SuppressWarnings("all")
  public List<Integer> getParameterValues() {
    return this.parameterValues;
  }

  @java.lang.SuppressWarnings("all")
  public List<Integer> getResultFormats() {
    return this.resultFormats;
  }

  @java.lang.Override
  @java.lang.SuppressWarnings("all")
  public boolean equals(final java.lang.Object o) {
    if (o == this) return true;
    if (!(o instanceof Bind)) return false;
    final Bind other = (Bind) o;
    final java.lang.Object this$destinationPortal = this.getDestinationPortal();
    final java.lang.Object other$destinationPortal = other.getDestinationPortal();
    if (this$destinationPortal == null ? other$destinationPortal != null : !this$destinationPortal.equals(other$destinationPortal)) return false;
    final java.lang.Object this$sourcePreparedStatement = this.getSourcePreparedStatement();
    final java.lang.Object other$sourcePreparedStatement = other.getSourcePreparedStatement();
    if (this$sourcePreparedStatement == null ? other$sourcePreparedStatement != null : !this$sourcePreparedStatement.equals(other$sourcePreparedStatement)) return false;
    final java.lang.Object this$parameterFormats = this.getParameterFormats();
    final java.lang.Object other$parameterFormats = other.getParameterFormats();
    if (this$parameterFormats == null ? other$parameterFormats != null : !this$parameterFormats.equals(other$parameterFormats)) return false;
    final java.lang.Object this$parameterValues = this.getParameterValues();
    final java.lang.Object other$parameterValues = other.getParameterValues();
    if (this$parameterValues == null ? other$parameterValues != null : !this$parameterValues.equals(other$parameterValues)) return false;
    final java.lang.Object this$resultFormats = this.getResultFormats();
    final java.lang.Object other$resultFormats = other.getResultFormats();
    if (this$resultFormats == null ? other$resultFormats != null : !this$resultFormats.equals(other$resultFormats)) return false;
    return true;
  }

  @java.lang.Override
  @java.lang.SuppressWarnings("all")
  public int hashCode() {
    final int PRIME = 59;
    int result = 1;
    final java.lang.Object $destinationPortal = this.getDestinationPortal();
    result = result * PRIME + ($destinationPortal == null ? 43 : $destinationPortal.hashCode());
    final java.lang.Object $sourcePreparedStatement = this.getSourcePreparedStatement();
    result = result * PRIME + ($sourcePreparedStatement == null ? 43 : $sourcePreparedStatement.hashCode());
    final java.lang.Object $parameterFormats = this.getParameterFormats();
    result = result * PRIME + ($parameterFormats == null ? 43 : $parameterFormats.hashCode());
    final java.lang.Object $parameterValues = this.getParameterValues();
    result = result * PRIME + ($parameterValues == null ? 43 : $parameterValues.hashCode());
    final java.lang.Object $resultFormats = this.getResultFormats();
    result = result * PRIME + ($resultFormats == null ? 43 : $resultFormats.hashCode());
    return result;
  }

  @java.lang.Override
  @java.lang.SuppressWarnings("all")
  public java.lang.String toString() {
    return "Bind(destinationPortal=" + this.getDestinationPortal() + ", sourcePreparedStatement=" + this.getSourcePreparedStatement() + ", parameterFormats=" + this.getParameterFormats() + ", parameterValues=" + this.getParameterValues() + ", resultFormats=" + this.getResultFormats() + ")";
  }
}
