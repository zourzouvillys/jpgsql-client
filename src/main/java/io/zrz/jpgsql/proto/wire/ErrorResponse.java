// Generated by delombok at Tue Sep 22 10:54:18 PDT 2020
package io.zrz.jpgsql.proto.wire;

import java.util.List;

public final class ErrorResponse implements PostgreSQLPacket {

  private final List<String> messages;

  @Override
  public <T> T apply(PostgreSQLPacketVisitor<T> visitor) {
    return visitor.visitErrorResponse(this);
  }

  @java.lang.SuppressWarnings("all")
  public ErrorResponse(final List<String> messages) {
    this.messages = messages;
  }

  @java.lang.SuppressWarnings("all")
  public List<String> getMessages() {
    return this.messages;
  }

  @java.lang.Override
  @java.lang.SuppressWarnings("all")
  public boolean equals(final java.lang.Object o) {
    if (o == this)
      return true;
    if (!(o instanceof ErrorResponse))
      return false;
    final ErrorResponse other = (ErrorResponse) o;
    final java.lang.Object this$messages = this.getMessages();
    final java.lang.Object other$messages = other.getMessages();
    if (this$messages == null ? other$messages != null
                              : !this$messages.equals(other$messages))
      return false;
    return true;
  }

  @java.lang.Override
  @java.lang.SuppressWarnings("all")
  public int hashCode() {
    final int PRIME = 59;
    int result = 1;
    final java.lang.Object $messages = this.getMessages();
    result =
      (result * PRIME)
        + ($messages == null ? 43
                             : $messages.hashCode());
    return result;
  }

  @java.lang.Override
  @java.lang.SuppressWarnings("all")
  public java.lang.String toString() {
    return "ErrorResponse(messages=" + this.getMessages() + ")";
  }
}
