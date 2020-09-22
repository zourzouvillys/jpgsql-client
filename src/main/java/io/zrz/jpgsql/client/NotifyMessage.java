// Generated by delombok at Tue Sep 22 10:54:18 PDT 2020
package io.zrz.jpgsql.client;

import org.postgresql.PGNotification;

public class NotifyMessage {
  private final PGNotification msg;

  public NotifyMessage(PGNotification notifications) {
    this.msg = notifications;
  }

  public String channel() {
    return msg.getName();
  }

  public String parameter() {
    return msg.getParameter();
  }

  public int pid() {
    return msg.getPID();
  }

  @java.lang.Override
  @java.lang.SuppressWarnings("all")
  public java.lang.String toString() {
    return "NotifyMessage(msg=" + this.getMsg() + ")";
  }

  @java.lang.SuppressWarnings("all")
  public PGNotification getMsg() {
    return this.msg;
  }
}
