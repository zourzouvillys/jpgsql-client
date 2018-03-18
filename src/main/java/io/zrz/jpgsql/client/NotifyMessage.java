package io.zrz.jpgsql.client;

import org.postgresql.PGNotification;

import lombok.Getter;
import lombok.ToString;

@ToString
public class NotifyMessage {

  @Getter
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

}
