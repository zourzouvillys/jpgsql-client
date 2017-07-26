package io.zrz.jpgsql.client;

import org.postgresql.PGNotification;

import lombok.Getter;
import lombok.ToString;

@ToString
public class NotifyMessage {

  @Getter
  private final PGNotification[] notifications;

  public NotifyMessage(PGNotification[] notifications) {
    this.notifications = notifications;
  }

}
