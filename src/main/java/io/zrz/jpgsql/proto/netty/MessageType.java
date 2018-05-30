package io.zrz.jpgsql.proto.netty;

public enum MessageType {

  NoticeResponse((byte) 'N'),

  ErrorResponse((byte) 'E'),

  AuthRequest((byte) 'R'),

  ParameterStatus((byte) 'S'),

  BackendKeyData((byte) 'K'),

  ReadyForQuery((byte) 'Z'),

  CommandComplete((byte) 'C'),

  DataRow((byte) 'D'),

  CopyBothResponse((byte) 'W'),

  CopyData((byte) 'd'),

  CopyDone((byte) 'c'),

  RowDescription((byte) 'T'),

  ParseComplete((byte) '1'),

  BindComplete((byte) '2'),

  CloseComplete((byte) '3'),

  ;

  private byte type;

  MessageType(final byte type) {
    this.type = type;
  }

  public static MessageType getType(final byte type) {

    for (final MessageType t : MessageType.values()) {
      if (t.type == type) {
        return t;
      }
    }

    throw new RuntimeException(String.format("Unknown message type '%s'", type));

  }

}
