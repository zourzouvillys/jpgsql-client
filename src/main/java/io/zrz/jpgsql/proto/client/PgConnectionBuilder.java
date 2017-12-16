package io.zrz.jpgsql.proto.client;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

public class PgConnectionBuilder {

  private static final EventLoopGroup DEFAULT_EVENT_LOOP_GROUP = new NioEventLoopGroup();

  EventLoopGroup group = DEFAULT_EVENT_LOOP_GROUP;
  String username;
  String database;
  String password;

  public PgConnectionBuilder group(EventLoopGroup group) {
    this.group = group;
    return this;
  }

  public PgConnectionBuilder username(String username) {
    this.username = username;
    return this;
  }

  public PgConnectionBuilder password(String password) {
    this.password = password;
    return this;
  }

  public PgConnectionBuilder database(String database) {
    this.database = database;
    return this;
  }

  public PgConnection newConnection(String host, int port) {
    PgConnection conn = new PgConnection(this);
    conn.connect(host, port);
    return conn;
  }

}
