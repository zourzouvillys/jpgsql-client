package io.zrz.jpgsql.proto.replication;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.zrz.jpgsql.proto.WalPosition;

public class ReplicationConnectionBuilder
{
  
  private static final EventLoopGroup DEFAULT_EVENT_LOOP_GROUP = new NioEventLoopGroup();

  EventLoopGroup group = DEFAULT_EVENT_LOOP_GROUP;
  String username;
  String database;
  String slotId;
  WalPosition position;
  String create;

  public ReplicationConnectionBuilder group(EventLoopGroup group)
  {
    this.group = group;
    return this;
  }

  public ReplicationConnectionBuilder username(String username)
  {
    this.username = username;
    return this;
  }

  public ReplicationConnectionBuilder database(String database)
  {
    this.database = database;
    return this;
  }

  public ReplicationConnectionBuilder slotId(String slotId)
  {
    this.slotId = slotId;
    return this;
  }

  public ReplicationConnectionBuilder create(String outputPlugin)
  {
    this.create = outputPlugin;
    return this;
  }

  public ReplicationConnectionBuilder position(WalPosition position)
  {
    this.position = position;
    return this;
  }

  public ReplicationConnection newConnection(String host, int port)
  {
    ReplicationConnection conn = new ReplicationConnection(this);
    conn.connect(host, port);
    return conn;
  }

}
