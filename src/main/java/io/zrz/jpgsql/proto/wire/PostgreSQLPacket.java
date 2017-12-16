package io.zrz.jpgsql.proto.wire;

/**
 * The interface used by all packets.
 */

public interface PostgreSQLPacket
{

  <T> T apply(PostgreSQLPacketVisitor<T> visitor);
  
}
