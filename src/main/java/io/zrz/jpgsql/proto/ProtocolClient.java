package io.zrz.jpgsql.proto;

import io.reactivex.rxjava3.core.Flowable;
import io.zrz.jpgsql.proto.wire.PostgreSQLPacket;

/**
 * protocol level API which deals in low level FE packets, abstracting the details of the TCP connection.
 *
 * @author theo
 *
 */

public interface ProtocolClient {

  Flowable<PostgreSQLPacket> open(Flowable<PostgreSQLPacket> txmit);

}
