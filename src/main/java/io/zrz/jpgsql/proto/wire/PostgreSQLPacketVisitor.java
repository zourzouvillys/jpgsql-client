package io.zrz.jpgsql.proto.wire;

public interface PostgreSQLPacketVisitor<T> {

  T visitAuthenticationOk(AuthenticationOk pkt);

  T visitAuthenticationUnknown(AuthenticationUnknown pkt);

  T visitBackendKeyData(BackendKeyData data);

  T visitCommandComplete(CommandComplete cmd);

  T visitUnknownMessage(UnknownMessage unknownMessage);

  T visitCopyBothResponse(CopyBothResponse cmd);

  T visitCopyData(CopyData copyData);

  T visitCopyDone(CopyDone copyDone);

  T visitDataRow(DataRow dataRow);

  T visitErrorResponse(ErrorResponse errorResponse);

  T visitNoticeResponse(NoticeResponse noticeResponse);

  T visitParameterStatus(ParameterStatus parameterStatus);

  T visitReadyForQuery(ReadyForQuery readyForQuery);

  T visitRowDescription(RowDescription rowDescription);

  T visitStartupMessage(StartupMessage startupMessage);

  T visitQuery(Query query);

  T visitExecute(Execute execute);

  T visitSslRequest(SslRequest sslRequestMessage);

  T visitAuthenticationMD5Password(AuthenticationMD5Password authenticationMD5Password);

  T visitPasswordMessage(PasswordMessage passwordMessage);

}
