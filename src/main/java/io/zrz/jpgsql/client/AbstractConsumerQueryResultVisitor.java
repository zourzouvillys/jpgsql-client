package io.zrz.jpgsql.client;

public abstract class AbstractConsumerQueryResultVisitor implements MyNodeVisitors.ConsumerQueryResultVisitor {

  @Override
  public void acceptWarningResult(WarningResult arg0) {
  }

  @Override
  public void acceptCommandStatus(CommandStatus arg0) {
  }

  @Override
  public void acceptSecureProgress(SecureProgress arg0) {
  }

  @Override
  public void acceptRowBuffer(RowBuffer arg0) {
  }

  @Override
  public void acceptErrorResult(ErrorResult arg0) {
    throw arg0;
  }

}
