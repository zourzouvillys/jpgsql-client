package io.zrz.jpgsql.client;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;

public final class CopyQuery implements Query {

  private String command;
  private InputStream data;

  public CopyQuery(String command, InputStream data) {
    this.command = command;
    this.data = data;
  }

  public InputStream data() {
    return this.data;
  }

  @Override
  public int parameterCount() {
    return 0;
  }

  @Override
  public List<SimpleQuery> getSubqueries() {
    return Collections.emptyList();
  }

  public String command() {
    return this.command;
  }

}
