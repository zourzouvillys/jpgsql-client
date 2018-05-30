package io.zrz.jpgsql.proto.wire;

import java.util.List;

import com.google.common.collect.ImmutableList;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode
@Getter
@ToString
public class Parse implements PostgreSQLPacket {

  private String name;
  private String query;
  private List<Integer> paramOids;

  public Parse(final String name, final String query) {
    this.name = name;
    this.query = query;
    this.paramOids = ImmutableList.of();
  }

  public Parse(final String name, final String query, final List<Integer> paramOids) {
    this.name = name;
    this.query = query;
    this.paramOids = ImmutableList.copyOf(paramOids);
  }

  @Override
  public <T> T apply(final PostgreSQLPacketVisitor<T> visitor) {
    return visitor.visitParse(this);
  }

}
