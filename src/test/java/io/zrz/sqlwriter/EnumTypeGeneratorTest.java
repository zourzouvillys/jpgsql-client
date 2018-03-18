package io.zrz.sqlwriter;

import org.junit.Test;

public class EnumTypeGeneratorTest {

  @Test
  public void test() {

    EnumTypeGenerator x = new EnumTypeGenerator(DbIdent.of("public", "test_enum_type"))
        .addLabel("a")
        .addLabel("b")
        .addLabel("c")
        .addLabel("d")
        .addLabel("e");

  }

}
