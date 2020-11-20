package io.zrz.jpgsql;

public class InternalUtils {

  public static RuntimeException sneakyThrow(Throwable t) {
    if (t == null) {
      throw new NullPointerException("t");
    }
    return InternalUtils.<RuntimeException>sneakyThrow0(t);
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> T sneakyThrow0(Throwable t) throws T {
    throw (T) t;
  }

}
