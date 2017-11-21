package io.zrz.jpgsql.client.opj;

import java.util.Map;

import org.slf4j.MDC;

public class AmbientContext {

  private static final AmbientContext EMPTY_INSTANCE = new AmbientContext();

  private Map<String, String> map;

  public AmbientContext(Map<String, String> map) {
    this.map = map;
  }

  public AmbientContext() {
    this.map = null;
  }

  public static AmbientContext capture() {
    Map<String, String> current = MDC.getCopyOfContextMap();
    if (current != null) {
      return new AmbientContext(current);
    }
    return EMPTY_INSTANCE;
  }

  public Runnable wrap(Runnable runner) {

    if (this.map == null)
      return runner;

    // otherwise, wrap.
    return () -> {
      Map<String, String> previous = MDC.getCopyOfContextMap();
      try {
        MDC.setContextMap(map);
        runner.run();
      }
      finally {
        if (previous == null) {
          MDC.clear();
        }
        else {
          MDC.setContextMap(previous);
        }
      }
    };

  }

}
