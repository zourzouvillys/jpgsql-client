package io.zrz.jpgsql;

import com.google.common.escape.CharEscaper;

public class SqlEscapers {

  private static class LikeEscaper extends CharEscaper {

    @Override
    protected char[] escape(final char c) {
      switch (c) {
        case '%':
        case '_':
          return new char[] { '\\', c };
      }
      return null;
    }

  }

  private static final LikeEscaper LIKE_Escaper = new LikeEscaper();

  public static CharEscaper likeEscaper() {
    return LIKE_Escaper;
  }

}
