package io.zrz.sqlwriter;

import java.util.Arrays;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import io.zrz.sqlwriter.SqlWriter.SqlGenerator;

/**
 * used to look up reserved keywords
 */

public enum SqlKeyword implements SqlGenerator {

  SELECT,
  FROM,
  WHERE,
  AND,
  LIMIT,
  OFFSET,
  OR,
  NOT,
  BETWEEN,
  IS,
  NULL,
  TRUE,
  FALSE,
  AS,
  ORDER,
  BY,
  ASC,
  DESC,
  OVER,
  ROW_NUMBER,

  //
  TO,
  CREATE,
  TABLE,
  IF,
  EXISTS,
  ALL,
  TABLES,
  SCHEMA,
  OWNER,
  GRANT,
  REVOKE,
  INDEX,
  UNIQUE,
  NULLS,
  FIRST,
  LAST,
  TABLESPACE,
  CONCURRENTLY,

  //
  USER,
  INSERT,
  INTO,
  FOR,
  VALUES,
  INNER,
  JOIN,
  ON,
  WITH,
  LATERAL,
  LEFT,
  OUTER,
  USING,
  ANY,

  //
  DEFAULT,
  NOW,
  UPDATE,
  DELETE,
  SET,
  SHOW,
  REFERENCES,
  ALTER,
  ADD,
  COLUMN,

  UNLOGGED,
  LOGGED,

  IN,
  ARRAY,
  DROP,
  CASCADE,
  CONFLICT,
  DO,
  NOTHING,
  INSTEAD,
  RETURNING,
  CONSTRAINT,
  PRIMARY,
  KEY,
  EXCLUDED,
  PARTITION,
  ATTACH,
  DETACH,
  RANGE,
  LIST,
  COPY,
  STDIN,
  BINARY,
  GROUP,
  RENAME,
  USAGE,
  VACUUM,
  ANALYZE,
  LOCAL,
  TEMP,
  COMMIT,
  LIKE,
  VIEW,
  UNION,
  REPLACE,
  ROW,
  CURRENT_TIMESTAMP

  //
  ;

  private static final Set<String> LOOKUP = Arrays.asList(values()).stream().map(x -> x.name()).collect(ImmutableSet.toImmutableSet());

  public static boolean isKeyword(final String ident) {
    return LOOKUP.contains(ident.toUpperCase());
  }

  @Override
  public void write(SqlWriter w) {
    w.writeKeyword(this);
  }

}
