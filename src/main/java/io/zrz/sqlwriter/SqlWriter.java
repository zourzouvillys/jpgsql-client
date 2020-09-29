package io.zrz.sqlwriter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;

import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;

import io.zrz.jpgsql.client.DefaultParametersList;
import io.zrz.jpgsql.client.PostgresQueryProcessor;
import io.zrz.jpgsql.client.QueryExecutionBuilder;
import io.zrz.jpgsql.client.QueryParameters;
import io.zrz.jpgsql.client.QueryResult;
import io.zrz.jpgsql.client.SimpleQuery;
import io.zrz.jpgsql.client.opj.BinaryParamValue;

/**
 * helper to write safeish SQL.
 */

public class SqlWriter {

  private enum State {
    NONE,
    IDENT,
    KW,
    COMMA
  }

  private final StringBuilder sb;
  private State state = State.NONE;
  private final ArrayList<Object> params = new ArrayList<>();
  private final boolean inline;

  public SqlWriter() {
    this(false);
  }

  public SqlWriter(final boolean inline) {
    this.inline = inline;
    this.sb = new StringBuilder();
  }

  /**
   * write a keyword out. these are NOT escaped, so only accept the SQL keyword enum to avoid
   * accidentally using strings.
   */

  public SqlWriter writeKeyword(final SqlKeyword... keywords) {
    for (final SqlKeyword keyword : keywords) {
      this.spacing();
      this.sb.append(keyword.toString());
      this.state = State.KW;
    }
    return this;
  }

  public Collection<Object> getParameters() {
    return this.params;
  }

  /**
   * writes a quoted string out.
   *
   * prefer parameter binding where possible.
   *
   * null must not be passed in. use SqlKeyword.NULL instead for that.
   *
   */

  public SqlWriter writeQuotedString(final String value) {
    Objects.requireNonNull(value);
    this.spacing();
    if (this.inline) {
      this.sb.append("'").append(DbUtils.escape(value)).append("'");
    }
    else {
      final int pnum = this.addParam(value);
      this.sb.append("$").append(pnum).append("");
    }
    this.state = State.KW;
    return this;
  }

  public SqlWriter writeArrayLiteral(final Collection<String> values) {
    Objects.requireNonNull(values);
    this.spacing();

    if (inline) {
      writeKeyword(SqlKeyword.ARRAY);
      writeOperator("[");
      writeList(comma(), values.stream().map(val -> SqlWriters.literal(val)));
      writeOperator("]");
    }
    else {

      final int pnum = this.addParam(values.toArray(new String[0]));
      this.sb.append("$").append(pnum).append("");
      this.state = State.KW;

    }

    return this;

  }

  public SqlWriter writeByteArray(final byte[] value) {
    Objects.requireNonNull(value);
    this.spacing();
    Preconditions.checkArgument(!inline, "bytes can only be written when not inline");
    final int pnum = this.addParam(value);
    this.sb.append("$").append(pnum).append("");
    this.state = State.KW;
    return this;
  }

  private void spacing() {
    switch (this.state) {
      case COMMA:
      case IDENT:
      case KW:
        this.sb.append(" ");
        break;
      case NONE:
        break;
    }
  }

  private int addParam(final byte[] param) {
    final int idx = params.indexOf(param);
    if (idx == -1) {
      this.params.add(param);
      return this.params.size();
    }
    return idx + 1;
  }

  private int addParam(final BinaryParamValue param) {
    final int idx = params.indexOf(param);
    if (idx == -1) {
      this.params.add(param);
      return this.params.size();
    }
    return idx + 1;
  }

  public SqlWriter writeBinaryParam(final BinaryParamValue value) {
    Objects.requireNonNull(value);
    this.spacing();
    Preconditions.checkArgument(!inline, "parameter can only be written when not inline");
    final int pnum = this.addParam(value);
    this.sb.append("$").append(pnum).append("");
    this.state = State.KW;
    return this;
  }

  private int addParam(final String param) {
    final int idx = params.indexOf(param);
    if (idx == -1) {
      this.params.add(param);
      return this.params.size();
    }
    return idx + 1;
  }

  private int addParam(final String[] param) {
    final int idx = params.indexOf(param);
    if (idx == -1) {
      this.params.add(param);
      return this.params.size();
    }
    return idx + 1;
  }

  public SqlWriter writeTypename(final String type, final int dims) {
    this.spacing();
    writeIdent(type);
    for (int i = 0; i < dims; ++i) {
      this.sb.append("[]");
    }
    this.state = State.IDENT;
    return this;
  }

  public SqlWriter writeTypename(final DbIdent type, final int dims) {
    this.spacing();
    writeIdent(type);
    for (int i = 0; i < dims; ++i) {
      this.sb.append("[]");
    }
    this.state = State.IDENT;
    return this;
  }

  /**
   * write an identifier (column name, table name, etc) out.
   *
   * note that table.column is two seperate identifiers, seperated by '.'
   *
   */

  public SqlWriter writeIdent(final String... idents) {
    this.spacing();
    this.sb.append(Arrays.stream(idents).map(ident -> DbUtils.ident(ident)).collect(Collectors.joining(".")));
    this.state = State.IDENT;
    return this;
  }

  public SqlWriter writeIdent(final List<String> idents) {
    this.spacing();
    this.sb.append(idents.stream().map(ident -> DbUtils.ident(ident)).collect(Collectors.joining(".")));
    this.state = State.IDENT;
    return this;
  }

  public SqlWriter writeIdent(final DbIdent idents) {
    return writeIdent(idents.getNames());
  }

  public SqlWriter writeComma() {
    this.sb.append(",");
    this.state = State.COMMA;
    return this;
  }

  /**
   * writes a raw expression. only use if you are 100% certain it is generated safely, as this can
   * allow for injection if it is not.
   */

  public SqlWriter writeRawExpr(final String string) {
    this.spacing();
    this.sb.append("(");
    this.sb.append(string);
    this.sb.append(")");
    this.state = State.KW;
    return this;
  }

  private static final CharMatcher STORAGE_PARAM_KEY = CharMatcher.javaLetterOrDigit().or(CharMatcher.anyOf(".-_"));

  // note: package protected to avoid API consumers using. it doesn't do any escaping.

  public SqlWriter writeStorageParameterKey(final String key) {
    if (!STORAGE_PARAM_KEY.matchesAllOf(key)) {
      throw new IllegalArgumentException("invalid storage parameter key: " + key);
    }
    this.spacing();
    this.sb.append(key);
    this.state = State.KW;
    return this;
  }

  public SqlWriter writeLiteral(final int i) {
    this.spacing();
    this.sb.append(i);
    this.state = State.IDENT;
    return this;
  }

  public SqlWriter writeLiteral(final boolean i) {
    this.spacing();
    sb.append(i ? "true"
                : "false");
    this.state = State.IDENT;
    return this;
  }

  /**
   * return the SQL string.
   */

  @Override
  public String toString() {
    return this.sb.toString();
  }

  public void writeStartExpr() {
    switch (this.state) {
      case COMMA:
      case KW:
        this.sb.append(" ");
        break;
      case IDENT:
      case NONE:
        break;
    }
    this.sb.append("(");
    this.state = State.NONE;
  }

  public void writeEndExpr() {
    this.sb.append(")");
    this.state = State.KW;
  }

  public void writeOperator(final String string) {
    switch (string) {
      case "->":
      case "->>":
      case ".":
      case "::":
      case "[":
      case "]":
        this.sb.append(string);
        break;
      default:
        this.sb.append(" ").append(string).append(" ");
        break;
    }
    this.state = State.NONE;
  }

  public void addTo(final QueryExecutionBuilder b) {
    final SimpleQuery q = new SimpleQuery(this.sb.toString(), this.params.size());
    b.add(q, this.params.toArray());
  }

  public SimpleQuery createQuery() {
    return new SimpleQuery(this.sb.toString(), this.params.size());
  }

  public Tuple createTuple() {
    return Tuple.of(createQuery(), createParameters());
  }

  public QueryParameters createParameters() {
    final DefaultParametersList p = new DefaultParametersList(this.params.size());
    p.setFrom(this.params.toArray());
    return p;
  }

  public void writeTextSearchRHS(final String config, final String query) {
    this.writeOperator("@@");
    this.writeIdent("to_tsquery");
    this.writeStartExpr();
    this.writeQuotedString(config);
    this.writeComma();
    this.writeQuotedString(query);
    this.writeEndExpr();
  }

  public SqlWriter writeStar() {
    this.writeOperator("*");
    return this;
  }

  public void writeNewline(final boolean indent) {
    this.sb.append("\n");
    if (!indent) {
      this.state = State.NONE;
    }
  }

  public void writeNewline() {
    writeNewline(true);
  }

  public void writeLiteral(final BigInteger value) {
    this.spacing();
    this.sb.append(value.longValue());
    this.state = State.IDENT;
  }

  public void writeLiteral(final BigDecimal value) {
    this.spacing();
    this.sb.append(value.toString());
    this.state = State.IDENT;
  }

  public void writeLiteral(final double value) {
    this.spacing();
    this.sb.append(value);
    this.state = State.IDENT;
  }

  public void writeLiteral(final long value) {
    this.spacing();
    this.sb.append(value);
    this.state = State.IDENT;
  }

  public static SqlGenerator ident(final String... idents) {
    return (w) -> w.writeIdent(idents);
  }

  public static SqlGenerator quotedString(final String value) {
    return (w) -> w.writeQuotedString(value);
  }

  private static final SqlGenerator _comma = (w) -> w.writeComma();

  /**
   *
   */

  public static SqlGenerator comma() {
    return _comma;
  }

  /**
   *
   */

  public void write(final Consumer<SqlWriter> gen) {
    gen.accept(this);
  }

  /**
   *
   */

  @FunctionalInterface
  public interface SqlGenerator {

    void write(SqlWriter w);

    default void addTo(final QueryExecutionBuilder qb) {
      addTo(qb, false);
    }

    default String asString() {
      final SqlWriter w = new SqlWriter(true);
      w.write(this);
      return w.toString();
    }

    default Publisher<QueryResult> submitTo(final PostgresQueryProcessor pg) {
      final SqlWriter w = new SqlWriter(false);
      w.write(this);
      return w.submitTo(pg);
    }

    default void addTo(final QueryExecutionBuilder qb, final boolean forceInline) {
      final SqlWriter w = new SqlWriter(forceInline);
      w.write(this);
      w.addTo(qb);
    }

    default Tuple asTuple() {
      return asTuple(false);
    }

    default Tuple asTuple(final boolean forceInline) {
      final SqlWriter w = new SqlWriter(forceInline);
      w.write(this);
      return w.createTuple();
    }

  }

  /**
   *
   */

  public void write(final SqlGenerator... gen) {
    Arrays.stream(gen).peek(Objects::requireNonNull).forEach(x -> x.write(this));
  }

  /**
   *
   */

  public void writeStream(final Stream<SqlGenerator> stream, final SqlGenerator seperator) {

    final Iterator<SqlGenerator> it = stream.iterator();

    int i = 0;

    while (it.hasNext()) {

      final SqlGenerator item = it.next();

      if (i++ > 0) {
        seperator.write(this);
      }

      //
      item.write(this);

    }

  }

  public void writeExprList(final Stream<SqlGenerator> items) {
    writeExprList(items.collect(Collectors.toList()));
  }

  public void writeExprList(final List<SqlGenerator> items) {
    this.writeStartExpr();
    this.writeList(_comma, items);
    this.writeEndExpr();
  }

  public void writeExprList(final String... idents) {
    this.writeStartExpr();
    this.writeList(_comma, Arrays.stream(idents).map(ident -> ident(ident)));
    this.writeEndExpr();
  }

  public void writeExprList(final SqlGenerator... items) {
    this.writeExprList(Arrays.asList(items));
  }

  public int writeList(final SqlGenerator seperator, final Collection<SqlGenerator> items) {

    final Iterator<SqlGenerator> it = items.iterator();

    int i = 0;

    while (it.hasNext()) {

      if (i++ > 0) {
        seperator.write(this);
      }

      it.next().write(this);

    }

    return i;

  }

  /**
   *
   */

  public int writeList(final SqlGenerator seperator, final SqlGenerator... items) {
    return this.writeList(seperator, Arrays.asList(items));
  }

  public int writeList(final SqlGenerator seperator, final Stream<SqlGenerator> stream) {
    return this.writeList(seperator, stream.collect(Collectors.toList()));
  }

  public void writeFunction(final String func, final SqlGenerator... generators) {
    this.writeIdent(func);
    this.writeExprList(generators);
  }

  public Publisher<QueryResult> submitTo(final PostgresQueryProcessor db) {
    final QueryExecutionBuilder qb = Objects.requireNonNull(db, "missing db").executionBuilder();
    addTo(qb);
    return qb.execute();
  }

}
