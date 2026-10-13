/*
 ************************************************************************************
 * Copyright (C) 2026 Etendo Software
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 ************************************************************************************
 */
package org.apache.ddlutils.platform.postgresql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** SQL tokens and relation references needed to compare and order PostgreSQL views. */
class ViewDefinition {
  private static final Set<String> CLAUSE_END = new HashSet<>(Arrays.asList("WHERE", "GROUP",
      "HAVING", "ORDER", "LIMIT", "OFFSET", "FETCH", "FOR", "WINDOW", "UNION", "EXCEPT",
      "INTERSECT"));
  private static final Set<String> RELATION_KEYWORDS = new HashSet<>(Arrays.asList("JOIN", "LEFT",
      "RIGHT", "FULL", "INNER", "OUTER", "CROSS", "NATURAL", "ON", "USING", "TABLESAMPLE"));
  private static final Set<String> TWO_CHAR_OPERATORS = new HashSet<>(
      Arrays.asList("::", "<=", ">=", "<>", "!=", "||", "->", "=>"));
  private static final Set<String> DATE_UNITS = new HashSet<>(
      Arrays.asList("'month'", "'day'", "'week'", "'quarter'"));
  private static final Pattern DOLLAR_QUOTE = Pattern.compile("\\$\\w*\\$");
  private static final String OPEN_PARENTHESIS = "(";
  private static final String LITERAL_PREFIX = "__DBSM_VIEW_LITERAL_";
  private final List<Token> tokens = new ArrayList<>();
  private final Set<Integer> redundantQualifiers = new HashSet<>();
  private final Set<String> relations = new LinkedHashSet<>();
  private final Set<String> functions = new LinkedHashSet<>();
  private final boolean caseSensitive;

  ViewDefinition(String sql, boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
    tokenize(sql);
    for (int i = 0; i + 1 < tokens.size(); i++) {
      if (tokens.get(i).isIdentifier() && tokens.get(i + 1).text.equals(OPEN_PARENTHESIS)) {
        functions.add(name(tokens.get(i)));
      }
    }
    scan(0, tokens.size(), new HashSet<>(), false, null);
  }

  Set<String> getRelations() {
    return relations;
  }

  Set<String> getFunctions() {
    return functions;
  }

  List<String> normalizedTokens() {
    List<String> result = new ArrayList<>();
    for (int i = 0; i < tokens.size(); i++) {
      if (!redundantQualifiers.contains(i) && !tokens.get(i).text.equals(";")) {
        result.add(tokens.get(i).key());
      }
    }
    return result;
  }

  static String standardize(String sql) {
    // Standardization removes casts and semicolons. Protect literal contents so comparison
    // cannot mistake a changed value (for example 'a;b' -> 'ab') for an unchanged view.
    ViewDefinition definition = new ViewDefinition(sql, false);
    StringBuilder prefix = new StringBuilder(LITERAL_PREFIX);
    while (sql.contains(prefix)) {
      prefix.append('_');
    }
    List<String> literals = new ArrayList<>();
    List<String> markers = new ArrayList<>();
    StringBuilder protectedSql = new StringBuilder();
    int copied = 0;
    for (int i = 0; i < definition.tokens.size(); i++) {
      if (definition.isProtected(i)) {
        Token token = definition.tokens.get(i);
        String quote = definition.isQuotedIdentifier(i) ? "\"" : "'";
        String marker = quote + prefix + literals.size() + "__" + quote;
        protectedSql.append(sql, copied, token.start).append(marker);
        markers.add(marker);
        literals.add(token.text);
        copied = token.end;
      }
    }
    protectedSql.append(sql.substring(copied));
    String standardized = new PostgreSQLStandarization().exec(protectedSql.toString()).trim();
    for (int i = 0; i < literals.size(); i++) {
      standardized = standardized.replace(markers.get(i), literals.get(i));
    }
    return standardized;
  }

  private boolean isProtected(int i) {
    return (isLiteral(tokens.get(i)) || isQuotedIdentifier(i)) && !hasTranslation(i);
  }

  private static boolean isLiteral(Token token) {
    return token.text.startsWith("'") || (token.text.startsWith("$") && token.text.length() > 1
        && DOLLAR_QUOTE.matcher(token.text).lookingAt());
  }

  private boolean isQuotedIdentifier(int i) {
    return tokens.get(i).quoted && (i == 0 || !tokens.get(i - 1).text.equals("::"));
  }

  /** Literals intentionally handled by the existing integer and date translations. */
  private boolean hasTranslation(int i) {
    Token token = tokens.get(i);
    boolean integerCast = token.text.matches("'[0-9-]+'") && i + 2 < tokens.size()
        && tokens.get(i + 1).text.equals("::") && tokens.get(i + 2).keyword("INTEGER");
    boolean dateUnit = i >= 2 && tokens.get(i - 2).keyword("DATE_TRUNC")
        && tokens.get(i - 1).text.equals(OPEN_PARENTHESIS)
        && DATE_UNITS.contains(token.text.toLowerCase(Locale.ROOT));
    return integerCast || dateUnit;
  }

  private String name(Token token) {
    String value = token.identifier();
    return caseSensitive ? value : value.toUpperCase(Locale.ROOT);
  }

  /** Mutable state of one {@link #scan} pass over a range of tokens. */
  private class ScanState {
    private final Set<String> ctes;
    private final int end;
    private Scope scope;
    private boolean query;
    private boolean inFrom;
    private boolean expectRelation;

    ScanState(Set<String> inheritedCtes, int end, boolean relationGroup, Scope enclosingScope) {
      this.ctes = new HashSet<>(inheritedCtes);
      this.end = end;
      this.scope = enclosingScope;
      this.inFrom = relationGroup;
      this.expectRelation = relationGroup;
    }

    void startSelect() {
      if (scope != null && query) {
        scope.normalize();
      }
      scope = new Scope();
      query = true;
      inFrom = false;
      expectRelation = false;
    }

    void addSource(Set<String> qualifiers) {
      if (scope != null) {
        scope.sources.add(qualifiers);
      }
    }
  }

  private void scan(int start, int end, Set<String> inheritedCtes, boolean relationGroup,
      Scope enclosingScope) {
    ScanState state = new ScanState(inheritedCtes, end, relationGroup, enclosingScope);
    int i = start;
    while (i < end) {
      i = scanToken(i, state);
    }
    if (state.query && state.scope != null) {
      state.scope.normalize();
    }
  }

  /** Processes the token at {@code i} and returns the index of the next token to process. */
  private int scanToken(int i, ScanState state) {
    Token token = tokens.get(i);
    if (token.keyword("WITH")) {
      return readCtes(i + 1, state.end, state.ctes);
    }
    if (token.keyword("SELECT")) {
      state.startSelect();
    }
    if (token.text.equals(OPEN_PARENTHESIS)) {
      return scanGroup(i, state);
    }
    if (state.scope != null) {
      state.scope.positions.add(i);
    }
    return scanClause(i, token, state);
  }

  private int scanGroup(int i, ScanState state) {
    int close = tokens.get(i).close;
    boolean nestedQuery = startsQuery(i + 1, close);
    scan(i + 1, close, state.ctes, state.expectRelation && !nestedQuery,
        nestedQuery ? null : state.scope);
    int last = close;
    if (state.expectRelation) {
      Set<String> qualifiers = new HashSet<>();
      last = readAlias(last, state.end, qualifiers);
      state.addSource(qualifiers);
      state.expectRelation = false;
    }
    return last + 1;
  }

  private int scanClause(int i, Token token, ScanState state) {
    int next = i + 1;
    if (state.query && token.keyword("FROM")) {
      state.inFrom = true;
      state.expectRelation = true;
    } else if (state.inFrom && token.keyword("JOIN")) {
      state.expectRelation = true;
    } else if (CLAUSE_END.contains(token.keyword())) {
      state.inFrom = false;
      state.expectRelation = false;
    } else if (state.inFrom && token.text.equals(",")) {
      state.expectRelation = true;
    } else if (state.expectRelation && token.isIdentifier() && !token.keyword("ONLY")
        && !token.keyword("LATERAL")) {
      next = scanRelation(i, state);
    }
    return next;
  }

  private int scanRelation(int start, ScanState state) {
    int i = start;
    Token relation = tokens.get(i);
    boolean qualified = false;
    while (i + 2 < state.end && tokens.get(i + 1).text.equals(".")
        && tokens.get(i + 2).isIdentifier()) {
      qualified = true;
      i += 2;
      relation = tokens.get(i);
    }
    boolean function = i + 1 < state.end && tokens.get(i + 1).text.equals(OPEN_PARENTHESIS);
    if (!function && (qualified || !state.ctes.contains(name(relation)))) {
      relations.add(name(relation));
    }
    Set<String> qualifiers = new HashSet<>();
    if (!function) {
      qualifiers.add(relation.key());
    }
    i = readAlias(i, state.end, qualifiers);
    state.addSource(qualifiers);
    state.expectRelation = false;
    return i + 1;
  }

  /**
   * Reads an optional {@code [AS] alias} following the token at {@code last}, adds the alias to
   * the qualifiers and returns the index of the last token consumed.
   */
  private int readAlias(int last, int end, Set<String> qualifiers) {
    int i = last;
    if (i + 1 < end && tokens.get(i + 1).keyword("AS")) {
      i++;
    }
    if (i + 1 < end && isAlias(tokens.get(i + 1))) {
      i++;
      qualifiers.add(tokens.get(i).key());
    }
    return i;
  }

  private boolean isAlias(Token token) {
    return token.isIdentifier() && !CLAUSE_END.contains(token.keyword())
        && !RELATION_KEYWORDS.contains(token.keyword());
  }

  /**
   * Reads the common table expressions of a WITH clause, registering their names and scanning
   * their bodies. Returns the index of the first token after the clause.
   */
  private int readCtes(int start, int end, Set<String> ctes) {
    int i = start;
    boolean recursive = i < end && tokens.get(i).keyword("RECURSIVE");
    if (recursive) {
      i++;
    }
    boolean more = true;
    while (more && i < end && tokens.get(i).isIdentifier()) {
      i = readCte(i, end, ctes, recursive);
      // A comma as the last consumed token means another expression follows.
      more = i <= tokens.size() && tokens.get(i - 1).text.equals(",");
    }
    return i;
  }

  /**
   * Reads one {@code name [(columns)] AS [NOT] [MATERIALIZED] (body)} expression and its trailing
   * comma, if any. On malformed input it stops at the offending token.
   */
  private int readCte(int start, int end, Set<String> ctes, boolean recursive) {
    String cte = name(tokens.get(start));
    int i = start + 1;
    if (i < end && tokens.get(i).text.equals(OPEN_PARENTHESIS)) {
      i = tokens.get(i).close + 1;
    }
    if (i >= end || !tokens.get(i).keyword("AS")) {
      return i;
    }
    i++;
    if (i < end && tokens.get(i).keyword("NOT")) {
      i++;
    }
    if (i < end && tokens.get(i).keyword("MATERIALIZED")) {
      i++;
    }
    if (i >= end || !tokens.get(i).text.equals(OPEN_PARENTHESIS)) {
      return i;
    }
    if (recursive) {
      ctes.add(cte);
    }
    int close = tokens.get(i).close;
    scan(i + 1, close, ctes, false, null);
    ctes.add(cte);
    i = close + 1;
    if (i < end && tokens.get(i).text.equals(",")) {
      i++;
    }
    return i;
  }

  private boolean startsQuery(int start, int end) {
    return start < end && (tokens.get(start).keyword("SELECT")
        || tokens.get(start).keyword("WITH"));
  }

  private class Scope {
    private final List<Set<String>> sources = new ArrayList<>();
    private final Set<Integer> positions = new HashSet<>();

    void normalize() {
      // pg_get_viewdef omits column qualifiers when a SELECT has a single source. Do the
      // same without changing literals, quoted identifiers, or correlated subqueries.
      if (sources.size() != 1) {
        return;
      }
      for (int i : positions) {
        if (i + 2 < tokens.size() && sources.get(0).contains(tokens.get(i).key())
            && tokens.get(i + 1).text.equals(".") && positions.contains(i + 2)
            && (tokens.get(i + 2).isIdentifier() || tokens.get(i + 2).text.equals("*"))
            && (i == 0 || !tokens.get(i - 1).text.equals("."))
            && (i + 3 == tokens.size() || !tokens.get(i + 3).text.equals(OPEN_PARENTHESIS))) {
          redundantQualifiers.add(i);
          redundantQualifiers.add(i + 1);
        }
      }
    }
  }

  private void tokenize(String sql) {
    List<Integer> parentheses = new ArrayList<>();
    int i = 0;
    while (i < sql.length()) {
      int next = skipBlank(sql, i);
      i = next == i ? readToken(sql, i, parentheses) : next;
    }
    // Invalid SQL must be reported by PostgreSQL, rather than breaking the ordering pass.
    for (int open : parentheses) {
      tokens.get(open).close = tokens.size();
    }
  }

  /** Skips one whitespace character or one comment; returns {@code i} if there is none. */
  private static int skipBlank(String sql, int i) {
    int next = i;
    if (Character.isWhitespace(sql.charAt(i))) {
      next = i + 1;
    } else if (sql.startsWith("--", i)) {
      int newline = sql.indexOf('\n', i + 2);
      next = newline < 0 ? sql.length() : newline + 1;
    } else if (sql.startsWith("/*", i)) {
      next = skipBlockComment(sql, i + 2);
    }
    return next;
  }

  /** Skips the rest of a (possibly nested) block comment that starts after its opening. */
  private static int skipBlockComment(String sql, int afterOpening) {
    int depth = 1;
    int i = afterOpening;
    while (i < sql.length() && depth > 0) {
      if (sql.startsWith("/*", i)) {
        depth++;
        i += 2;
      } else if (sql.startsWith("*/", i)) {
        depth--;
        i += 2;
      } else {
        i++;
      }
    }
    return i;
  }

  /** Adds the token starting at {@code start} and returns the index where it ends. */
  private int readToken(String sql, int start, List<Integer> parentheses) {
    char c = sql.charAt(start);
    boolean quoted = c == '"';
    boolean identifier = quoted || Character.isLetter(c) || c == '_';
    int end;
    if (c == '\'' || quoted) {
      end = readQuoted(sql, start, isEscapeString(sql, start));
    } else if (identifier || Character.isDigit(c)) {
      end = readWord(sql, start + 1);
    } else if (c == '$') {
      end = readDollarQuoted(sql, start);
    } else {
      end = readOperator(sql, start);
    }
    tokens.add(new Token(sql.substring(start, end), identifier, quoted, start, end));
    trackParenthesis(c, parentheses);
    return end;
  }

  /** Whether the quote at {@code start} opens an E'...' string, where backslash escapes. */
  private static boolean isEscapeString(String sql, int start) {
    return sql.charAt(start) == '\'' && start > 0
        && (sql.charAt(start - 1) == 'E' || sql.charAt(start - 1) == 'e');
  }

  /** Links each closing parenthesis to its opening token as soon as it is read. */
  private void trackParenthesis(char c, List<Integer> parentheses) {
    if (c == '(') {
      parentheses.add(tokens.size() - 1);
    } else if (c == ')' && !parentheses.isEmpty()) {
      tokens.get(parentheses.remove(parentheses.size() - 1)).close = tokens.size() - 1;
    }
  }

  /** Reads a quoted string or identifier; a doubled quote is an escaped quote. */
  private static int readQuoted(String sql, int start, boolean escapeString) {
    char quote = sql.charAt(start);
    int i = start + 1;
    while (i < sql.length()) {
      char current = sql.charAt(i++);
      if (current == quote) {
        if (i >= sql.length() || sql.charAt(i) != quote) {
          return i;
        }
        i++;
      } else if (current == '\\' && escapeString && i < sql.length()) {
        i++;
      }
    }
    return i;
  }

  private static int readWord(String sql, int from) {
    int i = from;
    while (i < sql.length() && (Character.isLetterOrDigit(sql.charAt(i)) || sql.charAt(i) == '_'
        || sql.charAt(i) == '$')) {
      i++;
    }
    return i;
  }

  private static int readDollarQuoted(String sql, int start) {
    int end = start + 1;
    Matcher matcher = DOLLAR_QUOTE.matcher(sql);
    matcher.region(start, sql.length());
    if (matcher.lookingAt()) {
      String delimiter = matcher.group();
      int close = sql.indexOf(delimiter, matcher.end());
      if (close >= 0) {
        end = close + delimiter.length();
      }
    }
    return end;
  }

  private static int readOperator(String sql, int start) {
    boolean twoChars = start + 2 <= sql.length()
        && TWO_CHAR_OPERATORS.contains(sql.substring(start, start + 2));
    return twoChars ? start + 2 : start + 1;
  }

  private static class Token {
    private final String text;
    private final boolean identifier;
    private final boolean quoted;
    private final int start;
    private final int end;
    private int close;

    Token(String text, boolean identifier, boolean quoted, int start, int end) {
      this.text = text;
      this.identifier = identifier;
      this.quoted = quoted;
      this.start = start;
      this.end = end;
    }

    boolean isIdentifier() {
      return identifier;
    }

    String identifier() {
      return quoted ? text.substring(1, text.length() - 1).replace("\"\"", "\"") : text;
    }

    String keyword() {
      return identifier && !quoted ? text.toUpperCase(Locale.ROOT) : "";
    }

    boolean keyword(String word) {
      return keyword().equals(word);
    }

    String key() {
      return identifier && !quoted ? "I:" + text.toUpperCase(Locale.ROOT) : "L:" + text;
    }
  }
}
