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
  private static final Pattern DOLLAR_QUOTE = Pattern.compile("\\$[a-zA-Z_0-9]*\\$");
  private final List<Token> tokens = new ArrayList<>();
  private final Set<Integer> redundantQualifiers = new HashSet<>();
  private final Set<String> relations = new LinkedHashSet<>();
  private final Set<String> functions = new LinkedHashSet<>();
  private final boolean caseSensitive;

  ViewDefinition(String sql, boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
    tokenize(sql);
    for (int i = 0; i + 1 < tokens.size(); i++) {
      if (tokens.get(i).isIdentifier() && tokens.get(i + 1).text.equals("(")) {
        functions.add(name(tokens.get(i)));
      }
    }
    scan(0, tokens.size(), new HashSet<String>(), false, null);
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
    String prefix = "__DBSM_VIEW_LITERAL_";
    while (sql.contains(prefix)) {
      prefix += "_";
    }
    List<String> literals = new ArrayList<>();
    List<String> markers = new ArrayList<>();
    StringBuilder protectedSql = new StringBuilder();
    int copied = 0;
    for (int i = 0; i < definition.tokens.size(); i++) {
      Token token = definition.tokens.get(i);
      boolean literal = token.text.startsWith("'") || (token.text.startsWith("$")
          && token.text.length() > 1 && DOLLAR_QUOTE.matcher(token.text).lookingAt());
      boolean quotedIdentifier = token.quoted && (i == 0
          || !definition.tokens.get(i - 1).text.equals("::"));
      if (!literal && !quotedIdentifier) {
        continue;
      }
      // Keep the literals intentionally handled by the existing integer/date translations.
      boolean integerCast = token.text.matches("'[0-9-]+'")
          && i + 2 < definition.tokens.size()
          && definition.tokens.get(i + 1).text.equals("::")
          && definition.tokens.get(i + 2).keyword("INTEGER");
      boolean dateUnit = i >= 2 && definition.tokens.get(i - 2).keyword("DATE_TRUNC")
          && definition.tokens.get(i - 1).text.equals("(")
          && Arrays.asList("'month'", "'day'", "'week'", "'quarter'")
              .contains(token.text.toLowerCase(Locale.ROOT));
      if (integerCast || dateUnit) {
        continue;
      }
      protectedSql.append(sql, copied, token.start);
      String quote = quotedIdentifier ? "\"" : "'";
      String marker = quote + prefix + literals.size() + "__" + quote;
      protectedSql.append(marker);
      markers.add(marker);
      literals.add(token.text);
      copied = token.end;
    }
    protectedSql.append(sql.substring(copied));
    String standardized = new PostgreSQLStandarization().exec(protectedSql.toString()).trim();
    for (int i = 0; i < literals.size(); i++) {
      standardized = standardized.replace(markers.get(i), literals.get(i));
    }
    return standardized;
  }

  private String name(Token token) {
    String value = token.identifier();
    return caseSensitive ? value : value.toUpperCase(Locale.ROOT);
  }

  private void scan(int start, int end, Set<String> inheritedCtes, boolean relationGroup,
      Scope enclosingScope) {
    Set<String> ctes = new HashSet<>(inheritedCtes);
    Scope scope = enclosingScope;
    boolean query = false;
    boolean inFrom = relationGroup;
    boolean expectRelation = relationGroup;
    for (int i = start; i < end; i++) {
      Token token = tokens.get(i);
      if (token.keyword("WITH")) {
        i = readCtes(i + 1, end, ctes) - 1;
        continue;
      }
      if (token.keyword("SELECT")) {
        if (scope != null && query) {
          scope.normalize();
        }
        scope = new Scope();
        query = true;
        inFrom = false;
        expectRelation = false;
      }
      if (token.text.equals("(")) {
        int close = token.close;
        boolean nestedQuery = startsQuery(i + 1, close);
        scan(i + 1, close, ctes, expectRelation && !nestedQuery,
            nestedQuery ? null : scope);
        i = close;
        if (expectRelation) {
          Set<String> qualifiers = new HashSet<>();
          if (i + 1 < end && tokens.get(i + 1).keyword("AS")) {
            i++;
          }
          if (i + 1 < end && isAlias(tokens.get(i + 1))) {
            qualifiers.add(tokens.get(++i).key());
          }
          if (scope != null) {
            scope.sources.add(qualifiers);
          }
          expectRelation = false;
        }
        continue;
      }
      if (scope != null) {
        scope.positions.add(i);
      }
      if (query && token.keyword("FROM")) {
        inFrom = true;
        expectRelation = true;
        continue;
      }
      if (inFrom && token.keyword("JOIN")) {
        expectRelation = true;
        continue;
      }
      if (CLAUSE_END.contains(token.keyword())) {
        inFrom = false;
        expectRelation = false;
      }
      if (inFrom && token.text.equals(",")) {
        expectRelation = true;
        continue;
      }
      if (expectRelation && (token.keyword("ONLY") || token.keyword("LATERAL"))) {
        continue;
      }
      if (expectRelation && token.isIdentifier()) {
        Token relation = token;
        boolean qualified = false;
        while (i + 2 < end && tokens.get(i + 1).text.equals(".")
            && tokens.get(i + 2).isIdentifier()) {
          qualified = true;
          i += 2;
          relation = tokens.get(i);
        }
        boolean function = i + 1 < end && tokens.get(i + 1).text.equals("(");
        if (!function && (qualified || !ctes.contains(name(relation)))) {
          relations.add(name(relation));
        }
        Set<String> qualifiers = new HashSet<>();
        if (!function) {
          qualifiers.add(relation.key());
        }
        if (i + 1 < end && tokens.get(i + 1).keyword("AS")) {
          i++;
        }
        if (i + 1 < end && isAlias(tokens.get(i + 1))) {
          qualifiers.add(tokens.get(++i).key());
        }
        if (scope != null) {
          scope.sources.add(qualifiers);
        }
        expectRelation = false;
      }
    }
    if (query && scope != null) {
      scope.normalize();
    }
  }

  private boolean isAlias(Token token) {
    return token.isIdentifier() && !CLAUSE_END.contains(token.keyword())
        && !RELATION_KEYWORDS.contains(token.keyword());
  }

  private int readCtes(int start, int end, Set<String> ctes) {
    int i = start;
    boolean recursive = i < end && tokens.get(i).keyword("RECURSIVE");
    if (recursive) {
      i++;
    }
    while (i < end && tokens.get(i).isIdentifier()) {
      String cte = name(tokens.get(i++));
      if (i < end && tokens.get(i).text.equals("(")) {
        i = tokens.get(i).close + 1;
      }
      if (i >= end || !tokens.get(i).keyword("AS")) {
        break;
      }
      i++;
      if (i < end && tokens.get(i).keyword("NOT")) {
        i++;
      }
      if (i < end && tokens.get(i).keyword("MATERIALIZED")) {
        i++;
      }
      if (i >= end || !tokens.get(i).text.equals("(")) {
        break;
      }
      if (recursive) {
        ctes.add(cte);
      }
      int close = tokens.get(i).close;
      scan(i + 1, close, ctes, false, null);
      ctes.add(cte);
      i = close + 1;
      if (i >= end || !tokens.get(i).text.equals(",")) {
        break;
      }
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
            && (i + 3 == tokens.size() || !tokens.get(i + 3).text.equals("("))) {
          redundantQualifiers.add(i);
          redundantQualifiers.add(i + 1);
        }
      }
    }
  }

  private void tokenize(String sql) {
    List<Integer> parentheses = new ArrayList<>();
    for (int i = 0; i < sql.length();) {
      char c = sql.charAt(i);
      if (Character.isWhitespace(c)) {
        i++;
        continue;
      }
      if (sql.startsWith("--", i)) {
        int newline = sql.indexOf('\n', i + 2);
        i = newline < 0 ? sql.length() : newline + 1;
        continue;
      }
      if (sql.startsWith("/*", i)) {
        int depth = 1;
        i += 2;
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
        continue;
      }
      int start = i++;
      boolean quoted = c == '"';
      boolean identifier = quoted || Character.isLetter(c) || c == '_';
      boolean escapeString = c == '\'' && start > 0
          && (sql.charAt(start - 1) == 'E' || sql.charAt(start - 1) == 'e');
      if (c == '\'' || quoted) {
        while (i < sql.length()) {
          if (sql.charAt(i++) == c) {
            if (i >= sql.length() || sql.charAt(i) != c) {
              break;
            }
            i++;
          } else if (sql.charAt(i - 1) == '\\' && escapeString && i < sql.length()) {
            i++;
          }
        }
      } else if (identifier || Character.isDigit(c)) {
        while (i < sql.length() && (Character.isLetterOrDigit(sql.charAt(i))
            || sql.charAt(i) == '_' || sql.charAt(i) == '$')) {
          i++;
        }
      } else if (c == '$') {
        Matcher matcher = DOLLAR_QUOTE.matcher(sql);
        matcher.region(start, sql.length());
        if (matcher.lookingAt()) {
          String delimiter = matcher.group();
          int close = sql.indexOf(delimiter, matcher.end());
          if (close >= 0) {
            i = close + delimiter.length();
          }
        }
      } else if (i < sql.length() && Arrays.asList("::", "<=", ">=", "<>", "!=", "||", "->",
          "=>").contains(sql.substring(start, i + 1))) {
        i++;
      }
      Token token = new Token(sql.substring(start, i), identifier, quoted, start, i);
      tokens.add(token);
      if (c == '(') {
        parentheses.add(tokens.size() - 1);
      } else if (c == ')' && !parentheses.isEmpty()) {
        tokens.get(parentheses.remove(parentheses.size() - 1)).close = tokens.size() - 1;
      }
    }
    // Invalid SQL must be reported by PostgreSQL, rather than breaking the ordering pass.
    for (int open : parentheses) {
      tokens.get(open).close = tokens.size();
    }
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
