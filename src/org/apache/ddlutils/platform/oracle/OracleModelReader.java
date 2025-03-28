package org.apache.ddlutils.platform.oracle;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.ddlutils.DdlUtilsException;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.TypeMap;
import org.apache.ddlutils.platform.DatabaseMetaDataWrapper;
import org.apache.ddlutils.platform.JdbcModelReader;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;

/**
 * Reads a database model from an Oracle 8 database.
 * 
 * @version $Revision: $
 */
public class OracleModelReader extends JdbcModelReader {
  /** The regular expression pattern for the Oracle conversion of ISO dates. */
  private Pattern _oracleIsoDatePattern;
  /** The regular expression pattern for the Oracle conversion of ISO times. */
  private Pattern _oracleIsoTimePattern;
  /**
   * The regular expression pattern for the Oracle conversion of ISO timestamps.
   */
  private Pattern _oracleIsoTimestampPattern;

  /**
   * Creates a new model reader for Oracle 8 databases.
   * 
   * @param platform
   *          The platform that this model reader belongs to
   */
  public OracleModelReader(Platform platform) {
    super(platform);
    setDefaultCatalogPattern(null);
    setDefaultSchemaPattern(null);
    setDefaultTablePattern("%");

    PatternCompiler compiler = new Perl5Compiler();

    try {
      _oracleIsoDatePattern = compiler.compile("TO_DATE\\('([^']*)'\\, 'YYYY\\-MM\\-DD'\\)");
      _oracleIsoTimePattern = compiler.compile("TO_DATE\\('([^']*)'\\, 'HH24:MI:SS'\\)");
      _oracleIsoTimestampPattern = compiler
          .compile("TO_DATE\\('([^']*)'\\, 'YYYY\\-MM\\-DD HH24:MI:SS'\\)");
    } catch (MalformedPatternException ex) {
      throw new DdlUtilsException(ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Table readTable(DatabaseMetaDataWrapper metaData, Map values) throws SQLException {
    String tableName = (String) values.get("TABLE_NAME");

    // system table ?
    if (tableName.indexOf('$') > 0) {
      return null;
    }

    Table table = super.readTable(metaData, values);

    if (table != null) {
      determineAutoIncrementColumns(table);
    }

    return table;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Column readColumn(DatabaseMetaDataWrapper metaData, Map values) throws SQLException {
    Column column = super.readColumn(metaData, values);

    if (column.getDefaultValue() != null) {
      // Oracle pads the default value with spaces
      column.setDefaultValue(column.getDefaultValue().trim());
    }
    if (column.getTypeCode() == Types.DECIMAL) {
      // We're back-mapping the NUMBER columns returned by Oracle
      // Note that the JDBC driver returns DECIMAL for these NUMBER
      // columns
      switch (column.getSizeAsInt()) {
        case 1:
          if (column.getScaleAsInt() == 0) {
            column.setTypeCode(Types.BIT);
          }
          break;
        case 3:
          if (column.getScaleAsInt() == 0) {
            column.setTypeCode(Types.TINYINT);
          }
          break;
        case 5:
          if (column.getScaleAsInt() == 0) {
            column.setTypeCode(Types.SMALLINT);
          }
          break;
        case 18:
          column.setTypeCode(Types.REAL);
          break;
        case 22:
          if (column.getScaleAsInt() == 0) {
            column.setTypeCode(Types.INTEGER);
          }
          break;
        case 38:
          if (column.getScaleAsInt() == 0) {
            column.setTypeCode(Types.BIGINT);
          } else {
            column.setTypeCode(Types.DOUBLE);
          }
          break;
      }
    } else if (column.getTypeCode() == Types.FLOAT) {
      // Same for REAL, FLOAT, DOUBLE PRECISION, which all back-map to
      // FLOAT but with
      // different sizes (63 for REAL, 126 for FLOAT/DOUBLE PRECISION)
      switch (column.getSizeAsInt()) {
        case 63:
          column.setTypeCode(Types.REAL);
          break;
        case 126:
          column.setTypeCode(Types.DOUBLE);
          break;
      }
    } else if ((column.getTypeCode() == Types.DATE) || (column.getTypeCode() == Types.TIMESTAMP)) {
      // Oracle has only one DATE/TIME type, so we can't know which it is
      // and thus map
      // it back to TIMESTAMP
      column.setTypeCode(Types.TIMESTAMP);

      // we also reverse the ISO-format adaptation, and adjust the default
      // value to timestamp
      if (column.getDefaultValue() != null) {
        PatternMatcher matcher = new Perl5Matcher();
        Timestamp timestamp = null;

        if (matcher.matches(column.getDefaultValue(), _oracleIsoTimestampPattern)) {
          String timestampVal = matcher.getMatch().group(1);

          timestamp = Timestamp.valueOf(timestampVal);
        } else if (matcher.matches(column.getDefaultValue(), _oracleIsoDatePattern)) {
          String dateVal = matcher.getMatch().group(1);

          timestamp = new Timestamp(Date.valueOf(dateVal).getTime());
        } else if (matcher.matches(column.getDefaultValue(), _oracleIsoTimePattern)) {
          String timeVal = matcher.getMatch().group(1);

          timestamp = new Timestamp(Time.valueOf(timeVal).getTime());
        }
        if (timestamp != null) {
          column.setDefaultValue(timestamp.toString());
        }
      }
    } else if (TypeMap.isTextType(column.getTypeCode())) {
      column.setDefaultValue(unescape(column.getDefaultValue(), "'", "''"));
    }
    return column;
  }

  /**
   * Helper method that determines the auto increment status using Firebird's system tables.
   * 
   * @param table
   *          The table
   */
  protected void determineAutoIncrementColumns(Table table) throws SQLException {
    Column[] columns = table.getColumns();

    for (int idx = 0; idx < columns.length; idx++) {
      columns[idx].setAutoIncrement(isAutoIncrement(table, columns[idx]));
    }
  }

  /**
   * Tries to determine whether the given column is an identity column.
   * 
   * @param table
   *          The table
   * @param column
   *          The column
   * @return <code>true</code> if the column is an identity column
   */
  protected boolean isAutoIncrement(Table table, Column column) throws SQLException {
    // TODO: For now, we only check whether there is a sequence & trigger as
    // generated by DdlUtils
    // But once sequence/trigger support is in place, it might be possible
    // to 'parse' the
    // trigger body (via SELECT trigger_name, trigger_body FROM
    // user_triggers) in order to
    // determine whether it fits our auto-increment definition
    PreparedStatement prepStmt = null;
    String triggerName = getPlatform().getSqlBuilder()
        .getConstraintName("trg", table, column.getName(), null);
    String seqName = getPlatform().getSqlBuilder()
        .getConstraintName("seq", table, column.getName(), null);

    if (!getPlatform().isDelimitedIdentifierModeOn()) {
      triggerName = triggerName.toUpperCase();
      seqName = seqName.toUpperCase();
    }
    try {
      prepStmt = getConnection()
          .prepareStatement("SELECT * FROM user_triggers WHERE trigger_name = ?");
      prepStmt.setString(1, triggerName);

      ResultSet resultSet = prepStmt.executeQuery();

      if (!resultSet.next()) {
        return false;
      }
      // we have a trigger, so lets check the sequence
      prepStmt.close();

      prepStmt = getConnection()
          .prepareStatement("SELECT * FROM user_sequences WHERE sequence_name = ?");
      prepStmt.setString(1, seqName);

      resultSet = prepStmt.executeQuery();
      return resultSet.next();
    } finally {
      if (prepStmt != null) {
        prepStmt.close();
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Collection readIndices(DatabaseMetaDataWrapper metaData, String tableName)
      throws SQLException {
    // Oracle has a bug in the DatabaseMetaData#getIndexInfo method which
    // fails when
    // delimited identifiers are being used
    // Therefore, we're rather accessing the user_indexes table which
    // contains the same info
    // This also allows us to filter system-generated indices which are
    // identified by either
    // having GENERATED='Y' in the query result, or by their index names
    // being equal to the
    // name of the primary key of the table

    StringBuffer query = new StringBuffer();

    query.append(
        "SELECT a.INDEX_NAME, a.INDEX_TYPE, a.UNIQUENESS, b.COLUMN_NAME, b.COLUMN_POSITION FROM USER_INDEXES a, USER_IND_COLUMNS b WHERE ");
    query.append(
        "a.TABLE_NAME=? AND a.GENERATED=? AND a.TABLE_TYPE=? AND a.TABLE_NAME=b.TABLE_NAME AND a.INDEX_NAME=b.INDEX_NAME AND ");
    query.append(
        "a.INDEX_NAME NOT IN (SELECT DISTINCT c.CONSTRAINT_NAME FROM USER_CONSTRAINTS c WHERE c.CONSTRAINT_TYPE=? AND c.TABLE_NAME=a.TABLE_NAME");
    if (metaData.getSchemaPattern() != null) {
      query.append(" AND c.OWNER LIKE ?) AND a.TABLE_OWNER LIKE ?");
    } else {
      query.append(")");
    }

    Map indices = new ListOrderedMap();
    PreparedStatement stmt = null;

    try {
      stmt = getConnection().prepareStatement(query.toString());
      stmt.setString(1,
          getPlatform().isDelimitedIdentifierModeOn() ? tableName : tableName.toUpperCase());
      stmt.setString(2, "N");
      stmt.setString(3, "TABLE");
      stmt.setString(4, "P");
      if (metaData.getSchemaPattern() != null) {
        stmt.setString(5, metaData.getSchemaPattern().toUpperCase());
        stmt.setString(6, metaData.getSchemaPattern().toUpperCase());
      }

      ResultSet rs = stmt.executeQuery();
      Map values = new HashMap();

      while (rs.next()) {
        values.put("INDEX_NAME", rs.getString(1));
        values.put("INDEX_TYPE", Short.valueOf(DatabaseMetaData.tableIndexOther));
        values.put("NON_UNIQUE",
            "UNIQUE".equalsIgnoreCase(rs.getString(3)) ? Boolean.FALSE : Boolean.TRUE);
        values.put("COLUMN_NAME", rs.getString(4));
        values.put("ORDINAL_POSITION", Short.valueOf(rs.getShort(5)));

        readIndex(metaData, values, indices);
      }
    } finally {
      if (stmt != null) {
        stmt.close();
      }
    }
    return indices.values();
  }
}
