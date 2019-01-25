/*
 ************************************************************************************
 * Copyright (C) 2001-2018 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.platform.oracle;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.IndexColumn;
import org.apache.ddlutils.model.Reference;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.ModelLoaderBase;
import org.apache.ddlutils.platform.RowFiller;
import org.apache.ddlutils.util.ExtTypes;
import org.openbravo.ddlutils.util.DBSMContants;

/**
 * 
 * @author adrian
 */
public class OracleModelLoader extends ModelLoaderBase {

  protected PreparedStatement _stmt_comments_tables;

  private static final String VIRTUAL_COLUMN_PREFIX = "SYS_NC";

  /** Creates a new instance of BasicModelLoader */
  public OracleModelLoader() {
  }

  @Override
  protected String readName() {
    return "Oracle server";
  }

  @Override
  protected void initMetadataSentences() throws SQLException {
    String sql;
    boolean firstExpressionInWhereClause = true;
    String escapeClause = "ESCAPE '\\'";
    _stmt_listtables = _connection.prepareStatement("SELECT TABLE_NAME FROM USER_TABLES "
        + _filter.getExcludeFilterWhereClause("TABLE_NAME", _filter.getExcludedTables(),
            firstExpressionInWhereClause, escapeClause)
        + " ORDER BY TABLE_NAME");
    _stmt_pkname = _connection.prepareStatement(
        "SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P' AND TABLE_NAME = ?");
    _stmt_pkname_prefix = _connection.prepareStatement(
        "SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P' AND TABLE_NAME = ? AND (upper(CONSTRAINT_NAME) LIKE 'EM_"
            + _prefix
            + "\\_%' ESCAPE '\\' OR (upper(CONSTRAINT_NAME)||UPPER(TABLE_NAME) IN (SELECT upper(NAME1)||UPPER(NAME2) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
            + _moduleId + "')))");
    _stmt_pkname_noprefix = _connection.prepareStatement(
        "SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P' AND TABLE_NAME = ? AND upper(CONSTRAINT_NAME) NOT LIKE 'EM\\_%' ESCAPE '\\'");
    _stmt_listcolumns = _connection.prepareStatement(
        "SELECT COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, DATA_LENGTH ,DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ? ORDER BY COLUMN_ID");
    _stmt_listcolumns_prefix = _connection.prepareStatement(
        "SELECT COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, DATA_LENGTH ,DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ? AND (upper(COLUMN_NAME) LIKE 'EM_"
            + _prefix
            + "\\_%' ESCAPE '\\' OR (upper(COLUMN_NAME)||UPPER(TABLE_NAME) IN (SELECT upper(NAME1)||UPPER(NAME2) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
            + _moduleId + "'))) ORDER BY COLUMN_ID");
    _stmt_listcolumns_noprefix = _connection.prepareStatement(
        "SELECT COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, DATA_LENGTH ,DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ? AND upper(COLUMN_NAME) NOT LIKE 'EM\\_%' ESCAPE '\\' ORDER BY COLUMN_ID");
    _stmt_pkcolumns = _connection.prepareStatement(
        "SELECT COLUMN_NAME FROM USER_CONS_COLUMNS WHERE CONSTRAINT_NAME = ? ORDER BY POSITION");
    _stmt_listchecks = _connection.prepareStatement(
        "SELECT CONSTRAINT_NAME, SEARCH_CONDITION FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'C' AND GENERATED = 'USER NAME' AND TABLE_NAME = ? ORDER BY CONSTRAINT_NAME");
    _stmt_listchecks_prefix = _connection.prepareStatement(
        "SELECT CONSTRAINT_NAME, SEARCH_CONDITION FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'C' AND GENERATED = 'USER NAME' AND TABLE_NAME = ? AND (upper(CONSTRAINT_NAME) LIKE 'EM_"
            + _prefix
            + "\\_%' ESCAPE '\\' OR (upper(CONSTRAINT_NAME)||UPPER(TABLE_NAME) IN (SELECT upper(NAME1)||UPPER(NAME2) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
            + _moduleId + "'))) ORDER BY CONSTRAINT_NAME");
    _stmt_listchecks_noprefix = _connection.prepareStatement(
        "SELECT CONSTRAINT_NAME, SEARCH_CONDITION FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'C' AND GENERATED = 'USER NAME' AND TABLE_NAME = ? AND upper(CONSTRAINT_NAME) NOT LIKE 'EM\\_%' ESCAPE '\\' ORDER BY CONSTRAINT_NAME");

    sql = "SELECT C.CONSTRAINT_NAME, C2.TABLE_NAME, C.DELETE_RULE, 'NO ACTION', C.R_CONSTRAINT_NAME FROM USER_CONSTRAINTS C, USER_CONSTRAINTS C2 WHERE C.R_CONSTRAINT_NAME = C2.CONSTRAINT_NAME AND C.CONSTRAINT_TYPE = 'R' AND C.TABLE_NAME = ?";
    _stmt_listfks = _connection.prepareStatement(sql + " ORDER BY C.CONSTRAINT_NAME");
    _stmt_listfks_prefix = _connection.prepareStatement(sql
        + " AND (upper(C.CONSTRAINT_NAME) LIKE 'EM_" + _prefix
        + "\\_%' ESCAPE '\\' OR (upper(C.CONSTRAINT_NAME)||UPPER(C2.TABLE_NAME) IN (SELECT upper(NAME1)||UPPER(NAME2) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
        + _moduleId + "'))) ORDER BY C.CONSTRAINT_NAME");
    _stmt_listfks_noprefix = _connection.prepareStatement(sql
        + " AND upper(C.CONSTRAINT_NAME) NOT LIKE 'EM\\_%' ESCAPE '\\' ORDER BY C.CONSTRAINT_NAME");

    _stmt_fkcolumns = _connection.prepareStatement(
        "SELECT C.COLUMN_NAME, C2.COLUMN_NAME FROM USER_CONS_COLUMNS C, USER_CONS_COLUMNS C2 WHERE C.CONSTRAINT_NAME = ? and C2.CONSTRAINT_NAME = ? and c.position = c2.position ORDER BY C.POSITION");

    _stmt_listindexes = _connection.prepareStatement(
        "SELECT U.INDEX_NAME, U.UNIQUENESS, U.TABLE_OWNER FROM USER_INDEXES U WHERE U.TABLE_NAME = ? AND (U.INDEX_TYPE = 'NORMAL' OR U.INDEX_TYPE = 'FUNCTION-BASED NORMAL') AND NOT EXISTS (SELECT 1 FROM USER_CONSTRAINTS WHERE TABLE_NAME = U.TABLE_NAME AND INDEX_NAME = U.INDEX_NAME AND CONSTRAINT_TYPE IN ('U', 'P')) ORDER BY INDEX_NAME");
    _stmt_listindexes_prefix = _connection.prepareStatement(
        "SELECT U.INDEX_NAME, U.UNIQUENESS, U.TABLE_OWNER FROM USER_INDEXES U WHERE U.TABLE_NAME = ? AND (U.INDEX_TYPE = 'NORMAL' OR U.INDEX_TYPE = 'FUNCTION-BASED NORMAL') AND NOT EXISTS (SELECT 1 FROM USER_CONSTRAINTS WHERE TABLE_NAME = U.TABLE_NAME AND INDEX_NAME = U.INDEX_NAME AND CONSTRAINT_TYPE IN ('U', 'P')) AND (upper(U.INDEX_NAME) LIKE 'EM_"
            + _prefix
            + "\\_%' ESCAPE '\\' OR (upper(U.INDEX_NAME)||UPPER(U.TABLE_NAME) IN (SELECT upper(NAME1)||UPPER(NAME2) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
            + _moduleId + "'))) ORDER BY U.INDEX_NAME");
    _stmt_listindexes_noprefix = _connection.prepareStatement(
        "SELECT U.INDEX_NAME, U.UNIQUENESS, U.TABLE_OWNER FROM USER_INDEXES U WHERE U.TABLE_NAME = ? AND (U.INDEX_TYPE = 'NORMAL' OR U.INDEX_TYPE = 'FUNCTION-BASED NORMAL') AND NOT EXISTS (SELECT 1 FROM USER_CONSTRAINTS WHERE TABLE_NAME = U.TABLE_NAME AND INDEX_NAME = U.INDEX_NAME AND CONSTRAINT_TYPE IN ('U', 'P')) AND upper(U.INDEX_NAME) NOT LIKE 'EM_%'  ORDER BY U.INDEX_NAME");
    _stmt_indexcolumns = _connection.prepareStatement(
        " SELECT column_name, column_expression FROM USER_IND_COLUMNS i left join USER_IND_EXPRESSIONS e on e.index_name = i.index_name and i.column_position = e.column_position\n"
            + " WHERE i.INDEX_NAME = ? ORDER BY i.COLUMN_POSITION");

    _stmt_listuniques = _connection.prepareStatement(
        "SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'U' AND TABLE_NAME = ? ORDER BY CONSTRAINT_NAME");
    _stmt_listuniques_prefix = _connection.prepareStatement(
        "SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'U' AND TABLE_NAME = ? AND (upper(CONSTRAINT_NAME) LIKE 'EM_"
            + _prefix
            + "\\_%' ESCAPE '\\' OR (upper(CONSTRAINT_NAME)||UPPER(TABLE_NAME) IN (SELECT upper(NAME1)||UPPER(NAME2) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
            + _moduleId + "'))) ORDER BY CONSTRAINT_NAME");
    _stmt_listuniques_noprefix = _connection.prepareStatement(
        "SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'U' AND TABLE_NAME = ? AND upper(CONSTRAINT_NAME) NOT LIKE 'EM\\_%' ESCAPE '\\' ORDER BY CONSTRAINT_NAME");
    _stmt_uniquecolumns = _connection.prepareStatement(
        "SELECT COLUMN_NAME FROM USER_CONS_COLUMNS WHERE CONSTRAINT_NAME = ? ORDER BY POSITION");
    firstExpressionInWhereClause = true;
    sql = "SELECT VIEW_NAME, TEXT FROM USER_VIEWS " + _filter.getExcludeFilterWhereClause(
        "VIEW_NAME", _filter.getExcludedViews(), firstExpressionInWhereClause, escapeClause);
    if (_prefix != null) {
      sql += " AND (UPPER(VIEW_NAME) LIKE '" + _prefix
          + "\\_%' ESCAPE '\\' OR (upper(VIEW_NAME) IN (SELECT upper(name1) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
          + _moduleId + "')))";
    }
    _stmt_listviews = _connection.prepareStatement(sql);
    firstExpressionInWhereClause = true;
    sql = "SELECT SEQUENCE_NAME, MIN_VALUE, INCREMENT_BY FROM USER_SEQUENCES "
        + _filter.getExcludeFilterWhereClause("SEQUENCE_NAME", _filter.getExcludedSequences(),
            firstExpressionInWhereClause, escapeClause);
    if (_prefix != null) {
      if (!sql.contains("WHERE")) {
        sql += " WHERE 1=1";
      }
      sql += " AND UPPER(SEQUENCE_NAME) LIKE '" + _prefix + "\\_%' ESCAPE '\\'";
    }
    _stmt_listsequences = _connection.prepareStatement(sql);
    firstExpressionInWhereClause = false;
    sql = "SELECT TRIGGER_NAME, TABLE_NAME, TRIGGER_TYPE, TRIGGERING_EVENT, TRIGGER_BODY FROM USER_TRIGGERS WHERE UPPER(TRIGGER_NAME) NOT LIKE 'AU\\_%' ESCAPE '\\' "
        + _filter.getExcludeFilterWhereClause("TRIGGER_NAME", _filter.getExcludedTriggers(),
            firstExpressionInWhereClause, escapeClause);
    if (_prefix != null) {
      if (!sql.contains("WHERE")) {
        sql += " WHERE 1=1";
      }
      sql += " AND (UPPER(TRIGGER_NAME) LIKE '" + _prefix
          + "\\_%' ESCAPE '\\' OR (upper(TRIGGER_NAME) IN (SELECT upper(name1) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
          + _moduleId + "')))";
    }
    _stmt_listtriggers = _connection.prepareStatement(sql);

    firstExpressionInWhereClause = false;
    sql = "SELECT DISTINCT NAME FROM USER_SOURCE WHERE (TYPE = 'PROCEDURE' OR TYPE = 'FUNCTION') "
        + _filter.getExcludeFilterWhereClause("NAME", _filter.getExcludedFunctions(),
            firstExpressionInWhereClause, escapeClause);
    if (_prefix != null) {
      sql += " AND (UPPER(NAME) LIKE '" + _prefix
          + "\\_%' ESCAPE '\\' OR (upper(NAME) IN (SELECT upper(name1) FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
          + _moduleId + "')))";
    }
    _stmt_listfunctions = _connection.prepareStatement(sql);

    _stmt_functioncode = _connection
        .prepareStatement("SELECT TEXT FROM USER_SOURCE WHERE NAME = ? ORDER BY LINE");
    _stmt_comments_tables = _connection.prepareStatement(
        "SELECT COMMENTS FROM USER_COL_COMMENTS WHERE TABLE_NAME= ? AND COLUMN_NAME= ?");
  }

  @Override
  protected boolean translateRequired(String required) {
    return "N".equals(required);
  }

  @Override
  protected String translateDefault(String value, int type) {

    switch (type) {
    case Types.CHAR:
    case Types.VARCHAR:
    case ExtTypes.NCHAR:
    case ExtTypes.NVARCHAR:
    case Types.LONGVARCHAR:
      if (value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
        value = value.substring(1, value.length() - 1);
        int i = 0;
        StringBuffer sunescaped = new StringBuffer();
        while (i < value.length()) {
          char c = value.charAt(i);
          if (c == '\'') {
            i++;
            if (i < value.length()) {
              sunescaped.append(c);
              i++;
            }
          } else {
            sunescaped.append(c);
            i++;
          }
        }
        if (sunescaped.length() == 0) {
          return null;
        } else {
          return sunescaped.toString();
        }
      } else {
        return value;
      }
    default:
      return value;
    }
  }

  @Override
  protected int translateColumnType(String nativeType) {

    if (nativeType == null) {
      return Types.NULL;
    } else if ("CHAR".equalsIgnoreCase(nativeType)) {
      return Types.CHAR;
    } else if ("VARCHAR2".equalsIgnoreCase(nativeType)) {
      return Types.VARCHAR;
    } else if ("NCHAR".equalsIgnoreCase(nativeType)) {
      return ExtTypes.NCHAR;
    } else if ("NVARCHAR2".equalsIgnoreCase(nativeType)) {
      return ExtTypes.NVARCHAR;
    } else if ("NUMBER".equalsIgnoreCase(nativeType)) {
      return Types.DECIMAL;
    } else if ("DATE".equalsIgnoreCase(nativeType)) {
      return Types.TIMESTAMP;
    } else if ("CLOB".equalsIgnoreCase(nativeType)) {
      return Types.CLOB;
    } else if ("BLOB".equalsIgnoreCase(nativeType)) {
      return Types.BLOB;
    } else {
      return Types.OTHER;
    }
  }

  @Override
  protected int translateParamType(String nativeType) {

    if (nativeType == null) {
      return Types.NULL;
    } else if ("CHAR".equalsIgnoreCase(nativeType)) {
      return Types.CHAR;
    } else if ("VARCHAR2".equalsIgnoreCase(nativeType)) {
      return Types.VARCHAR;
    } else if ("NCHAR".equalsIgnoreCase(nativeType)) {
      return ExtTypes.NCHAR;
    } else if ("NVARCHAR2".equalsIgnoreCase(nativeType)) {
      return ExtTypes.NVARCHAR;
    } else if ("NUMBER".equalsIgnoreCase(nativeType)) {
      return Types.NUMERIC;
    } else if ("DATE".equalsIgnoreCase(nativeType)) {
      return Types.TIMESTAMP;
    } else if ("CLOB".equalsIgnoreCase(nativeType)) {
      return Types.CLOB;
    } else if ("BLOB".equalsIgnoreCase(nativeType)) {
      return Types.BLOB;
    } else {
      return Types.VARCHAR;
    }
  }

  @Override
  protected int translateFKEvent(String fkevent) {
    if ("CASCADE".equalsIgnoreCase(fkevent)) {
      return DatabaseMetaData.importedKeyCascade;
    } else if ("SET NULL".equalsIgnoreCase(fkevent)) {
      return DatabaseMetaData.importedKeySetNull;
    } else if ("RESTRICT".equalsIgnoreCase(fkevent)) {
      return DatabaseMetaData.importedKeyRestrict;
    } else {
      return DatabaseMetaData.importedKeyNoAction;
    }
  }

  String commentCol;

  @Override
  protected Table readTable(String tablename, boolean usePrefix) throws SQLException {

    Table t = super.readTable(tablename, usePrefix);

    for (int i = 0; i < t.getColumnCount(); i++) {
      _stmt_comments_tables.setString(1, tablename);
      _stmt_comments_tables.setString(2, t.getColumn(i).getName());
      fillList(_stmt_comments_tables, new RowFiller() {
        @Override
        public void fillRow(ResultSet r) throws SQLException {
          commentCol = r.getString(1);
        }
      });
      if (commentCol != null && !commentCol.equals("")) {
        List<String> commentLines = new ArrayList<String>(Arrays.asList(commentCol.split("\\$")));
        Pattern pat3 = Pattern.compile("--OBTG:ONCREATEDEFAULT:(.*?)--");
        for (String comment : commentLines) {
          Matcher match3 = pat3.matcher(comment);
          if (match3.matches()) {
            t.getColumn(i).setOnCreateDefault(match3.group(1));
            break;
          }
        }
      }
    }

    return t;
  }

  @Override
  // Overrides readIndex to be able to discard indexes that use non supported functions
  protected Index readIndex(ResultSet rs) throws SQLException {
    String indexRealName = rs.getString(1);
    final String indexName = indexRealName.toUpperCase();

    final Index inx = new Index();

    inx.setName(indexName);
    inx.setUnique(translateUniqueness(rs.getString(2)));

    final String databaseOwner = rs.getString(3);

    _stmt_indexcolumns.setString(1, indexRealName);
    fillList(_stmt_indexcolumns, new RowFiller() {
      @Override
      public void fillRow(ResultSet r) throws SQLException {
        String columnName = r.getString(1);
        IndexColumn inxcol = null;
        if (columnName.startsWith(VIRTUAL_COLUMN_PREFIX)) {
          // The name of function base index columns needs to be translated from the name of the
          // virtual column created by Oracle to the name of the column in its table
          final String indexExpression = r.getString(2);

          inxcol = getFunctionBasedIndexColumn(indexExpression, databaseOwner);
        } else {
          inxcol = new IndexColumn();
          inxcol.setName(columnName);
        }

        String operatorClass = getIndexOperatorClass(indexName, inxcol.getName());
        if (operatorClass != null && !operatorClass.isEmpty()) {
          if (DBSMContants.CONTAINS_SEARCH.equals(operatorClass)) {
            inx.setContainsSearch(true);
          } else {
            inxcol.setOperatorClass(operatorClass);
          }
        }
        inx.addColumn(inxcol);
      }
    });

    // The index where clause will be defined only for partial indexes
    String indexWhereClause = getIndexWhereClause(indexName);
    if (indexWhereClause != null && !indexWhereClause.isEmpty()) {
      inx.setWhereClause(indexWhereClause);
    }

    return inx;
  }

  // Given an index expression, returns the name of the referenced column
  // The index expression will be like this: UPPER("COL1")
  private IndexColumn getFunctionBasedIndexColumn(String indexExpression, String databaseOwner) {
    IndexColumn indexColumn = new IndexColumn();
    indexColumn.setName("functionBasedColumn");
    String transformedExpression = removeDoubleQuotes(indexExpression);
    transformedExpression = removeDatabaseOwnerFromIndexExpression(transformedExpression,
        databaseOwner);
    indexColumn.setFunctionExpression(transformedExpression.trim());
    return indexColumn;
  }

  private String removeDatabaseOwnerFromIndexExpression(String indexExpression,
      String databaseOwner) {
    if (databaseOwner == null) {
      return indexExpression;
    }
    String dbPrefix = databaseOwner + ".";
    if (indexExpression.startsWith(dbPrefix)) {
      return indexExpression.substring(dbPrefix.length());
    } else {
      return indexExpression;
    }
  }

  /**
   * Remove the double quotes
   * 
   * @param indexExpression
   * @return
   */
  private String removeDoubleQuotes(String indexExpression) {
    return indexExpression.replace("\"", "");
  }

  /**
   * Given the name of an index and the name of one of its columns, returns the operator class that
   * is applied to that index column, if any. In Oracle, the operator classes are stored in the
   * comments of the table owner of the indexes like this:
   * "indexName1.indexColumn1.operatorClass=operatorClass1$indexName2.indexColumn2.operatorClass=operatorClass2$..."
   */
  private String getIndexOperatorClass(String indexName, String indexColumnName) {
    String operatorClass = null;
    try (PreparedStatement st = _connection
        .prepareStatement("SELECT comments FROM user_tab_comments WHERE UPPER(table_name) = ?")) {
      String tableName = getTableNameFromIndexName(indexName);
      st.setString(1, tableName.toUpperCase());
      ResultSet rs = st.executeQuery();
      String commentText = null;
      if (rs.next()) {
        commentText = rs.getString(1);
      }
      if (commentText != null && commentText.contains("$")) {
        String[] commentLines = commentText.split("\\$");
        for (String commentLine : commentLines) {
          if (commentLine.startsWith(indexName + "." + indexColumnName)) {
            operatorClass = commentLine.substring(commentLine.indexOf("=") + 1);
          } else if (commentLine.startsWith(indexName + "." + DBSMContants.CONTAINS_SEARCH)) {
            operatorClass = DBSMContants.CONTAINS_SEARCH;
          }
        }
      }
    } catch (SQLException e) {
      _log.error("Error while getting the operator class of the index column " + indexName + "."
          + indexColumnName, e);
    }
    return operatorClass;
  }

  /**
   * Given the name of an index, returns the where clause of the index, if it was defined as
   * partial. In Oracle, the where clause is stored in the comments of the first column in the index
   * like this: "indexName1.whereClause=whereClause1$indexName2.whereClause=whereClause2$..."
   */
  private String getIndexWhereClause(String indexName) {
    String whereClause = null;
    try (PreparedStatement st = _connection.prepareStatement(
        "SELECT comments FROM user_col_comments WHERE UPPER(table_name) = ? AND UPPER(column_name) = ?")) {
      String tableName = getTableNameFromIndexName(indexName);
      String columnName = getFirstColumnNameFromTableIndex(tableName, indexName);
      st.setString(1, tableName.toUpperCase());
      st.setString(2, columnName.toUpperCase());
      ResultSet rs = st.executeQuery();
      String commentText = null;
      if (rs.next()) {
        commentText = rs.getString(1);
      }
      if (commentText != null && commentText.contains("$")) {
        String[] commentLines = commentText.split("\\$");
        for (String commentLine : commentLines) {
          if (commentLine.startsWith(indexName + ".whereClause")) {
            whereClause = commentLine.substring(commentLine.indexOf("=") + 1);
          }
        }
      }
    } catch (SQLException e) {
      _log.error("Error while getting the where clause of the index " + indexName, e);
    }
    return whereClause;
  }

  /**
   * Given the name of an index, returns the name of the table it belongs to
   * 
   * @param indexName
   *          the name of the index
   * @return the name of the table it belongs to
   */
  private String getTableNameFromIndexName(String indexName) {
    String tableName = null;
    try (PreparedStatement st = _connection
        .prepareStatement("SELECT table_name FROM USER_INDEXES U WHERE INDEX_NAME = ?")) {
      st.setString(1, indexName.toUpperCase());
      ResultSet rs = st.executeQuery();
      if (rs.next()) {
        tableName = rs.getString(1);
      }
    } catch (SQLException e) {
      _log.error("Error while checking the table where the index " + indexName + " belongs", e);
    }
    return tableName;
  }

  /**
   * Given the index name and the table name it belongs, returns the first column where the index is
   * applied
   * 
   * @param tableName
   *          the name of the table
   * @param indexName
   *          the name of the index
   * @return the name of the first column where the index is applied
   */
  private String getFirstColumnNameFromTableIndex(String tableName, String indexName) {
    String columnName = null;
    try (PreparedStatement st = _connection.prepareStatement(
        "SELECT column_name FROM USER_IND_COLUMNS U WHERE INDEX_NAME = ? AND TABLE_NAME = ? AND COLUMN_POSITION = 1")) {
      st.setString(1, indexName.toUpperCase());
      st.setString(2, tableName.toUpperCase());
      ResultSet rs = st.executeQuery();
      if (rs.next()) {
        columnName = rs.getString(1);
      }
    } catch (SQLException e) {
      _log.error("Error while checking the first column where the index " + indexName
          + "of the table" + tableName + " is applied", e);
    }
    return columnName;
  }

  /*
   * Overloaded version for oracle as first sql does return one more value which needs to be passed
   * to the 2nd sql reading the columns (done this way to improve performance (issue 17796)
   */
  @Override
  protected ForeignKey readForeignKey(ResultSet rs) throws SQLException {
    String fkRealName = rs.getString(1);
    String fkName = fkRealName.toUpperCase();

    final ForeignKey fk = new ForeignKey();

    fk.setName(fkName);
    fk.setForeignTableName(rs.getString(2));
    fk.setOnDeleteCode(translateFKEvent(rs.getString(3)));
    fk.setOnUpdateCode(translateFKEvent(rs.getString(4)));

    String r_fkName = rs.getString(5);

    _stmt_fkcolumns.setString(1, fkRealName);
    _stmt_fkcolumns.setString(2, r_fkName);
    fillList(_stmt_fkcolumns, new RowFiller() {
      @Override
      public void fillRow(ResultSet r) throws SQLException {
        Reference ref = new Reference();
        ref.setLocalColumnName(r.getString(1));
        ref.setForeignColumnName(r.getString(2));
        fk.addReference(ref);
      }
    });

    return fk;
  }
}
