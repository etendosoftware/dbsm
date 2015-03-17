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

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.Map;

import org.apache.ddlutils.DdlUtilsException;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.alteration.ColumnSizeChange;
import org.apache.ddlutils.alteration.RemovePrimaryKeyChange;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.TypeMap;
import org.apache.ddlutils.model.Unique;
import org.apache.ddlutils.model.ValueObject;
import org.apache.ddlutils.model.View;
import org.apache.ddlutils.platform.SqlBuilder;
import org.apache.ddlutils.util.Jdbc3Utils;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;

/**
 * The SQL Builder for Oracle.
 * 
 * @version $Revision: 517050 $
 */
public class Oracle8Builder extends SqlBuilder {
  /** The regular expression pattern for ISO dates, i.e. 'YYYY-MM-DD'. */
  private Pattern _isoDatePattern;
  /** The regular expression pattern for ISO times, i.e. 'HH:MI:SS'. */
  private Pattern _isoTimePattern;
  /**
   * The regular expression pattern for ISO timestamps, i.e. 'YYYY-MM-DD HH:MI:SS.fffffffff'.
   */
  private Pattern _isoTimestampPattern;

  /**
   * Creates a new builder instance.
   * 
   * @param platform
   *          The plaftform this builder belongs to
   */
  public Oracle8Builder(Platform platform) {
    super(platform);
    addEscapedCharSequence("'", "''");

    PatternCompiler compiler = new Perl5Compiler();

    try {
      _isoDatePattern = compiler.compile("\\d{4}\\-\\d{2}\\-\\d{2}");
      _isoTimePattern = compiler.compile("\\d{2}:\\d{2}:\\d{2}");
      _isoTimestampPattern = compiler
          .compile("\\d{4}\\-\\d{2}\\-\\d{2} \\d{2}:\\d{2}:\\d{2}[\\.\\d{1,8}]?");
    } catch (MalformedPatternException ex) {
      throw new DdlUtilsException(ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createTable(Database database, Table table, Map parameters) throws IOException {
    // lets create any sequences
    Column[] columns = table.getAutoIncrementColumns();

    for (int idx = 0; idx < columns.length; idx++) {
      createAutoIncrementSequence(table, columns[idx]);
    }

    super.createTable(database, table, parameters);

    writeTableCommentsStmt(database, table);
    for (int idx = 0; idx < columns.length; idx++) {
      createAutoIncrementTrigger(table, columns[idx]);
    }

  }

  @Override
  public void writeTableCommentsStmt(Database database, Table table) throws IOException {
    // Create comments for onCreateDefault
    for (int idx = 0; idx < table.getColumnCount(); idx++) {
      Column column = table.getColumn(idx);
      writeColumnCommentStmt(database, table, column);
    }
  }

  @Override
  public void writeColumnCommentStmt(Database database, Table table, Column column)
      throws IOException {
    String comment = "";
    if (column.getOnCreateDefault() != null && !column.getOnCreateDefault().equals("")) {
      String oncreatedefaultp = column.getOnCreateDefault();
      String oncreatedefault = "";
      int lengthoncreate = oncreatedefaultp.length();
      // Parse oncreatedefault
      for (int i = 0; i < lengthoncreate; i++) {
        String tchar = oncreatedefaultp.substring(0, 1);
        oncreatedefaultp = oncreatedefaultp.substring(1);
        if (tchar.equals("'"))
          oncreatedefault += "''";
        else
          oncreatedefault += tchar;
      }
      comment += "--OBTG:ONCREATEDEFAULT:" + oncreatedefault + "--";
    }
    println("COMMENT ON COLUMN " + table.getName() + "." + column.getName() + " IS '" + comment
        + "'");
    printEndOfStatement();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void dropTable(Table table) throws IOException {
    Column[] columns = table.getAutoIncrementColumns();

    for (int idx = 0; idx < columns.length; idx++) {
      dropAutoIncrementTrigger(table, columns[idx]);
      dropAutoIncrementSequence(table, columns[idx]);
    }

    printStartOfStatement("TABLE", getStructureObjectName(table));
    print("DROP TABLE ");
    printIdentifier(getStructureObjectName(table));
    print(" CASCADE CONSTRAINTS");
    printEndOfStatement(getStructureObjectName(table));

  }

  /**
   * Creates the sequence necessary for the auto-increment of the given column.
   * 
   * @param table
   *          The table
   * @param column
   *          The column
   */
  protected void createAutoIncrementSequence(Table table, Column column) throws IOException {
    print("CREATE SEQUENCE ");
    printIdentifier(getConstraintName("seq", table, column.getName(), null));
    printEndOfStatement();
  }

  /**
   * Creates the trigger necessary for the auto-increment of the given column.
   * 
   * @param table
   *          The table
   * @param column
   *          The column
   */
  protected void createAutoIncrementTrigger(Table table, Column column) throws IOException {
    String columnName = getColumnName(column);
    String triggerName = getConstraintName("trg", table, column.getName(), null);

    if (getPlatform().isScriptModeOn()) {
      // For the script, we output a more nicely formatted version
      print("CREATE OR REPLACE TRIGGER ");
      printlnIdentifier(triggerName);
      print("BEFORE INSERT ON ");
      printlnIdentifier(getStructureObjectName(table));
      print("FOR EACH ROW WHEN (new.");
      printIdentifier(columnName);
      println(" IS NULL)");
      println("BEGIN");
      print("  SELECT ");
      printIdentifier(getConstraintName("seq", table, column.getName(), null));
      print(".nextval INTO :new.");
      printIdentifier(columnName);
      print(" FROM dual");
      println(getPlatformInfo().getSqlCommandDelimiter());
      print("END");
      println(getPlatformInfo().getSqlCommandDelimiter());
      println("/");
      println();
    } else {
      // note that the BEGIN ... SELECT ... END; is all in one line and
      // does
      // not contain a semicolon except for the END-one
      // this way, the tokenizer will not split the statement before the
      // END
      print("CREATE OR REPLACE TRIGGER ");
      printIdentifier(triggerName);
      print(" BEFORE INSERT ON ");
      printIdentifier(getStructureObjectName(table));
      print(" FOR EACH ROW WHEN (new.");
      printIdentifier(columnName);
      println(" IS NULL)");
      print("BEGIN SELECT ");
      printIdentifier(getConstraintName("seq", table, column.getName(), null));
      print(".nextval INTO :new.");
      printIdentifier(columnName);
      print(" FROM dual");
      print(getPlatformInfo().getSqlCommandDelimiter());
      print(" END");
      // It is important that there is a semicolon at the end of the
      // statement (or more
      // precisely, at the end of the PL/SQL block), and thus we put two
      // semicolons here
      // because the tokenizer will remove the one at the end
      print(getPlatformInfo().getSqlCommandDelimiter());
      printEndOfStatement();
    }
  }

  /**
   * Drops the sequence used for the auto-increment of the given column.
   * 
   * @param table
   *          The table
   * @param column
   *          The column
   */
  protected void dropAutoIncrementSequence(Table table, Column column) throws IOException {
    print("DROP SEQUENCE ");
    printIdentifier(getConstraintName("seq", table, column.getName(), null));
    printEndOfStatement();
  }

  /**
   * Drops the trigger used for the auto-increment of the given column.
   * 
   * @param table
   *          The table
   * @param column
   *          The column
   */
  protected void dropAutoIncrementTrigger(Table table, Column column) throws IOException {
    print("DROP TRIGGER ");
    printIdentifier(getConstraintName("trg", table, column.getName(), null));
    printEndOfStatement();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void createTemporaryTable(Database database, Table table, Map parameters)
      throws IOException {
    createTable(database, table, parameters);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void dropTemporaryTable(Database database, Table table) throws IOException {
    dropTable(table);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void dropExternalForeignKeys(Table table) throws IOException {
    // no need to as we drop the table with CASCASE CONSTRAINTS
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeExternalIndexDropStmt(Table table, Index index) throws IOException {
    // Index names in Oracle are unique to a schema and hence Oracle does
    // not
    // use the ON <tablename> clause
    print("DROP INDEX ");
    printIdentifier(getConstraintObjectName(index));
    printEndOfStatement();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getDefaultValue(Object defaultValue, int typeCode) throws IOException {
    if (defaultValue == null) {
      return null;
    } else {
      String defaultValueStr = defaultValue.toString();
      boolean shouldUseQuotes = !TypeMap.isNumericType(typeCode)
          && !defaultValueStr.startsWith("TO_DATE(");

      if (shouldUseQuotes) {
        // characters are only escaped when within a string literal
        return getPlatformInfo().getValueQuoteToken() + escapeStringValue(defaultValueStr)
            + getPlatformInfo().getValueQuoteToken();
      } else {
        return defaultValueStr;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getNativeDefaultValue(ValueObject column) {
    if ((column.getTypeCode() == Types.BIT)
        || (Jdbc3Utils.supportsJava14JdbcTypes() && (column.getTypeCode() == Jdbc3Utils
            .determineBooleanTypeCode()))) {
      return getDefaultValueHelper().convert(column.getDefaultValue(), column.getTypeCode(),
          Types.SMALLINT).toString();
    }
    // Oracle does not accept ISO formats, so we have to convert an ISO spec
    // if we find one
    // But these are the only formats that we make sure work, every other
    // format has to be database-dependent
    // and thus the user has to ensure that it is correct
    else if (column.getTypeCode() == Types.DATE) {
      if (new Perl5Matcher().matches(column.getDefaultValue(), _isoDatePattern)) {
        return "TO_DATE('" + column.getDefaultValue() + "', 'YYYY-MM-DD')";
      }
    } else if (column.getTypeCode() == Types.TIME) {
      if (new Perl5Matcher().matches(column.getDefaultValue(), _isoTimePattern)) {
        return "TO_DATE('" + column.getDefaultValue() + "', 'HH24:MI:SS')";
      }
    } else if (column.getTypeCode() == Types.TIMESTAMP) {
      if (new Perl5Matcher().matches(column.getDefaultValue(), _isoTimestampPattern)) {
        return "TO_DATE('" + column.getDefaultValue() + "', 'YYYY-MM-DD HH24:MI:SS')";
      }
    }
    return super.getNativeDefaultValue(column);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void writeColumnAutoIncrementStmt(Table table, Column column) throws IOException {
    // we're using sequences instead
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getSelectLastIdentityValues(Table table) {
    Column[] columns = table.getAutoIncrementColumns();

    if (columns.length > 0) {
      StringBuffer result = new StringBuffer();

      result.append("SELECT ");
      for (int idx = 0; idx < columns.length; idx++) {
        if (idx > 0) {
          result.append(",");
        }
        result.append(getDelimitedIdentifier(getConstraintName("seq", table,
            columns[idx].getName(), null)));
        result.append(".currval");
      }
      result.append(" FROM dual");
      return result.toString();
    } else {
      return null;
    }
  }

  /**
   * Processes the removal of a primary key from a table.
   * 
   * @param currentModel
   *          The current database schema
   * @param desiredModel
   *          The desired database schema
   * @param change
   *          The change object
   */
  protected void processChange(Database currentModel, Database desiredModel,
      RemovePrimaryKeyChange change) throws IOException {
    print("ALTER TABLE ");
    printlnIdentifier(getStructureObjectName(change.getChangedTable()));
    printIndent();
    print("DROP PRIMARY KEY DROP INDEX");
    printEndOfStatement();
    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeExternalUniqueDropStmt(Table table, Unique unique) throws IOException {
    print("ALTER TABLE ");
    printIdentifier(getStructureObjectName(table));
    print(" DROP CONSTRAINT ");
    printIdentifier(getConstraintObjectName(unique));
    print(" DROP INDEX ");
    printEndOfStatement();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void writeForeignKeyOnUpdateOption(ForeignKey key) throws IOException {
    // Not supported by Oracle
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void writeForeignKeyOnDeleteOption(ForeignKey key) throws IOException {

    if (key.getOnDeleteCode() == DatabaseMetaData.importedKeyCascade) {
      print(" ON DELETE CASCADE");
    } else if (key.getOnDeleteCode() == DatabaseMetaData.importedKeySetNull) {
      print(" ON DELETE SET NULL");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getFunctionEndBody() {
    return ";";
  }

  @Override
  protected void writeCreateViewStatement(View view) throws IOException {

    print("CREATE OR REPLACE FORCE VIEW ");
    printIdentifier(getStructureObjectName(view));
    print(" AS ");
    print(getSQLTranslation().exec(view.getStatement()));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getNativeFunction(String neutralFunction, int typeCode) throws IOException {
    switch (typeCode) {
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
    case Types.BIGINT:
    case Types.DECIMAL:
    case Types.NUMERIC:
    case Types.REAL:
    case Types.DOUBLE:
    case Types.FLOAT:
      return neutralFunction;
    case Types.DATE:
    case Types.TIME:
    case Types.TIMESTAMP:
      if ("SYSDATE".equals(neutralFunction.toUpperCase())) {
        return "SYSDATE";
      } else {
        return neutralFunction;
      }
    case Types.BIT:
    default:
      return neutralFunction;
    }
  }

  @Override
  public void printColumnSizeChange(Database database, ColumnSizeChange change) throws IOException {
    Table table = database.findTable(change.getTablename());
    Column column = table.findColumn(change.getColumnname());
    column.setSize(Integer.toString(change.getNewSize()));
    print("ALTER TABLE " + table.getName() + " MODIFY ");
    writeColumn(table, column);
    printEndOfStatement();
  }

}
