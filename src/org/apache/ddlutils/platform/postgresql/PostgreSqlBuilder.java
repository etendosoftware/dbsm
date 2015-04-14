package org.apache.ddlutils.platform.postgresql;

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
import java.sql.Types;
import java.util.Map;
import java.util.Vector;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.alteration.AddColumnChange;
import org.apache.ddlutils.alteration.ColumnChange;
import org.apache.ddlutils.alteration.ColumnDefaultValueChange;
import org.apache.ddlutils.alteration.ColumnRequiredChange;
import org.apache.ddlutils.alteration.ColumnSizeChange;
import org.apache.ddlutils.model.Check;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Parameter;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.Trigger;
import org.apache.ddlutils.model.View;
import org.apache.ddlutils.platform.SqlBuilder;
import org.apache.ddlutils.translation.CommentFilter;
import org.apache.ddlutils.translation.LiteralFilter;
import org.apache.ddlutils.translation.Translation;
import org.apache.ddlutils.util.ExtTypes;

/**
 * The SQL Builder for PostgresSql.
 * 
 * @version $Revision: 504014 $
 */
public class PostgreSqlBuilder extends SqlBuilder {

  private Translation plsqltranslation = null;
  private Translation sqltranslation = null;

  /**
   * Creates a new builder instance.
   * 
   * @param platform
   *          The plaftform this builder belongs to
   */
  public PostgreSqlBuilder(Platform platform) {
    super(platform);
    // we need to handle the backslash first otherwise the other
    // already escaped sequences would be affected
    addEscapedCharSequence("\\", "\\\\");
    addEscapedCharSequence("'", "\\'");
    addEscapedCharSequence("\b", "\\b");
    addEscapedCharSequence("\f", "\\f");
    addEscapedCharSequence("\n", "\\n");
    addEscapedCharSequence("\r", "\\r");
    addEscapedCharSequence("\t", "\\t");
  }

  /*
   * public void alterDatabase(Database currentModel, Database desiredModel, CreationParameters
   * params) throws IOException { super.alterDatabase(currentModel, desiredModel, params);
   * 
   * //Now we recreate the views, because they may have been //deleted during the table recreation
   * process for(int i=0;i<desiredModel.getViewCount();i++) { createView(desiredModel.getView(i)); }
   * }
   */
  /**
   * {@inheritDoc}
   */
  @Override
  public void dropTable(Table table) throws IOException {
    printStartOfStatement("TABLE", getStructureObjectName(table));
    print("DROP TABLE ");
    printIdentifier(getStructureObjectName(table));
    print(" CASCADE");
    printEndOfStatement(getStructureObjectName(table));

    Column[] columns = table.getAutoIncrementColumns();

    for (int idx = 0; idx < columns.length; idx++) {
      dropAutoIncrementSequence(table, columns[idx]);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeExternalIndexDropStmt(Table table, Index index) throws IOException {
    print("DROP INDEX ");
    printIdentifier(getConstraintObjectName(index));
    printEndOfStatement();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createTable(Database database, Table table, Map parameters) throws IOException {
    for (int idx = 0; idx < table.getColumnCount(); idx++) {
      Column column = table.getColumn(idx);

      if (column.isAutoIncrement()) {
        createAutoIncrementSequence(table, column);
      }
    }
    super.createTable(database, table, parameters);
    writeTableCommentsStmt(database, table);
    printEndOfStatement();
  }

  @Override
  public void writeTableCommentsStmt(Database database, Table table) throws IOException {

    // Add comments for NVARCHAR types

    for (int idx = 0; idx < table.getColumnCount(); idx++) {
      Column column = table.getColumn(idx);
      writeColumnCommentStmt(database, table, column);
    }
  }

  @Override
  public void writeColumnCommentStmt(Database database, Table table, Column column)
      throws IOException {
    String comment = "";

    if (column.getTypeCode() == ExtTypes.NVARCHAR) {
      comment += "--OBTG:NVARCHAR--";
    }

    if (column.getTypeCode() == ExtTypes.NCHAR) {
      comment += "--OBTG:NCHAR--";
    }
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
        + "';");
    printEndOfStatement();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getDelimitedIdentifier(String identifier) {
    if ("OFFSET".equalsIgnoreCase(identifier) || "NOW".equalsIgnoreCase(identifier)
        || "WHEN".equalsIgnoreCase(identifier)) {
      return getPlatformInfo().getDelimiterToken() + identifier
          + getPlatformInfo().getDelimiterToken();
    } else {
      return super.getDelimitedIdentifier(identifier);
    }
  }

  /**
   * Creates the auto-increment sequence that is then used in the column.
   * 
   * @param table
   *          The table
   * @param column
   *          The column
   */
  protected void createAutoIncrementSequence(Table table, Column column) throws IOException {
    print("CREATE SEQUENCE ");
    printIdentifier(getConstraintName(null, table, column.getName(), "seq"));
    printEndOfStatement();
  }

  protected void disableAllNOTNULLColumns(Table table) throws IOException {
    for (int i = 0; i < table.getColumnCount(); i++) {
      Column column = table.getColumn(i);
      if (column.isRequired() && !column.isPrimaryKey()
          && !recreatedTables.contains(table.getName())) {
        println("ALTER TABLE " + table.getName() + " ALTER " + getColumnName(column)
            + " DROP NOT NULL");
        printEndOfStatement();
      }

    }
  }

  @Override
  protected void disableTempNOTNULLColumns(Vector<AddColumnChange> newColumns) throws IOException {
    for (int i = 0; i < newColumns.size(); i++) {
      Column column = newColumns.get(i).getNewColumn();
      if (column.isRequired()) {
        println("ALTER TABLE " + newColumns.get(i).getChangedTable().getName() + "_ ALTER "
            + getColumnName(column) + " DROP NOT NULL");
        printEndOfStatement();
      }

    }
  }

  @Override
  protected void enableAllNOTNULLColumns(Table table) throws IOException {
    for (int i = 0; i < table.getColumnCount(); i++) {
      Column column = table.getColumn(i);
      if (column.isRequired()) {
        println("ALTER TABLE " + table.getName() + " ALTER " + getColumnName(column)
            + " SET NOT NULL");
        printEndOfStatement();
      }
    }
  }

  @Override
  protected void enableNOTNULLColumns(Vector<ColumnChange> newColumns) throws IOException {
    for (int i = 0; i < newColumns.size(); i++) {
      Column column = newColumns.get(i).getChangedColumn();
      if (column.isRequired() && !column.isPrimaryKey()) {
        println("ALTER TABLE " + newColumns.get(i).getChangedTable().getName() + " ALTER "
            + getColumnName(column) + " SET NOT NULL");
        printEndOfStatement();
      }

    }
  }

  /**
   * Creates the auto-increment sequence that is then used in the column.
   * 
   * @param table
   *          The table
   * @param column
   *          The column
   */
  protected void dropAutoIncrementSequence(Table table, Column column) throws IOException {
    print("DROP SEQUENCE ");
    printIdentifier(getConstraintName(null, table, column.getName(), "seq"));
    printEndOfStatement();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void writeColumnAutoIncrementStmt(Table table, Column column) throws IOException {
    print("UNIQUE DEFAULT nextval('");
    print(getConstraintName(null, table, column.getName(), "seq"));
    print("')");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getSelectLastIdentityValues(Table table) {
    Column[] columns = table.getAutoIncrementColumns();

    if (columns.length == 0) {
      return null;
    } else {
      StringBuffer result = new StringBuffer();

      result.append("SELECT ");
      for (int idx = 0; idx < columns.length; idx++) {
        if (idx > 0) {
          result.append(", ");
        }
        result.append("currval('");
        result.append(getConstraintName(null, table, columns[idx].getName(), "seq"));
        result.append("') AS ");
        result.append(getDelimitedIdentifier(columns[idx].getName()));
      }
      return result.toString();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void writeCreateFunctionStmt(Function function) throws IOException {
    print("CREATE OR REPLACE FUNCTION ");
    printIdentifier(getStructureObjectName(function));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void writeDropFunctionStmt(Function function) throws IOException {
    print("DROP FUNCTION ");
    printIdentifier(getStructureObjectName(function));

    print("(");
    for (int idx = 0; idx < function.getParameterCount(); idx++) {
      if (idx > 0) {
        print(", ");
      }
      writeParameter(function.getParameter(idx));
    }
    print(")");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getNoParametersDeclaration() {
    return "()";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getFunctionReturn(Function function) {

    if (function.getTypeCode() == Types.NULL) {
      if (isProcedure(function)) {
        return "";
      } else {
        return "RETURNS VOID";
      }
    } else {
      return "RETURNS " + getSqlType(function.getTypeCode());
    }
  }

  private boolean isProcedure(Function function) {
    for (int i = 0; i < function.getParameterCount(); i++) {
      if (function.getParameter(i).getModeCode() == Parameter.MODE_OUT) {
        return true;
      }
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getFunctionBeginBody() {
    return "AS $BODY$ DECLARE ";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getFunctionEndBody() {
    return "; $BODY$ LANGUAGE plpgsql;";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void createFunction(Function function) throws IOException {

    super.createFunction(function);
    /*
     * System.out.println("Funcion: "+function.getName()); for(int
     * i=0;i<function.getParameterCount();i++) System.out.println("    Param: "
     * +function.getParameter(i).getName()+"   default: "
     * +function.getParameter(i).getDefaultValue());
     */
    // We'll add a comment to save the NVARCHAR, VARCHAR2, and similar types
    // of data which don't have
    // a corresponding type in PostgreSQL
    String comment = "--OBTG:";
    boolean b = false;
    if (function.getTypeCode() == ExtTypes.NVARCHAR) {
      comment += function.getName() + "func=" + "NVARCHAR";
      b = true;
    }
    for (int i = 0; i < function.getParameterCount(); i++) {
      Parameter p = function.getParameter(i);
      if (p.getTypeCode() == ExtTypes.NVARCHAR) {
        if (b)
          comment += ",";
        comment += p.getName() + "=" + "NVARCHAR";
        b = true;
      }
    }
    if (!comment.equals("--OBTG:")) {
      print("COMMENT ON FUNCTION " + function.getName() + " ");

      if (function.getParameterCount() == 0) {
        print(getNoParametersDeclaration());
      } else {
        print("(");
        for (int idx = 0; idx < function.getParameterCount(); idx++) {
          if (idx > 0) {
            print(", ");
          }
          writeParameter(function.getParameter(idx));
        }
        print(")");
      }
      println(" IS '" + comment + "--';");
    }
    String sLastDefault = function.getParameterCount() == 0 ? null : getDefaultValue(function
        .getParameter(function.getParameterCount() - 1));
    if (sLastDefault != null && !sLastDefault.equals("")) {
      try {
        Function f = (Function) function.clone();
        f.removeParameter(function.getParameterCount() - 1);
        StringBuffer sBody = new StringBuffer();
        sBody.append("BEGIN\n");
        sBody.append(function.getTypeCode() == Types.NULL ? " " : "RETURN ");
        sBody.append(getStructureObjectName(function));
        sBody.append(" (");
        for (int i = 0; i < f.getParameterCount(); i++) {
          sBody.append("$");
          sBody.append(i + 1);
          sBody.append(", ");
        }
        sBody.append(sLastDefault);
        sBody.append(");\n");
        sBody.append("END");
        f.setBody(sBody.toString());
        createFunction(f);
      } catch (CloneNotSupportedException e) {
        // Will not happen
      }
    } else {
      // System.out.println("funcion "+function.getName()+" doesn't have defaults");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void dropFunction(Function function) throws IOException {

    String sLastDefault = function.getParameterCount() == 0 ? null : function.getParameter(
        function.getParameterCount() - 1).getDefaultValue();
    if (sLastDefault != null && !sLastDefault.equals("")) {
      try {
        Function f = (Function) function.clone();
        f.removeParameter(function.getParameterCount() - 1);
        dropFunction(f);
      } catch (CloneNotSupportedException e) {
        // Will not happen
      }
    }

    super.dropFunction(function);

  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void writeParameter(Parameter parameter) throws IOException {

    if (parameter.getName() != null) {
      print(parameter.getName());
      print(" ");
    }

    String mode = getParameterMode(parameter);
    if (mode != null) {
      print(mode);
      print(" ");
    }

    print(getSqlType(parameter.getTypeCode()));

    // Postgre does not support default values...
    // writeDefaultValueStmt(parameter);
  }

  @Override
  public void writeCreateTriggerFunction(Trigger trigger) throws IOException {

    printStartOfStatement("FUNCTION FOR TRIGGER", getStructureObjectName(trigger));

    print("CREATE FUNCTION ");
    printIdentifier(getStructureObjectName(trigger));
    print("()");
    println();
    print("RETURNS trigger");
    println();

    print(getFunctionBeginBody());
    println();

    String body = trigger.getBody();

    LiteralFilter litFilter = new LiteralFilter();
    CommentFilter comFilter = new CommentFilter();
    body = litFilter.removeLiterals(body);
    body = comFilter.removeComments(body);

    body = getPLSQLTriggerTranslation().exec(body);

    body = comFilter.restoreComments(body);
    body = litFilter.restoreLiterals(body);

    print(body);
    println();
    print(getFunctionEndBody());

    printEndOfStatement(getStructureObjectName(trigger));
  }

  @Override
  public void writeTriggerExecuteStmt(Trigger trigger) throws IOException {
    print("EXECUTE PROCEDURE ");
    printIdentifier(getStructureObjectName(trigger));
    print("()");
  }

  @Override
  public void disableTrigger(Database database, Trigger trigger) throws IOException {
    if (trigger.getName() == null) {
      _log.warn("Cannot write unnamed trigger " + trigger);
    } else {
      printStartOfStatement("TRIGGER", getStructureObjectName(trigger));
      print("ALTER TABLE " + trigger.getTable() + " DISABLE TRIGGER ");
      printIdentifier(getStructureObjectName(trigger));
      printEndOfStatement(getStructureObjectName(trigger));
    }
  }

  @Override
  public void enableTrigger(Database database, Trigger trigger) throws IOException {

    if (trigger.getName() == null) {
      _log.warn("Cannot write unnamed trigger " + trigger);
    } else {
      printStartOfStatement("TRIGGER", getStructureObjectName(trigger));
      print("ALTER TABLE " + trigger.getTable() + " ENABLE TRIGGER ");
      printIdentifier(getStructureObjectName(trigger));
      printEndOfStatement(getStructureObjectName(trigger));
    }
  }

  @Override
  protected void writeDropTriggerEndStatement(Database database, Trigger trigger)
      throws IOException {
    print(" ON ");
    print(getStructureObjectName(trigger.getTable()));
    print(" CASCADE");
  }

  @Override
  protected void writeDropTriggerFunction(Trigger trigger) throws IOException {

    printStartOfStatement("FUNCTION FOR TRIGGER", getStructureObjectName(trigger));

    print("DROP FUNCTION ");
    printIdentifier(getStructureObjectName(trigger));
    print("()");
    printEndOfStatement(getStructureObjectName(trigger));
  }

  protected void dropView(View view) throws IOException {

    if (getPlatformInfo().isViewsSupported()) {
      if (view.getName() == null) {
        _log.warn("Cannot write unnamed view " + view);
      } else {

        dropUpdateRules(view);

        printStartOfStatement("VIEW", getStructureObjectName(view));

        printScriptOptions("FORCE = TRUE");
        print("DROP VIEW IF EXISTS ");
        printIdentifier(getStructureObjectName(view));

        printEndOfStatement(getStructureObjectName(view));
      }
    }
  }

  @Override
  protected void writeCreateViewStatement(View view) throws IOException {

    printScriptOptions("FORCE = TRUE");
    print("CREATE OR REPLACE VIEW ");
    printIdentifier(getStructureObjectName(view));
    print(" AS ");
    print(getSQLTranslation().exec(view.getStatement()));
  }

  @Override
  protected void createUpdateRules(View view) throws IOException {

    RuleProcessor rule = new RuleProcessor(view.getStatement());

    if (rule.isUpdatable()) {

      // INSERT RULE
      print("CREATE OR REPLACE RULE ");
      printIdentifier(shortenName(view.getName() + "_INS", getMaxTableNameLength()));
      print(" AS ON INSERT TO ");
      printIdentifier(getStructureObjectName(view));
      print(" DO INSTEAD INSERT INTO ");
      printIdentifier(shortenName(rule.getViewTable(), getMaxTableNameLength()));
      print(" ( ");
      for (int i = 0; i < rule.getViewFields().size(); i++) {
        RuleProcessor.ViewField field = rule.getViewFields().get(i);
        if (i > 0) {
          print(", ");
        }
        print(field.getField());
      }
      print(" ) VALUES ( ");
      for (int i = 0; i < rule.getViewFields().size(); i++) {
        RuleProcessor.ViewField field = rule.getViewFields().get(i);
        if (i > 0) {
          print(", ");
        }
        print("NEW.");
        print(field.getFieldas());
      }
      print(")");
      printEndOfStatement(getStructureObjectName(view));

      // UPDATE RULE
      print("CREATE OR REPLACE RULE ");
      printIdentifier(shortenName(view.getName() + "_UPD", getMaxTableNameLength()));
      print(" AS ON UPDATE TO ");
      printIdentifier(getStructureObjectName(view));
      print(" DO INSTEAD UPDATE ");
      printIdentifier(shortenName(rule.getViewTable(), getMaxTableNameLength()));
      print(" SET ");

      for (int i = 0; i < rule.getViewFields().size(); i++) {
        RuleProcessor.ViewField field = rule.getViewFields().get(i);
        if (i > 0) {
          print(", ");
        }
        print(field.getField());
        print(" = NEW.");
        print(field.getFieldas());
      }
      print(" WHERE ");
      print(rule.getViewFields().get(0).getField());
      print(" = NEW.");
      print(rule.getViewFields().get(0).getFieldas());
      printEndOfStatement(getStructureObjectName(view));

      // DELETE RULE
      print("CREATE OR REPLACE RULE ");
      printIdentifier(shortenName(view.getName() + "_DEL", getMaxTableNameLength()));
      print(" AS ON DELETE TO ");
      printIdentifier(getStructureObjectName(view));
      print(" DO INSTEAD DELETE FROM ");
      printIdentifier(shortenName(rule.getViewTable(), getMaxTableNameLength()));
      print(" WHERE ");
      print(rule.getViewFields().get(0).getField());
      print(" = OLD.");
      print(rule.getViewFields().get(0).getFieldas());
      printEndOfStatement(getStructureObjectName(view));
    }
  }

  @Override
  protected void dropUpdateRules(View view) throws IOException {

    RuleProcessor rule = new RuleProcessor(view.getStatement());

    if (rule.isUpdatable()) {
      // INSERT RULE
      print("DROP RULE IF EXISTS ");
      printIdentifier(shortenName(view.getName() + "_INS", getMaxTableNameLength()));
      print(" ON ");
      printIdentifier(getStructureObjectName(view));
      printEndOfStatement(getStructureObjectName(view));

      // UPDATE RULE
      print("DROP RULE IF EXISTS ");
      printIdentifier(shortenName(view.getName() + "_UPD", getMaxTableNameLength()));
      print(" ON ");
      printIdentifier(getStructureObjectName(view));
      printEndOfStatement(getStructureObjectName(view));

      // DELETE RULE
      print("DROP RULE IF EXISTS ");
      printIdentifier(shortenName(view.getName() + "_DEL", getMaxTableNameLength()));
      print(" ON ");
      printIdentifier(getStructureObjectName(view));
      printEndOfStatement(getStructureObjectName(view));
    }
  }

  @Override
  protected Translation createPLSQLFunctionTranslation(Database database) {
    return new PostgrePLSQLFunctionTranslation(database);
  }

  @Override
  public Translation createPLSQLTriggerTranslation(Database database) {
    return new PostgrePLSQLTriggerTranslation(database);
  }

  @Override
  protected Translation createSQLTranslation(Database database) {
    return new PostgreSQLTranslation();
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
        return "now()";
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
    print("ALTER TABLE " + table.getName() + " ALTER COLUMN " + column.getName() + " TYPE ");
    print(getSqlType(column));
    printEndOfStatement();
  }

  public void executeOnCreateDefault(Table table, Table tempTable, Column col, boolean recreated)
      throws IOException {
    String pk = "";
    Column[] pks1 = table.getPrimaryKeyColumns();
    for (int i = 0; i < pks1.length; i++) {
      if (i > 0)
        pk += " AND ";
      pk += table.getName() + "." + pks1[i].getName() + "::text=" + tempTable.getName() + "."
          + pks1[i].getName() + "::text";
    }
    String oncreatedefault = col.getOnCreateDefault();
    if (oncreatedefault != null && !oncreatedefault.equals("")) {
      if (recreated)
        println("UPDATE " + table.getName() + " SET " + col.getName() + "=(" + oncreatedefault
            + ") WHERE EXISTS (SELECT 1 FROM " + tempTable.getName() + " WHERE " + pk + ") AND "
            + col.getName() + " IS NULL");
      else
        println("UPDATE " + table.getName() + " SET " + col.getName() + "=(" + oncreatedefault
            + ")");
      printEndOfStatement();
    }
  }

  protected void writeCastExpression(Column sourceColumn, Column targetColumn) throws IOException {
    if (sourceColumn.isOfTextType() && targetColumn.isOfNumericType()) {
      print("TO_NUMBER(");
      printIdentifier(getColumnName(sourceColumn));
      print(")");
    } else if (sourceColumn.getTypeCode() == Types.OTHER
        && targetColumn.getTypeCode() == Types.BLOB) {
      printIdentifier("NULL");
    } else {
      printIdentifier(getColumnName(sourceColumn));
    }
  }

  public void deleteInvalidConstraintRows(Database model) {

    try {
      // We will now delete the rows in tables which have a foreign key constraint
      // with "on delete cascade" whose parent has been deleted
      for (int i = 0; i < model.getTableCount(); i++) {
        Table table = model.getTable(i);
        ForeignKey[] fksTable = table.getForeignKeys();
        for (int j = 0; j < fksTable.length; j++) {
          ForeignKey fk = fksTable[j];
          Table parentTable = fk.getForeignTable();
          String col1 = "";
          if (fk.getOnDelete() != null && fk.getOnDelete().contains("cascade")) {
            print("DELETE FROM " + table.getName() + " WHERE ");
            boolean first = true;
            for (int k = 0; k < table.getColumnCount(); k++) {
              if (fk.hasLocalColumn(table.getColumn(k))) {
                if (!first)
                  col1 += (",");
                first = false;
                col1 += (table.getColumn(k).getName());
              }
            }
            print(col1 + " IN ((SELECT " + col1 + " FROM " + table.getName() + ") EXCEPT (SELECT ");
            first = true;
            for (int k = 0; k < parentTable.getColumnCount(); k++) {
              if (fk.hasForeignColumn(parentTable.getColumn(k))) {
                if (!first)
                  print(",");
                first = false;
                print(parentTable.getColumn(k).getName());
              }
            }

            print(" FROM " + parentTable.getName() + "))");
            printEndOfStatement();
          }
        }
      }
    } catch (Exception e) {
      System.out.println(e);
      System.out.println(e.getMessage());
      _log.error(e.getLocalizedMessage());
    }
  }

  protected void disableAllChecks(Table table) throws IOException {

    for (int i = 0; i < table.getCheckCount(); i++) {
      Check check = table.getCheck(i);
      println("ALTER TABLE " + table.getName() + " DROP CONSTRAINT " + check.getName());
      printEndOfStatement();
    }

  }

  protected void enableAllChecks(Table table) throws IOException {

    for (int i = 0; i < table.getCheckCount(); i++) {
      Check check = table.getCheck(i);
      writeExternalCheckCreateStmt(table, check);
      printEndOfStatement();
    }

  }

  @Override
  protected void addColumnStatement(int position) throws IOException {
    if (position > 0) {
      println(",");
    }
    printIndent();
    print("ADD ");
  }

  @Override
  protected void dropColumnStatement(int position) throws IOException {
    if (position > 0) {
      println(",");
    }
    printIndent();
    print("DROP ");
  }

  @Override
  protected void endAlterTable() throws IOException {
    printEndOfStatement();
  }

  @Override
  protected void addDefault(AddColumnChange deferredDefault) throws IOException {
    Column col = deferredDefault.getNewColumn();

    if (col.getOnCreateDefault() != null && col.getLiteralOnCreateDefault() == null
        && col.getDefaultValue() == null) {
      return;
    }

    print("ALTER TABLE " + deferredDefault.getChangedTable().getName() + " ALTER COLUMN "
        + col.getName());
    String dafaultValue = getDefaultValue(col);

    if (dafaultValue != null) {
      print(" SET DEFAULT " + dafaultValue);
    } else {
      print(" DROP DEFAULT");
    }

    printEndOfStatement();
  }

  @Override
  protected void processChange(Database currentModel, Database desiredModel,
      ColumnDefaultValueChange change) throws IOException {

    change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
    print("ALTER TABLE " + change.getChangedTable().getName() + " ALTER COLUMN ");
    printIdentifier(getColumnName(change.getChangedColumn()));
    print(" SET DEFAULT ");
    print(getDefaultValue(change.getChangedColumn()));
    printEndOfStatement();
  }

  @Override
  protected void processChange(Database currentModel, Database desiredModel,
      ColumnRequiredChange change) throws IOException {
    boolean required = change.getRequired();

    change.apply(desiredModel, getPlatform().isDelimitedIdentifierModeOn());

    // whenever a column is set as not null, it needs to be referred in order to populate it with
    // onCreateDefault or moduleScripts
    if (required) {
      desiredModel.addDeferredNotNull(change);
      return;
    }

    print("ALTER TABLE " + change.getTableName() + " ALTER COLUMN ");
    printIdentifier(getColumnName(change.getChangedColumn()));

    if (required) {
      print(" SET NOT NULL");
    } else {
      print(" DROP NOT NULL");
    }
    printEndOfStatement();
  }
}
