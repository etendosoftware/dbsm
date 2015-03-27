package org.apache.ddlutils.platform;

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.DatabaseOperationException;
import org.apache.ddlutils.DdlUtilsException;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.alteration.AddColumnChange;
import org.apache.ddlutils.alteration.AddRowChange;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.ColumnDataChange;
import org.apache.ddlutils.alteration.RemoveRowChange;
import org.apache.ddlutils.dynabean.SqlDynaClass;
import org.apache.ddlutils.dynabean.SqlDynaProperty;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.Trigger;
import org.apache.ddlutils.model.TypeMap;
import org.apache.ddlutils.model.View;
import org.apache.ddlutils.util.ExtTypes;
import org.apache.ddlutils.util.Jdbc3Utils;
import org.apache.ddlutils.util.JdbcSupport;
import org.apache.ddlutils.util.SqlTokenizer;
import org.apache.ddlutils.util.diff_match_patch;
import org.apache.ddlutils.util.diff_match_patch.Diff;
import org.apache.ddlutils.util.diff_match_patch.Operation;
import org.openbravo.ddlutils.util.OBDataset;
import org.openbravo.ddlutils.util.OBDatasetTable;

/**
 * Base class for platform implementations.
 * 
 * @version $Revision: 231110 $
 */
public abstract class PlatformImplBase extends JdbcSupport implements Platform {
  /** The default name for models read from the database, if no name as given. */
  protected static final String MODEL_DEFAULT_NAME = "default";

  /** The log for this platform. */
  private final Log _log = LogFactory.getLog(getClass());

  /** The platform info. */
  private PlatformInfo _info = new PlatformInfo();
  /** The sql builder for this platform. */
  private SqlBuilder _builder;
  /** The model reader for this platform. */
  private JdbcModelReader _modelReader;
  /** The model loader for this platform. */
  private ModelLoader _modelLoader;
  /** Whether script mode is on. */
  private boolean _scriptModeOn;
  /** Whether SQL comments are generated or not. */
  private boolean _sqlCommentsOn = true;
  /** Whether delimited identifiers are used or not. */
  private boolean _delimitedIdentifierModeOn;
  /** Whether identity override is enabled. */
  private boolean _identityOverrideOn;
  /** Whether read foreign keys shall be sorted alphabetically. */
  private boolean _foreignKeysSorted = true;

  private boolean _ignoreWarns = true;

  private boolean _overrideDefaultValueOnMissingData = true;

  private SQLBatchEvaluator batchEvaluator = new StandardBatchEvaluator(this);

  /**
   * {@inheritDoc}
   */
  public SqlBuilder getSqlBuilder() {
    return _builder;
  }

  /**
   * Sets the sql builder for this platform.
   * 
   * @param builder
   *          The sql builder
   */
  protected void setSqlBuilder(SqlBuilder builder) {
    _builder = builder;
  }

  /**
   * {@inheritDoc}
   */
  public ModelLoader getModelLoader() {
    return _modelLoader;
  }

  /**
   * {@inheritDoc}
   */
  public JdbcModelReader getModelReader() {
    if (_modelReader == null) {
      _modelReader = new JdbcModelReader(this);
    }
    return _modelReader;
  }

  /**
   * Sets the model loader for this platform.
   * 
   * @param modelLoader
   *          The model loader
   */
  protected void setModelLoader(ModelLoader modelLoader) {
    _modelLoader = modelLoader;
  }

  /**
   * Sets the model reader for this platform.
   * 
   * @param modelReader
   *          The model reader
   */
  protected void setModelReader(JdbcModelReader modelReader) {
    _modelReader = modelReader;
  }

  /**
   * {@inheritDoc}
   */
  public PlatformInfo getPlatformInfo() {
    return _info;
  }

  /**
   * {@inheritDoc}
   */
  public boolean isScriptModeOn() {
    return _scriptModeOn;
  }

  /**
   * {@inheritDoc}
   */
  public void setScriptModeOn(boolean scriptModeOn) {
    _scriptModeOn = scriptModeOn;
  }

  /**
   * {@inheritDoc}
   */
  public boolean isSqlCommentsOn() {
    return _sqlCommentsOn;
  }

  /**
   * {@inheritDoc}
   */
  public void setSqlCommentsOn(boolean sqlCommentsOn) {
    if (!getPlatformInfo().isSqlCommentsSupported() && sqlCommentsOn) {
      throw new DdlUtilsException("Platform " + getName() + " does not support SQL comments");
    }
    _sqlCommentsOn = sqlCommentsOn;
  }

  /**
   * {@inheritDoc}
   */
  public boolean isDelimitedIdentifierModeOn() {
    return _delimitedIdentifierModeOn;
  }

  /**
   * {@inheritDoc}
   */
  public void setDelimitedIdentifierModeOn(boolean delimitedIdentifierModeOn) {
    if (!getPlatformInfo().isDelimitedIdentifiersSupported() && delimitedIdentifierModeOn) {
      throw new DdlUtilsException("Platform " + getName()
          + " does not support delimited identifier");
    }
    _delimitedIdentifierModeOn = delimitedIdentifierModeOn;
  }

  /**
   * {@inheritDoc}
   */
  public boolean isIdentityOverrideOn() {
    return _identityOverrideOn;
  }

  /**
   * {@inheritDoc}
   */
  public void setIdentityOverrideOn(boolean identityOverrideOn) {
    _identityOverrideOn = identityOverrideOn;
  }

  /**
   * {@inheritDoc}
   */
  public boolean isForeignKeysSorted() {
    return _foreignKeysSorted;
  }

  /**
   * {@inheritDoc}
   */
  public void setForeignKeysSorted(boolean foreignKeysSorted) {
    _foreignKeysSorted = foreignKeysSorted;
  }

  @Override
  public boolean isOverrideDefaultValueOnMissingData() {
    return _overrideDefaultValueOnMissingData;
  }

  @Override
  public void setOverrideDefaultValueOnMissingData(boolean _overrideDefaultValueOnMissingData) {
    this._overrideDefaultValueOnMissingData = _overrideDefaultValueOnMissingData;
  }

  /**
   * Returns the log for this platform.
   * 
   * @return The log
   */
  protected Log getLog() {
    return _log;
  }

  /**
   * Logs any warnings associated to the given connection. Note that the connection needs to be open
   * for this.
   * 
   * @param connection
   *          The open connection
   */
  protected void logWarnings(Connection connection) throws SQLException {
    SQLWarning warning = connection.getWarnings();

    while (warning != null) {
      getLog().warn(warning.getLocalizedMessage(), warning.getCause());
      warning = warning.getNextWarning();
    }
  }

  /**
   * {@inheritDoc}
   */
  public int evaluateBatch(String sql, boolean continueOnError) throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      return evaluateBatch(connection, sql, continueOnError);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public int evaluateBatch(Connection connection, String sql, boolean continueOnError)
      throws DatabaseOperationException {
    List<String> commands = new ArrayList<String>();
    SqlTokenizer tokenizer = new SqlTokenizer(sql);

    while (tokenizer.hasMoreStatements()) {
      commands.add(tokenizer.getNextStatement());
    }
    return evaluateBatch(connection, commands, continueOnError);
  }

  public int evaluateBatch(Connection connection, List<String> sql, boolean continueOnError) {
    return evaluateBatch(connection, sql, continueOnError, 0);
  }

  public int evaluateBatch(Connection connection, List<String> sql, boolean continueOnError,
      long firstSqlCommandIndex) throws DatabaseOperationException {
    return batchEvaluator.evaluateBatch(connection, sql, continueOnError, firstSqlCommandIndex);
  }

  public int evaluateBatchRealBatch(Connection connection, String sql, boolean continueOnError)
      throws DatabaseOperationException {
    ArrayList<String> commands = new ArrayList<String>();
    SqlTokenizer tokenizer = new SqlTokenizer(sql);

    while (tokenizer.hasMoreStatements()) {
      commands.add(tokenizer.getNextStatement());
    }
    return evaluateBatchRealBatch(connection, commands, continueOnError);
  }

  public int evaluateBatchRealBatch(Connection connection, List<String> sql, boolean continueOnError)
      throws DatabaseOperationException {
    return batchEvaluator.evaluateBatchRealBatch(connection, sql, continueOnError);
  }

  /**
   * {@inheritDoc}
   */
  public void shutdownDatabase() throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      shutdownDatabase(connection);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void shutdownDatabase(Connection connection) throws DatabaseOperationException {
    // Per default do nothing as most databases don't need this
  }

  /**
   * {@inheritDoc}
   */
  public void createDatabase(String jdbcDriverClassName, String connectionUrl, String username,
      String password, Map parameters) throws DatabaseOperationException,
      UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Database creation is not supported for the database platform " + getName());
  }

  /**
   * {@inheritDoc}
   */
  public void dropDatabase(String jdbcDriverClassName, String connectionUrl, String username,
      String password) throws DatabaseOperationException, UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Database deletion is not supported for the database platform " + getName());
  }

  /**
   * {@inheritDoc}
   */
  public void createTables(Database model, boolean dropTablesFirst, boolean continueOnError)
      throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      createTables(connection, model, dropTablesFirst, continueOnError);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void createTables(Connection connection, Database model, boolean dropTablesFirst,
      boolean continueOnError) throws DatabaseOperationException {
    String sql = getCreateTablesSql(model, dropTablesFirst, continueOnError);
    _log.info("Finished preparing SQL");

    evaluateBatch(connection, sql, continueOnError);
  }

  /**
   * {@inheritDoc}
   */
  public String getCreateTablesSql(Database model, boolean dropTablesFirst, boolean continueOnError) {
    String sql = null;

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().createTables(model, dropTablesFirst);
      sql = buffer.toString();
    } catch (IOException e) {
      // won't happen because we're using a string writer
    }
    return sql;
  }

  /**
   * {@inheritDoc}
   */
  public String getCreateTablesSqlScript(Database model, boolean dropTablesFirst,
      boolean continueOnError) {
    return getCreateTablesSql(model, dropTablesFirst, continueOnError);
  }

  /**
   * {@inheritDoc}
   */
  public void createTables(Database model, CreationParameters params, boolean dropTablesFirst,
      boolean continueOnError) throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      createTables(connection, model, params, dropTablesFirst, continueOnError);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void createTables(Connection connection, Database model, CreationParameters params,
      boolean dropTablesFirst, boolean continueOnError) throws DatabaseOperationException {
    String sql = getCreateTablesSql(model, params, dropTablesFirst, continueOnError);

    evaluateBatch(connection, sql, continueOnError);
  }

  /**
   * {@inheritDoc}
   */
  public String getCreateTablesSql(Database model, CreationParameters params,
      boolean dropTablesFirst, boolean continueOnError) {
    String sql = null;

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().createTables(model, params, dropTablesFirst);
      sql = buffer.toString();
    } catch (IOException e) {
      // won't happen because we're using a string writer
    }
    return sql;
  }

  @Override
  public void createAllFKs(Database model, boolean continueOnError) {
    Connection connection = borrowConnection();

    try {
      String sql;
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().createExternalForeignKeys(model);
      sql = buffer.toString();
      evaluateBatchRealBatch(connection, sql, continueOnError);
    } catch (IOException e) {
      // won't happen because we're using a string writer
    }

    returnConnection(connection);
  }

  /**
   * {@inheritDoc}
   */
  public void alterTables(Database desiredDb, boolean continueOnError)
      throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      alterTables(connection, desiredDb, continueOnError);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public String getAlterTablesSql(Database desiredDb) throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      return getAlterTablesSql(connection, desiredDb);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void alterTables(Database desiredDb, CreationParameters params, boolean continueOnError)
      throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      alterTables(connection, desiredDb, params, continueOnError);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public String getAlterTablesSql(Database desiredDb, CreationParameters params)
      throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      return getAlterTablesSql(connection, desiredDb, params);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void alterTables(Connection connection, Database desiredModel, boolean continueOnError)
      throws DatabaseOperationException {
    String sql = getAlterTablesSql(connection, desiredModel);

    evaluateBatch(connection, sql, continueOnError);
  }

  public void alterTables(Connection connection, Database desiredModel, boolean continueOnError,
      List changes) throws DatabaseOperationException {
    String sql = getAlterTablesSql(connection, desiredModel, changes);

    evaluateBatch(connection, sql, continueOnError);
  }

  /**
   * {@inheritDoc}
   */
  public void alterTables(Database currentModel, Database desiredModel, boolean continueOnError)
      throws DatabaseOperationException {

    Connection connection = borrowConnection();

    try {
      alterTables(connection, currentModel, desiredModel, continueOnError);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void alterTables(Connection connection, Database currentModel, Database desiredModel,
      boolean continueOnError) throws DatabaseOperationException {
    String sql = null;

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().alterDatabase(currentModel, desiredModel, null);
      sql = buffer.toString();
    } catch (IOException ex) {
      // won't happen because we're using a string writer
    }

    evaluateBatch(connection, sql, continueOnError);
  }

  public void alterTables(Database currentModel, Database desiredModel, boolean continueOnError,
      List changes) throws DatabaseOperationException {
    String sql = null;

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().alterDatabase(currentModel, desiredModel, null, changes);
      sql = buffer.toString();
    } catch (IOException ex) {
      // won't happen because we're using a string writer
    }
    evaluateBatch(sql, continueOnError);
  }

  /**
   * @deprecated should receive ad
   */
  public boolean alterTablesPostScript(Database currentModel, Database desiredModel,
      boolean continueOnError, List changes, Database fullModel) {
    return alterTablesPostScript(currentModel, desiredModel, continueOnError, changes, fullModel,
        null);
  }

  public boolean alterTablesPostScript(Database currentModel, Database desiredModel,
      boolean continueOnError, List changes, Database fullModel, OBDataset ad)
      throws DatabaseOperationException {

    Connection connection = borrowConnection();

    try {
      return alterTablesPostScript(connection, currentModel, desiredModel, continueOnError,
          changes, fullModel, ad);
    } finally {
      returnConnection(connection);
    }

  }

  public List alterTablesRecreatePKs(Database currentModel, Database desiredModel,
      boolean continueOnError) throws DatabaseOperationException {

    Connection connection = borrowConnection();

    try {
      return alterTablesRecreatePKs(connection, currentModel, desiredModel, continueOnError);
    } finally {
      returnConnection(connection);
    }

  }

  public boolean alterTablesPostScript(Connection connection, Database currentModel,
      Database desiredModel, boolean continueOnError, List changes, Database fullModel, OBDataset ad)
      throws DatabaseOperationException {
    String sql = null;

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().alterDatabasePostScript(currentModel, desiredModel, null, changes, fullModel,
          ad);
      sql = buffer.toString();
    } catch (IOException ex) {
      // won't happen because we're using a string writer
    }
    _ignoreWarns = false;
    int numErrors = evaluateBatch(connection, sql, continueOnError);
    if (numErrors > 0) {
      return false;
    }
    return true;
  }

  public List alterTablesRecreatePKs(Connection connection, Database currentModel,
      Database desiredModel, boolean continueOnError) throws DatabaseOperationException {
    String sql = null;

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      List changes = getSqlBuilder().alterDatabaseRecreatePKs(currentModel, desiredModel, null);
      sql = buffer.toString();
      evaluateBatch(connection, sql, continueOnError);

      return changes;
    } catch (IOException ex) {
      // won't happen because we're using a string writer
    }
    return null;

  }

  public void alterData(Connection connection, Database model, Vector<Change> changes)
      throws DatabaseOperationException {
    for (Change change : changes) {
      if (change instanceof AddRowChange) {
        upsert(connection, model, ((AddRowChange) change).getRow());
      } else if (change instanceof RemoveRowChange) {
        delete(connection, model, (RemoveRowChange) change);
      } else if (change instanceof ColumnDataChange) {
        update(connection, model, (ColumnDataChange) change);
      }
    }
  }

  public void alterData(Database model, Vector<Change> changes, Writer writer)
      throws DatabaseOperationException {
    try {
      getSqlBuilder().setWriter(writer);
      for (Change change : changes) {
        if (change instanceof AddRowChange) {
          HashMap map = new HashMap();
          AddRowChange addChange = (AddRowChange) change;
          Table table = addChange.getTable();
          for (int i = 0; i < table.getColumnCount(); i++) {
            if (table.getColumn(i).getName().equalsIgnoreCase("UPDATED"))
              map.put(table.getColumn(i).getName(), "now()");
            else if (table.getColumn(i).getName().equalsIgnoreCase("UPDATEDBY"))
              map.put(table.getColumn(i).getName(), "O");
            else if (table.getColumn(i).getName().equalsIgnoreCase("CREATED"))
              map.put(table.getColumn(i).getName(), "now()");
            else if (table.getColumn(i).getName().equalsIgnoreCase("CREATEDBY"))
              map.put(table.getColumn(i).getName(), "0");
            else
              map.put(table.getColumn(i).getName(),
                  addChange.getRow().get(table.getColumn(i).getName()));
          }

          writer.append(getSqlBuilder().getInsertSql(table, map, false));
          getSqlBuilder().printEndOfStatement("");
        } else if (change instanceof RemoveRowChange) {
          RemoveRowChange removeChange = (RemoveRowChange) change;
          HashMap map = new HashMap();
          Table table = removeChange.getTable();
          for (int i = 0; i < table.getPrimaryKeyColumns().length; i++)
            map.put(table.getPrimaryKeyColumns()[i].getName(),
                removeChange.getRow().get(table.getPrimaryKeyColumns()[i].getName()).toString());
          writer.append(getSqlBuilder().getDeleteSql(table, map, false));
          getSqlBuilder().printEndOfStatement("");
        } else if (change instanceof ColumnDataChange) {
          HashMap map = new HashMap();
          ColumnDataChange colChange = (ColumnDataChange) change;
          Table table = colChange.getTable();
          String pk = table.getPrimaryKeyColumns()[0].getName();
          map.put(pk, colChange.getPkRow());
          if (table.findColumn("UPDATED") != null)
            map.put("UPDATED", "now()");
          else if (table.findColumn("UPDATEDBY") != null)
            map.put("UPDATEDBY", "O");
          if (colChange.getColumnname().equalsIgnoreCase("CREATED"))
            map.put(colChange.getColumnname(), "now()");
          else if (colChange.getColumnname().equalsIgnoreCase("CREATEDBY"))
            map.put(colChange.getColumnname(), "0");
          else
            map.put(colChange.getColumnname(), colChange.getNewValue());

          writer.append(getSqlBuilder().getUpdateSql(table, map, false));
          getSqlBuilder().printEndOfStatement("");
        }
      }
      writer.flush();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
   * String sql = null;
   * 
   * try { StringWriter buffer = new StringWriter();
   * 
   * getSqlBuilder().setWriter(buffer); getSqlBuilder().alterData(model, changes); sql =
   * buffer.toString(); } catch (IOException ex) { // won't happen because we're using a string
   * writer } evaluateBatch(connection, sql, true);
   */

  /**
   * {@inheritDoc}
   */
  public String getAlterTablesSql(Connection connection, Database desiredModel)
      throws DatabaseOperationException {
    String sql = null;
    Database currentModel = readModelFromDatabase(connection, desiredModel.getName());

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().alterDatabase(currentModel, desiredModel, null);
      sql = buffer.toString();
    } catch (IOException ex) {
      // won't happen because we're using a string writer
    }
    return sql;
  }

  public String getAlterTablesSql(Connection connection, Database desiredModel, List changes)
      throws DatabaseOperationException {
    String sql = null;
    Database currentModel = readModelFromDatabase(connection, desiredModel.getName());

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      System.out.println("0");
      getSqlBuilder().alterDatabase(currentModel, desiredModel, null, changes);
      System.out.println("9");
      sql = buffer.toString();
      System.out.println(sql);
    } catch (IOException ex) {
      // won't happen because we're using a string writer
    }
    return sql;
  }

  /**
   * {@inheritDoc}
   */
  public void alterTables(Connection connection, Database desiredModel, CreationParameters params,
      boolean continueOnError) throws DatabaseOperationException {
    String sql = getAlterTablesSql(connection, desiredModel, params);

    evaluateBatch(connection, sql, continueOnError);
  }

  /**
   * {@inheritDoc}
   */
  public String getAlterTablesSql(Connection connection, Database desiredModel,
      CreationParameters params) throws DatabaseOperationException {
    String sql = null;
    Database currentModel = readModelFromDatabase(connection, desiredModel.getName());

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().alterDatabase(currentModel, desiredModel, params);
      sql = buffer.toString();
    } catch (IOException ex) {
      // won't happen because we're using a string writer
    }
    return sql;
  }

  /**
   * {@inheritDoc}
   */
  public void alterTables(String catalog, String schema, String[] tableTypes,
      Database desiredModel, boolean continueOnError) throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      alterTables(connection, catalog, schema, tableTypes, desiredModel, continueOnError);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public String getAlterTablesSql(String catalog, String schema, String[] tableTypes,
      Database desiredModel) throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      return getAlterTablesSql(connection, catalog, schema, tableTypes, desiredModel);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void alterTables(String catalog, String schema, String[] tableTypes,
      Database desiredModel, CreationParameters params, boolean continueOnError)
      throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      alterTables(connection, catalog, schema, tableTypes, desiredModel, params, continueOnError);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public String getAlterTablesSql(String catalog, String schema, String[] tableTypes,
      Database desiredModel, CreationParameters params) throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      return getAlterTablesSql(connection, catalog, schema, tableTypes, desiredModel, params);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void alterTables(Connection connection, String catalog, String schema,
      String[] tableTypes, Database desiredModel, boolean continueOnError)
      throws DatabaseOperationException {
    String sql = getAlterTablesSql(connection, catalog, schema, tableTypes, desiredModel);

    evaluateBatch(connection, sql, continueOnError);
  }

  /**
   * {@inheritDoc}
   */
  public String getAlterTablesSql(Connection connection, String catalog, String schema,
      String[] tableTypes, Database desiredModel) throws DatabaseOperationException {
    String sql = null;
    Database currentModel = readModelFromDatabase(connection, desiredModel.getName(), catalog,
        schema, tableTypes);

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().alterDatabase(currentModel, desiredModel, null);
      sql = buffer.toString();
    } catch (IOException ex) {
      // won't happen because we're using a string writer
    }
    return sql;
  }

  /**
   * {@inheritDoc}
   */
  public void alterTables(Connection connection, String catalog, String schema,
      String[] tableTypes, Database desiredModel, CreationParameters params, boolean continueOnError)
      throws DatabaseOperationException {
    String sql = getAlterTablesSql(connection, catalog, schema, tableTypes, desiredModel, params);

    evaluateBatch(connection, sql, continueOnError);
  }

  /**
   * {@inheritDoc}
   */
  public String getAlterTablesSql(Connection connection, String catalog, String schema,
      String[] tableTypes, Database desiredModel, CreationParameters params)
      throws DatabaseOperationException {
    String sql = null;
    Database currentModel = readModelFromDatabase(connection, desiredModel.getName(), catalog,
        schema, tableTypes);

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().alterDatabase(currentModel, desiredModel, params);
      sql = buffer.toString();
    } catch (IOException ex) {
      // won't happen because we're using a string writer
    }
    return sql;
  }

  /**
   * {@inheritDoc}
   */
  public void dropTable(Connection connection, Database model, Table table, boolean continueOnError)
      throws DatabaseOperationException {
    String sql = getDropTableSql(model, table, continueOnError);

    evaluateBatch(connection, sql, continueOnError);
  }

  /**
   * {@inheritDoc}
   */
  public void dropTable(Database model, Table table, boolean continueOnError)
      throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      dropTable(connection, model, table, continueOnError);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public String getDropTableSql(Database model, Table table, boolean continueOnError) {
    String sql = null;

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().dropTable(model, table);
      sql = buffer.toString();
    } catch (IOException e) {
      // won't happen because we're using a string writer
    }
    return sql;
  }

  /**
   * {@inheritDoc}
   */
  public void dropTables(Database model, boolean continueOnError) throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      dropTables(connection, model, continueOnError);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void dropTables(Connection connection, Database model, boolean continueOnError)
      throws DatabaseOperationException {
    String sql = getDropTablesSql(model, continueOnError);

    evaluateBatch(connection, sql, continueOnError);
  }

  /**
   * {@inheritDoc}
   */
  public String getDropTablesSql(Database model, boolean continueOnError) {
    String sql = null;

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().dropTables(model);
      sql = buffer.toString();
    } catch (IOException e) {
      // won't happen because we're using a string writer
    }
    return sql;
  }

  /**
   * {@inheritDoc}
   */
  public Iterator query(Database model, String sql) throws DatabaseOperationException {
    return query(model, sql, (Table[]) null);
  }

  /**
   * {@inheritDoc}
   */
  public Iterator query(Database model, String sql, Collection parameters)
      throws DatabaseOperationException {
    return query(model, sql, parameters, null);
  }

  /**
   * {@inheritDoc}
   */
  public Iterator query(Database model, String sql, Table[] queryHints)
      throws DatabaseOperationException {
    Connection connection = borrowConnection();
    Statement statement = null;
    ResultSet resultSet = null;
    Iterator answer = null;

    try {
      statement = connection.createStatement();
      resultSet = statement.executeQuery(sql);
      answer = createResultSetIterator(model, resultSet, queryHints);
      return answer;
    } catch (SQLException ex) {
      System.out.println(sql);
      System.out.println(ex.getLocalizedMessage());
      return null;
      // throw new
      // DatabaseOperationException("Error while performing a query", ex);
    } finally {
      // if any exceptions are thrown, close things down
      // otherwise we're leaving it open for the iterator
      if (answer == null) {
        closeStatement(statement);
        returnConnection(connection);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  public Iterator query(Database model, String sql, Collection parameters, Table[] queryHints)
      throws DatabaseOperationException {
    Connection connection = borrowConnection();
    PreparedStatement statement = null;
    ResultSet resultSet = null;
    Iterator answer = null;

    try {
      statement = connection.prepareStatement(sql);

      int paramIdx = 1;

      for (Iterator iter = parameters.iterator(); iter.hasNext(); paramIdx++) {
        Object arg = iter.next();

        if (arg instanceof BigDecimal) {
          // to avoid scale problems because setObject assumes a scale
          // of 0
          statement.setBigDecimal(paramIdx, (BigDecimal) arg);
        } else {
          statement.setObject(paramIdx, arg);
        }
      }
      resultSet = statement.executeQuery();
      answer = createResultSetIterator(model, resultSet, queryHints);
      return answer;
    } catch (SQLException ex) {
      throw new DatabaseOperationException("Error while performing a query", ex);
    } finally {
      // if any exceptions are thrown, close things down
      // otherwise we're leaving it open for the iterator
      if (answer == null) {
        closeStatement(statement);
        returnConnection(connection);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  public List fetch(Database model, String sql) throws DatabaseOperationException {
    return fetch(model, sql, (Table[]) null, 0, -1);
  }

  /**
   * {@inheritDoc}
   */
  public List fetch(Database model, String sql, Table[] queryHints)
      throws DatabaseOperationException {
    return fetch(model, sql, queryHints, 0, -1);
  }

  /**
   * {@inheritDoc}
   */
  public List fetch(Database model, String sql, int start, int end)
      throws DatabaseOperationException {
    return fetch(model, sql, (Table[]) null, start, end);
  }

  /**
   * {@inheritDoc}
   */
  public List fetch(Database model, String sql, Table[] queryHints, int start, int end)
      throws DatabaseOperationException {
    Connection connection = borrowConnection();
    Statement statement = null;
    ResultSet resultSet = null;
    List result = new ArrayList();

    try {
      statement = connection.createStatement();
      resultSet = statement.executeQuery(sql);

      int rowIdx = 0;

      for (ModelBasedResultSetIterator it = createResultSetIterator(model, resultSet, queryHints); ((end < 0) || (rowIdx <= end))
          && it.hasNext(); rowIdx++) {
        if (rowIdx >= start) {
          result.add(it.next());
        } else {
          it.advance();
        }
      }
    } catch (SQLException ex) {
      throw new DatabaseOperationException("Error while fetching data from the database", ex);
    } finally {
      // the iterator should return the connection automatically
      // so this is usually not necessary (but just in case)
      closeStatement(statement);
      returnConnection(connection);
    }
    return result;
  }

  /**
   * {@inheritDoc}
   */
  public List fetch(Database model, String sql, Collection parameters)
      throws DatabaseOperationException {
    return fetch(model, sql, parameters, null, 0, -1);
  }

  /**
   * {@inheritDoc}
   */
  public List fetch(Database model, String sql, Collection parameters, int start, int end)
      throws DatabaseOperationException {
    return fetch(model, sql, parameters, null, start, end);
  }

  /**
   * {@inheritDoc}
   */
  public List fetch(Database model, String sql, Collection parameters, Table[] queryHints)
      throws DatabaseOperationException {
    return fetch(model, sql, parameters, queryHints, 0, -1);
  }

  /**
   * {@inheritDoc}
   */
  public List fetch(Database model, String sql, Collection parameters, Table[] queryHints,
      int start, int end) throws DatabaseOperationException {
    Connection connection = borrowConnection();
    PreparedStatement statement = null;
    ResultSet resultSet = null;
    List result = new ArrayList();

    try {
      statement = connection.prepareStatement(sql);

      int paramIdx = 1;

      for (Iterator iter = parameters.iterator(); iter.hasNext(); paramIdx++) {
        Object arg = iter.next();

        if (arg instanceof BigDecimal) {
          // to avoid scale problems because setObject assumes a scale
          // of 0
          statement.setBigDecimal(paramIdx, (BigDecimal) arg);
        } else {
          statement.setObject(paramIdx, arg);
        }
      }
      resultSet = statement.executeQuery();

      int rowIdx = 0;

      for (ModelBasedResultSetIterator it = createResultSetIterator(model, resultSet, queryHints); ((end < 0) || (rowIdx <= end))
          && it.hasNext(); rowIdx++) {
        if (rowIdx >= start) {
          result.add(it.next());
        } else {
          it.advance();
        }
      }
    } catch (SQLException ex) {
      // any other exception comes from the iterator which closes the
      // resources automatically
      closeStatement(statement);
      returnConnection(connection);
      throw new DatabaseOperationException("Error while fetching data from the database", ex);
    }
    return result;
  }

  /**
   * Creates the SQL for inserting an object of the given type. If a concrete bean is given, then a
   * concrete insert statement is created, otherwise an insert statement usable in a prepared
   * statement is build.
   * 
   * @param model
   *          The database model
   * @param dynaClass
   *          The type
   * @param properties
   *          The properties to write
   * @param bean
   *          Optionally the concrete bean to insert
   * @return The SQL required to insert an instance of the class
   */
  protected String createInsertSql(Database model, SqlDynaClass dynaClass,
      SqlDynaProperty[] properties, DynaBean bean) {
    Table table = model.findTable(dynaClass.getTableName());
    HashMap columnValues = toColumnValues(properties, bean);

    return _builder.getInsertSql(table, columnValues, bean == null);
  }

  /**
   * Creates the SQL for querying for the id generated by the last insert of an object of the given
   * type.
   * 
   * @param model
   *          The database model
   * @param dynaClass
   *          The type
   * @return The SQL required for querying for the id, or <code>null</code> if the database does not
   *         support this
   */
  protected String createSelectLastInsertIdSql(Database model, SqlDynaClass dynaClass) {
    Table table = model.findTable(dynaClass.getTableName());

    return _builder.getSelectLastIdentityValues(table);
  }

  /**
   * {@inheritDoc}
   */
  public String getInsertSql(Database model, DynaBean dynaBean) {
    SqlDynaClass dynaClass = model.getDynaClassFor(dynaBean);
    SqlDynaProperty[] properties = dynaClass.getSqlDynaProperties();

    if (properties.length == 0) {
      _log.info("Cannot insert instances of type " + dynaClass + " because it has no properties");
      return null;
    }

    return createInsertSql(model, dynaClass, properties, dynaBean);
  }

  /**
   * Returns all properties where the column is not non-autoincrement and for which the bean either
   * has a value or the column hasn't got a default value, for the given dyna class.
   * 
   * @param model
   *          The database model
   * @param dynaClass
   *          The dyna class
   * @param bean
   *          The bean
   * @return The properties
   */
  private SqlDynaProperty[] getPropertiesForInsertion(Database model, SqlDynaClass dynaClass,
      final DynaBean bean) {
    SqlDynaProperty[] properties = dynaClass.getSqlDynaProperties();

    Collection result = CollectionUtils.select(Arrays.asList(properties), new Predicate() {
      public boolean evaluate(Object input) {
        SqlDynaProperty prop = (SqlDynaProperty) input;

        if (bean.get(prop.getName()) != null) {
          // we ignore properties for which a value is present
          // in the bean
          // only if they are identity and identity override
          // is off or
          // the platform does not allow the override of the
          // auto-increment
          // specification
          return !prop.getColumn().isAutoIncrement()
              || (isIdentityOverrideOn() && getPlatformInfo().isIdentityOverrideAllowed());
        } else {
          // we also return properties without a value in the
          // bean
          // if they ain't auto-increment and don't have a
          // default value
          // in this case, a NULL is inserted
          if (_overrideDefaultValueOnMissingData) {
            // CORRECTION: If a property hasn't got value, and
            // is not autoincrement,
            // a NULL will be inserted. We don't care about
            // default values.
            // Before this change, if a nullable column had
            // default value, if a row had NULL value
            // in the database, after exporting and importing it
            // the value would have been changed
            // to the default value, and this is not correct.
            return !prop.getColumn().isAutoIncrement();
          } else {
            /*
             * original logic: if a property has no value, but the column has a default value then
             * do skip it, so the default value is used
             */
            return !prop.getColumn().isAutoIncrement()
                && (prop.getColumn().getDefaultValue() == null);
          }
        }
      }
    });

    return (SqlDynaProperty[]) result.toArray(new SqlDynaProperty[result.size()]);
  }

  /**
   * Returns all identity properties whose value were defined by the database and which now need to
   * be read back from the DB.
   * 
   * @param model
   *          The database model
   * @param dynaClass
   *          The dyna class
   * @param bean
   *          The bean
   * @return The columns
   */
  private Column[] getRelevantIdentityColumns(Database model, SqlDynaClass dynaClass,
      final DynaBean bean) {
    SqlDynaProperty[] properties = dynaClass.getSqlDynaProperties();

    Collection relevantProperties = CollectionUtils.select(Arrays.asList(properties),
        new Predicate() {
          public boolean evaluate(Object input) {
            SqlDynaProperty prop = (SqlDynaProperty) input;

            // we only want those identity columns that were really
            // specified by the DB
            // if the platform allows specification of values for identity
            // columns
            // in INSERT/UPDATE statements, then we need to filter the
            // corresponding
            // columns out
            return prop.getColumn().isAutoIncrement()
                && (!isIdentityOverrideOn() || !getPlatformInfo().isIdentityOverrideAllowed() || (bean
                    .get(prop.getName()) == null));
          }
        });

    Column[] columns = new Column[relevantProperties.size()];
    int idx = 0;

    for (Iterator propIt = relevantProperties.iterator(); propIt.hasNext(); idx++) {
      columns[idx] = ((SqlDynaProperty) propIt.next()).getColumn();
    }
    return columns;
  }

  /**
   * {@inheritDoc}
   */
  public void upsert(Connection connection, Database model, DynaBean dynaBean)
      throws DatabaseOperationException {
    _log.debug("Attempting to update a single row " + dynaBean + " into table ");
    // int count = privateupdate(connection, model, dynaBean);
    // if (count == 0) {
    insert(connection, model, dynaBean);
    /*
     * } else if (count != 1) { _log.warn("Attempted to update a single row " + dynaBean +
     * " into table " + model.getDynaClassFor(dynaBean).getTableName() + " but changed " + count +
     * " row(s)"); }
     */
  }

  /**
   * {@inheritDoc}
   */
  public void updateinsert(Connection connection, Database model, DynaBean dynaBean)
      throws DatabaseOperationException {
    _log.debug("Attempting to update a single row " + dynaBean + " into table ");
    int count = privateupdate(connection, model, dynaBean);
    if (count == 0) {
      insert(connection, model, dynaBean);
    } else if (count != 1) {
      _log.warn("Attempted to update a single row " + dynaBean + " into table "
          + model.getDynaClassFor(dynaBean).getTableName() + " but changed " + count + " row(s)");
    }
  }

  /**
   * {@inheritDoc}
   */
  public void insert(Connection connection, Database model, DynaBean dynaBean)
      throws DatabaseOperationException {
    SqlDynaClass dynaClass = model.getDynaClassFor(dynaBean);
    SqlDynaProperty[] properties = getPropertiesForInsertion(model, dynaClass, dynaBean);
    Column[] autoIncrColumns = getRelevantIdentityColumns(model, dynaClass, dynaBean);

    if ((properties.length == 0) && (autoIncrColumns.length == 0)) {
      _log.warn("Cannot insert instances of type " + dynaClass
          + " because it has no usable properties");
      return;
    }

    String insertSql = createInsertSql(model, dynaClass, properties, null);
    String queryIdentitySql = null;

    if (_log.isDebugEnabled()) {
      _log.debug("About to execute SQL: " + insertSql);
    }

    if (autoIncrColumns.length > 0) {
      if (!getPlatformInfo().isLastIdentityValueReadable()) {
        _log.warn("The database does not support querying for auto-generated column values");
      } else {
        queryIdentitySql = createSelectLastInsertIdSql(model, dynaClass);
      }
    }

    boolean autoCommitMode = false;
    PreparedStatement statement = null;

    Table table = model.findTable(dynaClass.getTableName());
    try {
      if (!getPlatformInfo().isAutoCommitModeForLastIdentityValueReading()) {
        autoCommitMode = connection.getAutoCommit();
        connection.setAutoCommit(false);
      }

      Timestamp date = getDateStatement(connection);

      beforeInsert(connection, dynaClass.getTable());

      statement = connection.prepareStatement(insertSql);

      int sqlIndex = 1;
      for (int idx = 0; idx < properties.length; idx++) {
        if (table.findColumn(properties[idx].getName()) != null) {
          String propName = properties[idx].getName();
          Object propValue = dynaBean.get(propName);
          boolean valuePresent = (propValue != null);
          // if we have a value in the xml -> just use it
          if (valuePresent) {
            setObject(statement, sqlIndex++, dynaBean, properties[idx]); // idx + 1
            continue;
          }
          // if value is missing handle the 4 audit columns specially
          if (properties[idx].getName().equalsIgnoreCase("UPDATED")
              || properties[idx].getName().equalsIgnoreCase("CREATED")) {
            setStatementParameterValue(statement, sqlIndex++, properties[idx].getColumn()
                .getTypeCode(), date);
          } else if (properties[idx].getName().equalsIgnoreCase("UPDATEDBY")
              || properties[idx].getName().equalsIgnoreCase("CREATEDBY")) {
            setStatementParameterValue(statement, sqlIndex++, properties[idx].getColumn()
                .getTypeCode(), "0");
          } else {
            setObject(statement, sqlIndex++, dynaBean, properties[idx]); // idx + 1
          }
        } else
          _log.debug("Rejected column: " + properties[idx].getName());
      }

      int count = statement.executeUpdate();

      afterInsert(connection, dynaClass.getTable());

      if (count != 1) {
        _log.warn("Attempted to insert a single row " + dynaBean + " in table "
            + dynaClass.getTableName() + " but changed " + count + " row(s)");
      }
    } catch (SQLException ex) {
      throw new DatabaseOperationException("Error while inserting into the database: "
          + ex.getMessage(), ex);
    } finally {
      closeStatement(statement);
    }
    if (queryIdentitySql != null) {
      Statement queryStmt = null;
      ResultSet lastInsertedIds = null;

      try {
        if (getPlatformInfo().isAutoCommitModeForLastIdentityValueReading()) {
          // we'll commit the statement(s) if no auto-commit is
          // enabled because
          // otherwise it is possible that the auto increment hasn't
          // happened yet
          // (the db didn't actually perform the insert yet so no
          // triggering of
          // sequences did occur)
          if (!connection.getAutoCommit()) {
            connection.commit();
          }
        }

        queryStmt = connection.createStatement();
        lastInsertedIds = queryStmt.executeQuery(queryIdentitySql);

        lastInsertedIds.next();

        for (int idx = 0; idx < autoIncrColumns.length; idx++) {
          // we're using the index rather than the name because we
          // cannot know how
          // the SQL statement looks like; rather we assume that we
          // get the values
          // back in the same order as the auto increment columns
          Object value = getObjectFromResultSet(lastInsertedIds, autoIncrColumns[idx], idx + 1);

          PropertyUtils.setProperty(dynaBean, autoIncrColumns[idx].getName(), value);
        }
      } catch (NoSuchMethodException ex) {
        // Can't happen because we're using dyna beans
      } catch (IllegalAccessException ex) {
        // Can't happen because we're using dyna beans
      } catch (InvocationTargetException ex) {
        // Can't happen because we're using dyna beans
      } catch (SQLException ex) {
        throw new DatabaseOperationException(
            "Error while retrieving the identity column value(s) from the database", ex);
      } finally {
        if (lastInsertedIds != null) {
          try {
            lastInsertedIds.close();
          } catch (SQLException ex) {
            // we ignore this one
          }
        }
        closeStatement(statement);
      }
    }
    if (!getPlatformInfo().isAutoCommitModeForLastIdentityValueReading()) {
      try {
        // we need to do a manual commit now
        connection.commit();
        connection.setAutoCommit(autoCommitMode);
      } catch (SQLException ex) {
        throw new DatabaseOperationException(ex);
      }
    }
  }

  protected Timestamp getDateStatement(Connection connection) {
    try {
      PreparedStatement dateStatement = connection.prepareStatement("SELECT NOW() FROM DUAL");
      dateStatement.execute();
      ResultSet resDate = dateStatement.getResultSet();
      resDate.next();
      Timestamp date = resDate.getTimestamp(1);
      dateStatement.close();
      return date;
    } catch (Exception e) {
      // don't print stack trace, this can happen in test cases
      // e.printStackTrace();
    }
    return null;

  }

  /**
   * {@inheritDoc}
   */
  public void insert(Database model, DynaBean dynaBean) throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      insert(connection, model, dynaBean);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void insert(Connection connection, Database model, Collection dynaBeans)
      throws DatabaseOperationException {
    SqlDynaClass dynaClass = null;
    SqlDynaProperty[] properties = null;
    PreparedStatement statement = null;
    int addedStmts = 0;
    boolean identityWarningPrinted = false;

    for (Iterator it = dynaBeans.iterator(); it.hasNext();) {
      DynaBean dynaBean = (DynaBean) it.next();
      SqlDynaClass curDynaClass = model.getDynaClassFor(dynaBean);

      if (curDynaClass != dynaClass) {
        if (dynaClass != null) {
          executeBatch(statement, addedStmts, dynaClass.getTable());
          addedStmts = 0;
        }

        dynaClass = curDynaClass;
        properties = getPropertiesForInsertion(model, curDynaClass, dynaBean);

        if (properties.length == 0) {
          _log.warn("Cannot insert instances of type " + dynaClass
              + " because it has no usable properties");
          continue;
        }
        if (!identityWarningPrinted
            && (getRelevantIdentityColumns(model, curDynaClass, dynaBean).length > 0)) {
          _log.warn("Updating the bean properties corresponding to auto-increment columns is not supported in batch mode");
          identityWarningPrinted = true;
        }

        String insertSql = createInsertSql(model, dynaClass, properties, null);

        if (_log.isDebugEnabled()) {
          _log.debug("Starting new batch with SQL: " + insertSql);
        }
        try {
          statement = connection.prepareStatement(insertSql);
        } catch (SQLException ex) {
          throw new DatabaseOperationException("Error while preparing insert statement", ex);
        }
      }
      try {
        Table table = model.findTable(dynaClass.getTableName());
        Timestamp date = getDateStatement(connection);
        int sqlIndex = 1;
        for (int idx = 0; idx < properties.length; idx++) {
          if (table.findColumn(properties[idx].getName()) != null) {
            String propName = properties[idx].getName();
            Object propValue = dynaBean.get(propName);
            boolean valuePresent = (propValue != null);
            // if we have a value in the xml -> just use it
            if (valuePresent) {
              setObject(statement, sqlIndex++, dynaBean, properties[idx]); // idx + 1
              continue;
            }
            // if value is missing handle the 4 audit columns specially
            if (properties[idx].getName().equalsIgnoreCase("UPDATED")
                || properties[idx].getName().equalsIgnoreCase("CREATED")) {
              setStatementParameterValue(statement, sqlIndex++, properties[idx].getColumn()
                  .getTypeCode(), date);
            } else if (properties[idx].getName().equalsIgnoreCase("UPDATEDBY")
                || properties[idx].getName().equalsIgnoreCase("CREATEDBY")) {
              setStatementParameterValue(statement, sqlIndex++, properties[idx].getColumn()
                  .getTypeCode(), "0");
            } else {
              setObject(statement, sqlIndex++, dynaBean, properties[idx]); // idx + 1
            }
          } else
            _log.debug("Rejected column: " + properties[idx].getName());

        }
        statement.addBatch();
        addedStmts++;
      } catch (SQLException ex) {
        throw new DatabaseOperationException("Error while adding batch insert", ex);
      }
    }
    if (dynaClass != null) {
      executeBatch(statement, addedStmts, dynaClass.getTable());
    }
  }

  /**
   * Performs the batch for the given statement, and checks that the specified amount of rows have
   * been changed.
   * 
   * @param statement
   *          The prepared statement
   * @param numRows
   *          The number of rows that should change
   * @param table
   *          The changed table
   */
  private void executeBatch(PreparedStatement statement, int numRows, Table table)
      throws DatabaseOperationException {
    if (statement != null) {
      try {
        Connection connection = statement.getConnection();

        beforeInsert(connection, table);

        int[] results = statement.executeBatch();

        closeStatement(statement);
        afterInsert(connection, table);

        boolean hasSum = true;
        int sum = 0;

        for (int idx = 0; (results != null) && (idx < results.length); idx++) {
          if (results[idx] < 0) {
            hasSum = false;
            if (Jdbc3Utils.supportsJava14BatchResultCodes()) {
              String msg = Jdbc3Utils.getBatchResultMessage(table.getName(), idx, results[idx]);

              if (msg != null) {
                _log.warn(msg);
              }
            }
          } else {
            sum += results[idx];
          }
        }
        if (hasSum && (sum != numRows)) {
          _log.warn("Attempted to insert " + numRows + " rows into table " + table.getName()
              + " but changed " + sum + " rows");
        }
      } catch (SQLException ex) {
        throw new DatabaseOperationException("Error while inserting into the database : "
            + table.getName(), ex);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  public void insert(Database model, Collection dynaBeans) throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      insert(connection, model, dynaBeans);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * Allows platforms to issue statements directly before rows are inserted into the specified
   * table.
   * 
   * @param connection
   *          The connection used for the insertion
   * @param table
   *          The table that the rows are inserted into
   */
  protected void beforeInsert(Connection connection, Table table) throws SQLException {
  }

  /**
   * Allows platforms to issue statements directly after rows have been inserted into the specified
   * table.
   * 
   * @param connection
   *          The connection used for the insertion
   * @param table
   *          The table that the rows have been inserted into
   */
  protected void afterInsert(Connection connection, Table table) throws SQLException {
  }

  /**
   * Creates the SQL for updating an object of the given type. If a concrete bean is given, then a
   * concrete update statement is created, otherwise an update statement usable in a prepared
   * statement is build.
   * 
   * @param model
   *          The database model
   * @param dynaClass
   *          The type
   * @param primaryKeys
   *          The primary keys
   * @param properties
   *          The properties to write
   * @param bean
   *          Optionally the concrete bean to update
   * @return The SQL required to update the instance
   */
  protected String createUpdateSql(Database model, SqlDynaClass dynaClass,
      SqlDynaProperty[] primaryKeys, SqlDynaProperty[] properties, DynaBean bean) {
    Table table = model.findTable(dynaClass.getTableName());
    HashMap columnValues = toColumnValues(properties, bean);

    columnValues.putAll(toColumnValues(primaryKeys, bean));

    return _builder.getUpdateSql(table, columnValues, bean == null);
  }

  /**
   * {@inheritDoc}
   */
  public String getUpdateSql(Database model, DynaBean dynaBean) {
    SqlDynaClass dynaClass = model.getDynaClassFor(dynaBean);
    SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();

    if (primaryKeys.length == 0) {
      _log.info("Cannot update instances of type " + dynaClass + " because it has no primary keys");
      return null;
    }

    return createUpdateSql(model, dynaClass, primaryKeys, dynaClass.getNonPrimaryKeyProperties(),
        dynaBean);
  }

  private int privateupdate(Connection connection, Database model, DynaBean dynaBean)
      throws DatabaseOperationException {

    Timestamp date = getDateStatement(connection);
    SqlDynaClass dynaClass = model.getDynaClassFor(dynaBean);
    SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();
    SqlDynaProperty[] nonprimaryKeys = dynaClass.getNonPrimaryKeyProperties();

    if (primaryKeys.length == 0) {
      _log.info("Cannot update instances of type " + dynaClass
          + " because it has no primary keys (table " + dynaClass.getTableName() + ")");
      return 0;
    }

    if (nonprimaryKeys.length == 0) {
      _log.info("Cannot update instances of type " + dynaClass
          + " because it has no values to update (table " + dynaClass.getTableName() + ")");
      return 0;
    }

    SqlDynaProperty[] properties = dynaClass.getNonPrimaryKeyProperties();
    String sql = createUpdateSql(model, dynaClass, primaryKeys, properties, null);
    PreparedStatement statement = null;

    Table table = model.findTable(dynaClass.getTableName());
    if (_log.isDebugEnabled()) {
      _log.debug("About to execute SQL: " + sql);
    }
    try {
      beforeUpdate(connection, dynaClass.getTable());

      statement = connection.prepareStatement(sql);

      int sqlIndex = 1;

      for (int idx = 0; idx < properties.length; idx++) {

        if (properties[idx].getName().equalsIgnoreCase("UPDATED")) {
          setObject(statement, sqlIndex++, date, table.findColumn("UPDATED"));
        } else if (properties[idx].getName().equalsIgnoreCase("UPDATEDBY")) {
          setObject(statement, sqlIndex++, "0", table.findColumn("UPDATEDBY"));
        } else if (properties[idx].getName().equalsIgnoreCase("CREATEDBY")) {
          setObject(statement, sqlIndex++, "0", table.findColumn("CREATEDBY"));
        } else if (properties[idx].getName().equalsIgnoreCase("CREATED")) {
          setObject(statement, sqlIndex++, date, table.findColumn("CREATED"));
        } else if (table.findColumn(properties[idx].getName()) != null) {
          setObject(statement, sqlIndex++, dynaBean, properties[idx]);
        } else
          _log.debug("Rejected column: " + properties[idx].getName());
      }
      for (int idx = 0; idx < primaryKeys.length; idx++) {
        if (table.findColumn(primaryKeys[idx].getName()) != null)
          setObject(statement, sqlIndex++, dynaBean, primaryKeys[idx]);
        else
          _log.debug("Rejected column: " + properties[idx].getName());
      }

      int count = statement.executeUpdate();

      afterUpdate(connection, dynaClass.getTable());

      return count;
    } catch (SQLException ex) {
      throw new DatabaseOperationException("Error while updating in the database : "
          + ex.getMessage(), ex);
    } finally {
      closeStatement(statement);
    }
  }

  private int update(Connection connection, Database model, ColumnDataChange change)
      throws DatabaseOperationException {

    Timestamp date = getDateStatement(connection);

    HashMap map = new HashMap();
    Table table = model.findTable(change.getTablename());
    String pk = table.getPrimaryKeyColumns()[0].getName();
    map.put(pk, change.getPkRow());
    if (table.findColumn("UPDATED") != null)
      map.put("UPDATED", date);
    if (table.findColumn("UPDATEDBY") != null)
      map.put("UPDATEDBY", "O");
    map.put(change.getColumnname(), change.getNewValue());

    String sql = getSqlBuilder().getUpdateSql(table, map, true);
    // createUpdateSql(model, dynaClass, primaryKeys, properties, null);
    PreparedStatement statement = null;

    if (_log.isDebugEnabled()) {
      _log.debug("About to execute SQL: " + sql);
    }
    try {
      beforeUpdate(connection, table);

      statement = connection.prepareStatement(sql);
      int sqlIndex = 1;
      for (int i = 0; i < table.getColumnCount(); i++) {
        if (table.getColumn(i).getName().equalsIgnoreCase("UPDATED")) {
          setObject(statement, sqlIndex++, date, table.findColumn("UPDATED"));
        } else if (table.getColumn(i).getName().equalsIgnoreCase("UPDATEDBY")) {
          setObject(statement, sqlIndex++, "0", table.findColumn("UPDATEDBY"));
        } else if (table.getColumn(i).getName().equalsIgnoreCase(change.getColumnname())) {
          if (table.getColumn(i).getName().equalsIgnoreCase("CREATEDBY"))
            setObject(statement, sqlIndex++, "0", table.findColumn(change.getColumnname()));
          else if (table.getColumn(i).getName().equalsIgnoreCase("CREATED"))
            setObject(statement, sqlIndex++, date, table.findColumn(change.getColumnname()));
          else
            setObject(statement, sqlIndex++, change.getNewValue(),
                table.findColumn(change.getColumnname()));
        }
      }

      setObject(statement, sqlIndex++, change.getPkRow(), table.getPrimaryKeyColumns()[0]);
      int count = statement.executeUpdate();

      afterUpdate(connection, table);

      return count;
    } catch (SQLException ex) {
      ex.printStackTrace();
      throw new DatabaseOperationException("Error while updating in the database : "
          + ex.getMessage(), ex);
    } finally {
      closeStatement(statement);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void update(Connection connection, Database model, DynaBean dynaBean)
      throws DatabaseOperationException {
    int count = privateupdate(connection, model, dynaBean);
    if (count != 1) {
      _log.warn("Attempted to insert a single row " + dynaBean + " into table "
          + model.getDynaClassFor(dynaBean).getTableName() + " but changed " + count + " row(s)");
    }
  }

  /**
   * {@inheritDoc}
   */
  public void update(Database model, DynaBean dynaBean) throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      update(connection, model, dynaBean);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * Allows platforms to issue statements directly before rows are updated in the specified table.
   * 
   * @param connection
   *          The connection used for the update
   * @param table
   *          The table that the rows are updateed into
   */
  protected void beforeUpdate(Connection connection, Table table) throws SQLException {
  }

  /**
   * Allows platforms to issue statements directly after rows have been updated in the specified
   * table.
   * 
   * @param connection
   *          The connection used for the update
   * @param table
   *          The table that the rows have been updateed into
   */
  protected void afterUpdate(Connection connection, Table table) throws SQLException {
  }

  /**
   * Determines whether the given dyna bean is stored in the database.
   * 
   * @param dynaBean
   *          The bean
   * @param connection
   *          The connection
   * @return <code>true</code> if this dyna bean has a primary key
   */
  protected boolean exists(Connection connection, DynaBean dynaBean) {
    // TODO: check for the pk value, and if present, query against database
    return false;
  }

  /**
   * {@inheritDoc}
   */
  public void store(Database model, DynaBean dynaBean) throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      if (exists(connection, dynaBean)) {
        update(connection, model, dynaBean);
      } else {
        insert(connection, model, dynaBean);
      }
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * Creates the SQL for deleting an object of the given type. If a concrete bean is given, then a
   * concrete delete statement is created, otherwise a delete statement usable in a prepared
   * statement is build.
   * 
   * @param model
   *          The database model
   * @param dynaClass
   *          The type
   * @param primaryKeys
   *          The primary keys
   * @param bean
   *          Optionally the concrete bean to update
   * @return The SQL required to delete the instance
   */
  protected String createDeleteSql(Database model, SqlDynaClass dynaClass,
      SqlDynaProperty[] primaryKeys, DynaBean bean) {
    Table table = model.findTable(dynaClass.getTableName());
    HashMap pkValues = toColumnValues(primaryKeys, bean);

    return _builder.getDeleteSql(table, pkValues, bean == null);
  }

  /**
   * {@inheritDoc}
   */
  public String getDeleteSql(Database model, DynaBean dynaBean) {
    SqlDynaClass dynaClass = model.getDynaClassFor(dynaBean);
    SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();

    if (primaryKeys.length == 0) {
      _log.warn("Cannot delete instances of type " + dynaClass + " because it has no primary keys");
      return null;
    } else {
      return createDeleteSql(model, dynaClass, primaryKeys, dynaBean);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void delete(Database model, DynaBean dynaBean) throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      delete(connection, model, dynaBean);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void delete(Connection connection, Database model, DynaBean dynaBean)
      throws DatabaseOperationException {
    PreparedStatement statement = null;

    try {
      SqlDynaClass dynaClass = model.getDynaClassFor(dynaBean);
      SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();

      if (primaryKeys.length == 0) {
        _log.warn("Cannot delete instances of type " + dynaClass
            + " because it has no primary keys");
        return;
      }

      String sql = createDeleteSql(model, dynaClass, primaryKeys, null);

      if (_log.isDebugEnabled()) {
        _log.debug("About to execute SQL " + sql);
      }

      statement = connection.prepareStatement(sql);

      for (int idx = 0; idx < primaryKeys.length; idx++) {
        setObject(statement, idx + 1, dynaBean, primaryKeys[idx]);
      }

      int count = statement.executeUpdate();

      if (count != 1) {
        _log.warn("Attempted to delete a single row " + dynaBean + " in table "
            + dynaClass.getTableName() + " but changed " + count + " row(s).");
      }
    } catch (SQLException ex) {
      throw new DatabaseOperationException("Error while deleting from the database", ex);
    } finally {
      closeStatement(statement);
    }
  }

  public void delete(Connection connection, Database model, RemoveRowChange change)
      throws DatabaseOperationException {
    PreparedStatement statement = null;

    try {
      Table table = change.getTable();
      Column[] pk = table.getPrimaryKeyColumns();
      HashMap pkValues = new HashMap();
      pkValues.put(pk[0].getName(), change.getRow().get(pk[0].getName()).toString());
      String sql = getSqlBuilder().getDeleteSql(change.getTable(), pkValues, true);
      // createDeleteSql(model, dynaClass, primaryKeys, null);

      if (_log.isDebugEnabled()) {
        _log.debug("About to execute SQL " + sql);
      }

      statement = connection.prepareStatement(sql);
      for (int idx = 0; idx < pk.length; idx++) {
        setObject(statement, idx + 1, change.getRow().get(pk[idx].getName()).toString(), pk[idx]);
      }

      int count = statement.executeUpdate();

    } catch (SQLException ex) {
      throw new DatabaseOperationException("Error while deleting from the database", ex);
    } finally {
      closeStatement(statement);
    }
  }

  public Database loadTablesFromDatabase(ExcludeFilter filter) throws DatabaseOperationException {

    Connection connection = borrowConnection();
    try {
      getModelLoader().setLog(_log);
      getModelLoader().setOnlyLoadTableColumns(true);
      return getModelLoader().getDatabase(connection, filter);
    } catch (SQLException ex) {
      throw new DatabaseOperationException(ex);
    } finally {
      returnConnection(connection);
    }
  }

  public Database loadModelFromDatabase(ExcludeFilter filter) throws DatabaseOperationException {

    Connection connection = borrowConnection();
    try {
      getModelLoader().setLog(_log);
      return getModelLoader().getDatabase(connection, filter);
    } catch (SQLException ex) {
      throw new DatabaseOperationException(ex);
    } finally {
      returnConnection(connection);
    }
  }

  public Database loadModelFromDatabase(ExcludeFilter filter, String prefix,
      boolean loadCompleteTables, String moduleId) throws DatabaseOperationException {

    Connection connection = borrowConnection();
    try {
      getModelLoader().setLog(_log);
      return getModelLoader().getDatabase(connection, filter, prefix, loadCompleteTables, moduleId);
    } catch (SQLException ex) {
      ex.printStackTrace();
      throw new DatabaseOperationException(ex);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public Database readModelFromDatabase(String name) throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      return readModelFromDatabase(connection, name);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public Database readModelFromDatabase(Connection connection, String name)
      throws DatabaseOperationException {
    try {
      Database model = getModelReader().getDatabase(connection, name);

      postprocessModelFromDatabase(model);
      return model;
    } catch (SQLException ex) {
      throw new DatabaseOperationException(ex);
    }
  }

  /**
   * {@inheritDoc}
   */
  public Database readModelFromDatabase(String name, String catalog, String schema,
      String[] tableTypes) throws DatabaseOperationException {
    Connection connection = borrowConnection();

    try {
      return readModelFromDatabase(connection, name, catalog, schema, tableTypes);
    } finally {
      returnConnection(connection);
    }
  }

  /**
   * {@inheritDoc}
   */
  public Database readModelFromDatabase(Connection connection, String name, String catalog,
      String schema, String[] tableTypes) throws DatabaseOperationException {
    try {
      JdbcModelReader reader = getModelReader();
      Database model = reader.getDatabase(connection, name, catalog, schema, tableTypes);

      postprocessModelFromDatabase(model);
      if ((model.getName() == null) || (model.getName().length() == 0)) {
        model.setName(MODEL_DEFAULT_NAME);
      }
      return model;
    } catch (SQLException ex) {
      throw new DatabaseOperationException(ex);
    }
  }

  // /**
  // * {@inheritDoc}
  // */
  // public void deleteAllDataFromTables(Connection connection, Database
  // model, boolean continueOnError) throws DatabaseOperationException {
  // throw new DatabaseOperationException("Error: Operation not supported");
  // }

  /**
   * {@inheritDoc}
   */
  public void disableAllFK(Connection connection, Database model, boolean continueOnError)
      throws DatabaseOperationException {
    throw new DatabaseOperationException("Error: Operation not supported");
  }

  @Override
  public void disableDatasetFK(Connection connection, Database model, OBDataset dataset,
      boolean continueOnError) throws DatabaseOperationException {
    try {
      StringWriter buffer = new StringWriter();
      getSqlBuilder().setWriter(buffer);
      for (int i = 0; i < model.getTableCount(); i++) {
        Table table = model.getTable(i);
        if (dataset.getTable(table.getName()) != null) {
          for (int idx = 0; idx < table.getForeignKeyCount(); idx++) {
            getSqlBuilder().writeExternalForeignKeyDropStmt(table, table.getForeignKey(idx));
          }
        } else {
          for (int j = 0; j < table.getForeignKeyCount(); j++) {
            ForeignKey fk = table.getForeignKey(j);
            if (dataset.getTable(fk.getForeignTableName()) != null) {
              getSqlBuilder().writeExternalForeignKeyDropStmt(table, fk);
            }
          }
        }
      }
      evaluateBatchRealBatch(connection, buffer.toString(), continueOnError);
    } catch (Exception e) {
      e.printStackTrace();
      throw new DatabaseOperationException("Error while disabling foreign key ", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void disableAllTriggers(Connection connection, Database model, boolean continueOnError)
      throws DatabaseOperationException {
    throw new DatabaseOperationException("Error: Operation not supported");
  }

  /**
   * {@inheritDoc}
   */
  public boolean enableAllFK(Connection connection, Database model, boolean continueOnError)
      throws DatabaseOperationException {
    throw new DatabaseOperationException("Error: Operation not supported");
  }

  @Override
  public boolean enableDatasetFK(Connection connection, Database model, OBDataset dataset,
      boolean continueOnError) throws DatabaseOperationException {
    try {
      StringWriter buffer = new StringWriter();
      getSqlBuilder().setWriter(buffer);
      for (int i = 0; i < model.getTableCount(); i++) {
        Table table = model.getTable(i);
        if (dataset.getTable(table.getName()) != null) {
          getSqlBuilder().createExternalForeignKeys(model, table);
        } else {
          for (int j = 0; j < table.getForeignKeyCount(); j++) {
            ForeignKey fk = table.getForeignKey(j);
            if (dataset.getTable(fk.getForeignTableName()) != null) {
              getSqlBuilder().writeExternalForeignKeyCreateStmt(model, table, fk);
            }
          }
        }
      }
      int numErrors = evaluateBatchRealBatch(connection, buffer.toString(), continueOnError);
      if (numErrors > 0) {
        return false;
      }
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      throw new DatabaseOperationException("Error while enabling foreign key ", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  public boolean enableAllTriggers(Connection connection, Database model, boolean continueOnError)
      throws DatabaseOperationException {
    throw new DatabaseOperationException("Error: Operation not supported");
  }

  public void disableAllFK(Database model, boolean continueOnError, Writer writer)
      throws DatabaseOperationException {
    throw new DatabaseOperationException("Error: Operation not supported");
  }

  public void disableAllTriggers(Database model, boolean continueOnError, Writer writer)
      throws DatabaseOperationException {
    throw new DatabaseOperationException("Error: Operation not supported");
  }

  public void enableAllFK(Database model, boolean continueOnError, Writer writer)
      throws DatabaseOperationException {
    throw new DatabaseOperationException("Error: Operation not supported");
  }

  public void enableAllTriggers(Database model, boolean continueOnError, Writer writer)
      throws DatabaseOperationException {
    throw new DatabaseOperationException("Error: Operation not supported");
  }

  /**
   * {@inheritDoc}
   */
  public void deleteDataFromTable(Connection connection, Database model, Table table,
      boolean continueOnError) {
    String sql = getDeleteDataFromTableSql(model, table, continueOnError);
    evaluateBatch(connection, sql, continueOnError);
  }

  /**
   * {@inheritDoc}
   */
  public void deleteDataFromTable(Connection connection, Database model, String table,
      String sqlfilter, boolean continueOnError) {
    String sql = getDeleteDataFromTableSql(model, table, sqlfilter, continueOnError);
    evaluateBatch(connection, sql, continueOnError);
  }

  public void deleteDataFromTable(Connection connection, Database model, String[] tables,
      String[] sqlfilters, boolean continueOnError) {
    StringWriter buffer = new StringWriter();
    getSqlBuilder().setWriter(buffer);
    for (int i = 0; i < tables.length; i++) {
      try {
        getSqlBuilder().writeDeleteTable(model, tables[i], sqlfilters[i]);

      } catch (IOException e) {
        _log.error(e.getLocalizedMessage());
      }
    }
    evaluateBatch(connection, buffer.toString(), continueOnError);
  }

  /**
   * Allows the platform to postprocess the model just read from the database.
   * 
   * @param model
   *          The model
   */
  protected void postprocessModelFromDatabase(Database model) {
    // Default values for CHAR/VARCHAR/LONGVARCHAR columns have quotation
    // marks
    // around them which we'll remove now
    for (int tableIdx = 0; tableIdx < model.getTableCount(); tableIdx++) {
      Table table = model.getTable(tableIdx);

      for (int columnIdx = 0; columnIdx < table.getColumnCount(); columnIdx++) {
        Column column = table.getColumn(columnIdx);

        if (TypeMap.isTextType(column.getTypeCode())
            || TypeMap.isDateTimeType(column.getTypeCode())) {
          String defaultValue = column.getDefaultValue();

          if ((defaultValue != null) && (defaultValue.length() >= 2)
              && defaultValue.startsWith("'") && defaultValue.endsWith("'")) {
            defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
            column.setDefaultValue(defaultValue);
          }
        }
      }
    }
  }

  /**
   * Derives the column values for the given dyna properties from the dyna bean.
   * 
   * @param properties
   *          The properties
   * @param bean
   *          The bean
   * @return The values indexed by the column names
   */
  protected HashMap toColumnValues(SqlDynaProperty[] properties, DynaBean bean) {
    HashMap result = new HashMap();

    for (int idx = 0; idx < properties.length; idx++) {
      result.put(properties[idx].getName(),
          bean == null ? null : bean.get(properties[idx].getName()));
    }
    return result;
  }

  /**
   * Sets a parameter of the prepared statement based on the type of the column of the property.
   * 
   * @param statement
   *          The statement
   * @param sqlIndex
   *          The index of the parameter to set in the statement
   * @param dynaBean
   *          The bean of which to take the value
   * @param property
   *          The property of the bean, which also defines the corresponding column
   */
  protected void setObject(PreparedStatement statement, int sqlIndex, DynaBean dynaBean,
      SqlDynaProperty property) throws SQLException {
    int typeCode = property.getColumn().getTypeCode();
    Object value = dynaBean.get(property.getName());
    if (typeCode == Types.CLOB) {
      if (value == null)
        statement.setCharacterStream(sqlIndex, null, 0);
      else
        statement.setCharacterStream(sqlIndex, new StringReader(value.toString()), value.toString()
            .length());
    } else {
      setStatementParameterValue(statement, sqlIndex, typeCode, value);
    }
  }

  protected void setObject(PreparedStatement statement, int sqlIndex, Object value, Column column)
      throws SQLException {
    int typeCode = column.getTypeCode();

    setStatementParameterValue(statement, sqlIndex, typeCode, value);
  }

  /**
   * This is the core method to set the parameter of a prepared statement to a given value. The
   * primary purpose of this method is to call the appropriate method on the statement, and to give
   * database-specific implementations the ability to change this behavior.
   * 
   * @param statement
   *          The statement
   * @param sqlIndex
   *          The parameter index
   * @param typeCode
   *          The JDBC type code
   * @param value
   *          The value
   * @throws SQLException
   *           If an error occurred while setting the parameter value
   */
  protected void setStatementParameterValue(PreparedStatement statement, int sqlIndex,
      int typeCode, Object value) throws SQLException {
    // Downgrade typecode
    if (typeCode == ExtTypes.NCHAR)
      typeCode = Types.CHAR;
    if (typeCode == ExtTypes.NVARCHAR)
      typeCode = Types.VARCHAR;
    if (typeCode == Types.CLOB) {
      if (value == null)
        statement.setCharacterStream(sqlIndex, null, 0);
      else
        statement.setCharacterStream(sqlIndex, new StringReader(value.toString()), value.toString()
            .length());
    } else if (typeCode == Types.BLOB) {
      byte[] b = (byte[]) value;
      InputStream isr = new ByteArrayInputStream(b);
      statement.setBinaryStream(sqlIndex, isr, b.length);
    } else {
      statement.setObject(sqlIndex, value, typeCode);
    }
  }

  /**
   * Helper method esp. for the {@link ModelBasedResultSetIterator} class that retrieves the value
   * for a column from the given result set. If a table was specified, and it contains the column,
   * then the jdbc type defined for the column is used for extracting the value, otherwise the
   * object directly retrieved from the result set is returned.<br/>
   * The method is defined here rather than in the {@link ModelBasedResultSetIterator} class so that
   * concrete platforms can modify its behavior.
   * 
   * @param resultSet
   *          The result set
   * @param columnName
   *          The name of the column
   * @param table
   *          The table
   * @return The value
   */
  protected Object getObjectFromResultSet(ResultSet resultSet, String columnName, Table table)
      throws SQLException {
    Column column = (table == null ? null : table.findColumn(columnName,
        isDelimitedIdentifierModeOn()));
    Object value = null;

    if (column != null) {
      int originalJdbcType = column.getTypeCode();
      int targetJdbcType = getPlatformInfo().getTargetJdbcType(originalJdbcType);
      int jdbcType = originalJdbcType;

      // in general we're trying to retrieve the value using the original
      // type
      // but sometimes we also need the target type:
      if ((originalJdbcType == Types.BLOB) && (targetJdbcType != Types.BLOB)) {
        // we should not use the Blob interface if the database doesn't
        // map to this type
        jdbcType = targetJdbcType;
      }
      if ((originalJdbcType == Types.CLOB) && (targetJdbcType != Types.CLOB)) {
        // we should not use the Clob interface if the database doesn't
        // map to this type
        jdbcType = targetJdbcType;
      }
      value = extractColumnValue(resultSet, columnName, 0, jdbcType);
    } else {
      value = resultSet.getObject(columnName);
    }
    return resultSet.wasNull() ? null : value;
  }

  /**
   * Helper method for retrieving the value for a column from the given result set using the type
   * code of the column.
   * 
   * @param resultSet
   *          The result set
   * @param column
   *          The column
   * @param idx
   *          The value's index in the result set (starting from 1)
   * @return The value
   */
  protected Object getObjectFromResultSet(ResultSet resultSet, Column column, int idx)
      throws SQLException {
    int originalJdbcType = column.getTypeCode();
    int targetJdbcType = getPlatformInfo().getTargetJdbcType(originalJdbcType);
    int jdbcType = originalJdbcType;
    Object value = null;

    // in general we're trying to retrieve the value using the original type
    // but sometimes we also need the target type:
    if ((originalJdbcType == Types.BLOB) && (targetJdbcType != Types.BLOB)) {
      // we should not use the Blob interface if the database doesn't map
      // to this type
      jdbcType = targetJdbcType;
    }
    if ((originalJdbcType == Types.CLOB) && (targetJdbcType != Types.CLOB)) {
      // we should not use the Clob interface if the database doesn't map
      // to this type
      jdbcType = targetJdbcType;
    }
    value = extractColumnValue(resultSet, null, idx, jdbcType);
    return resultSet.wasNull() ? null : value;
  }

  /**
   * This is the core method to retrieve a value for a column from a result set. Its primary purpose
   * is to call the appropriate method on the result set, and to provide an extension point where
   * database-specific implementations can change this behavior.
   * 
   * @param resultSet
   *          The result set to extract the value from
   * @param columnName
   *          The name of the column; can be <code>null</code> in which case the
   *          <code>columnIdx</code> will be used instead
   * @param columnIdx
   *          The index of the column's value in the result set; is only used if
   *          <code>columnName</code> is <code>null</code>
   * @param jdbcType
   *          The jdbc type to extract
   * @return The value
   * @throws SQLException
   *           If an error occurred while accessing the result set
   */
  protected Object extractColumnValue(ResultSet resultSet, String columnName, int columnIdx,
      int jdbcType) throws SQLException {
    boolean useIdx = (columnName == null);
    Object value;

    switch (jdbcType) {
    case Types.CHAR:
    case Types.VARCHAR:
    case Types.LONGVARCHAR:
      value = useIdx ? resultSet.getString(columnIdx) : resultSet.getString(columnName);
      break;
    case Types.NUMERIC:
    case Types.DECIMAL:
      value = useIdx ? resultSet.getBigDecimal(columnIdx) : resultSet.getBigDecimal(columnName);
      break;
    case Types.BIT:
      value = new Boolean(useIdx ? resultSet.getBoolean(columnIdx)
          : resultSet.getBoolean(columnName));
      break;
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
      value = new Integer(useIdx ? resultSet.getInt(columnIdx) : resultSet.getInt(columnName));
      break;
    case Types.BIGINT:
      value = new Long(useIdx ? resultSet.getLong(columnIdx) : resultSet.getLong(columnName));
      break;
    case Types.REAL:
      value = new Float(useIdx ? resultSet.getFloat(columnIdx) : resultSet.getFloat(columnName));
      break;
    case Types.FLOAT:
    case Types.DOUBLE:
      value = new Double(useIdx ? resultSet.getDouble(columnIdx) : resultSet.getDouble(columnName));
      break;
    case Types.BINARY:
    case Types.VARBINARY:
    case Types.LONGVARBINARY:
      value = useIdx ? resultSet.getBytes(columnIdx) : resultSet.getBytes(columnName);
      break;
    case Types.DATE:
      value = useIdx ? resultSet.getDate(columnIdx) : resultSet.getDate(columnName);
      break;
    case Types.TIME:
      value = useIdx ? resultSet.getTime(columnIdx) : resultSet.getTime(columnName);
      break;
    case Types.TIMESTAMP:
      value = useIdx ? resultSet.getTimestamp(columnIdx) : resultSet.getTimestamp(columnName);
      break;
    case Types.CLOB:
      Clob clob = useIdx ? resultSet.getClob(columnIdx) : resultSet.getClob(columnName);

      if (clob == null) {
        value = null;
      } else {
        long length = clob.length();

        if (length > Integer.MAX_VALUE) {
          value = clob;
        } else if (length == 0) {
          // the javadoc is not clear about whether Clob.getSubString
          // can be used with a substring length of 0
          // thus we do the safe thing and handle it ourselves
          value = "";
        } else {
          value = clob.getSubString(1l, (int) length);
        }
      }
      break;
    case Types.BLOB:
      Blob blob = useIdx ? resultSet.getBlob(columnIdx) : resultSet.getBlob(columnName);

      if (blob == null) {
        value = null;
      } else {
        long length = blob.length();

        if (length > Integer.MAX_VALUE) {
          value = blob;
        } else if (length == 0) {
          // the javadoc is not clear about whether Blob.getBytes
          // can be used with for 0 bytes to be copied
          // thus we do the safe thing and handle it ourselves
          value = new byte[0];
        } else {
          value = blob.getBytes(1l, (int) length);
        }
      }
      break;
    case Types.ARRAY:
      value = useIdx ? resultSet.getArray(columnIdx) : resultSet.getArray(columnName);
      break;
    case Types.REF:
      value = useIdx ? resultSet.getRef(columnIdx) : resultSet.getRef(columnName);
      break;
    default:
      // special handling for Java 1.4/JDBC 3 types
      if (Jdbc3Utils.supportsJava14JdbcTypes()
          && (jdbcType == Jdbc3Utils.determineBooleanTypeCode())) {
        value = new Boolean(useIdx ? resultSet.getBoolean(columnIdx)
            : resultSet.getBoolean(columnName));
      } else {
        value = useIdx ? resultSet.getObject(columnIdx) : resultSet.getObject(columnName);
      }
      break;
    }
    return resultSet.wasNull() ? null : value;
  }

  /**
   * Creates an iterator over the given result set.
   * 
   * @param model
   *          The database model
   * @param resultSet
   *          The result set to iterate over
   * @param queryHints
   *          The tables that were queried in the query that produced the given result set
   *          (optional)
   * @return The iterator
   */
  public ModelBasedResultSetIterator createResultSetIterator(Database model, ResultSet resultSet,
      Table[] queryHints) {
    return new ModelBasedResultSetIterator(this, model, resultSet, queryHints, true);
  }

  protected String getDeleteDataFromTableSql(Database model, Table table, boolean continueOnError) {

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().writeDeleteTable(model, table);
      return buffer.toString();
    } catch (IOException e) {
      return null; // won't happen because we're using a string writer
    }
  }

  protected String getDeleteDataFromTableSql(Database model, String table, String sqlfilter,
      boolean continueOnError) {

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().writeDeleteTable(model, table, sqlfilter);
      return buffer.toString();
    } catch (IOException e) {
      return null; // won't happen because we're using a string writer
    }
  }

  public void deleteInvalidConstraintRows(Database model, OBDataset dataset, boolean continueOnError) {

    Connection connection = borrowConnection();
    deleteInvalidConstraintRows(connection, model, dataset, continueOnError);
    returnConnection(connection);

  }

  public void deleteInvalidConstraintRows(Connection connection, Database model, OBDataset dataset,
      boolean continueOnError) {

    StringWriter buffer = new StringWriter();

    getSqlBuilder().setWriter(buffer);
    getSqlBuilder().deleteInvalidConstraintRows(model, dataset, true);
    evaluateBatch(connection, buffer.toString(), continueOnError);
  }

  public void deleteAllInvalidConstraintRows(Database model, boolean continueOnError) {

    Connection connection = borrowConnection();
    deleteAllInvalidConstraintRows(connection, model, continueOnError);
    returnConnection(connection);

  }

  public void deleteAllInvalidConstraintRows(Connection connection, Database model,
      boolean continueOnError) {

    StringWriter buffer = new StringWriter();

    getSqlBuilder().setWriter(buffer);
    getSqlBuilder().deleteInvalidConstraintRows(model, null, false);
    evaluateBatch(connection, buffer.toString(), continueOnError);
  }

  public void applyConfigScript(Database database, Vector<Change> changes) {
    StringWriter buffer = new StringWriter();

    getSqlBuilder().setWriter(buffer);
    try {
      getSqlBuilder().getConfigScript(database, changes);
      Connection connection = borrowConnection();
      evaluateBatch(connection, buffer.toString(), true);
      connection.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void insertNonModuleTablesFromXML(Database loadedDatabase, Database correctDatabase) {
    Connection connection = borrowConnection();
    for (int i = 0; i < correctDatabase.getTableCount(); i++) {
      if (loadedDatabase.findTable(correctDatabase.getTable(i).getName()) == null) {
        // Table doesn't exist in the loaded database model. This is right if the module is adding a
        // table,
        // but is wrong if it's just adding a column to the table. We need to check

        this.getModelLoader().addAdditionalTableIfExists(connection, loadedDatabase,
            correctDatabase.getTable(i).getName());
      }
    }
    returnConnection(connection);
  }

  public void insertNonModuleTablesFromDatabase(Database loadedDatabase, Database fullXMLDatabase,
      Database filteredDatabase) {
    for (int i = 0; i < loadedDatabase.getTableCount(); i++) {
      if (filteredDatabase.findTable(loadedDatabase.getTable(i).getName()) == null) {
        // There is a table in the database that doesn't exist in the filtered xml model. We'll
        // search for it in the
        // full xml model
        if (fullXMLDatabase.findTable(loadedDatabase.getTable(i).getName()) != null)
          filteredDatabase
              .addTable(fullXMLDatabase.findTable(loadedDatabase.getTable(i).getName()));
      }
    }

  }

  public void insertFunctionsInBothModels(Database loadedDatabase, Database fullXMLDatabase,
      Database filteredDatabase) {
    try {
      for (int i = 0; i < fullXMLDatabase.getFunctionCount(); i++) {
        Function func = fullXMLDatabase.getFunction(i);
        Function func1 = (Function) func.clone();
        Function func2 = (Function) func.clone();
        if (loadedDatabase.findFunction(func.getName()) == null
            && filteredDatabase.findFunction(func.getName()) == null) {
          loadedDatabase.addFunction(func1);
          filteredDatabase.addFunction(func2);
        }
      }
      for (int i = 0; i < fullXMLDatabase.getTriggerCount(); i++) {
        Trigger trigger = fullXMLDatabase.getTrigger(i);
        Trigger trigger1 = (Trigger) trigger.clone();
        Trigger trigger2 = (Trigger) trigger.clone();
        if (loadedDatabase.findTrigger(trigger.getName()) == null
            && filteredDatabase.findTrigger(trigger.getName()) == null) {
          loadedDatabase.addTrigger(trigger1);
          filteredDatabase.addTrigger(trigger2);
        }
      }
    } catch (Exception e) {
      // won't happen
    }
  }

  public void insertViewsInBothModels(Database loadedDatabase, Database fullXMLDatabase,
      Database filteredDatabase) {
    try {
      for (int i = 0; i < fullXMLDatabase.getViewCount(); i++) {
        View func = fullXMLDatabase.getView(i);
        View func1 = (View) func.clone();
        View func2 = (View) func.clone();
        if (loadedDatabase.findView(func.getName()) == null
            && filteredDatabase.findView(func.getName()) == null) {
          loadedDatabase.addView(func1);
          filteredDatabase.addView(func2);
        }
      }
    } catch (Exception e) {
      // won't happen
    }
  }

  public void removeDeletedFKTriggers(Database modifiedDatabase, Database fullXMLDatabase) {
    for (int i = 0; i < fullXMLDatabase.getTableCount(); i++) {
      Table table = fullXMLDatabase.getTable(i);
      Table table2 = modifiedDatabase.findTable(table.getName());
      if (table2 != null) {
        ForeignKey[] fks = table.getForeignKeys();
        for (int j = 0; j < fks.length; j++) {
          if (table2.findForeignKey(fks[j]) == null)
            table.removeForeignKey(fks[j]);
        }
      }
    }

    Trigger[] triggers = fullXMLDatabase.getTriggers();
    for (int i = 0; i < triggers.length; i++) {
      Trigger trigger = triggers[i];
      Trigger trigger2 = modifiedDatabase.findTrigger(trigger.getName());
      if (trigger2 == null && modifiedDatabase.findTable(trigger.getTable()) != null) {
        fullXMLDatabase.removeTrigger(trigger);
      }

    }
  }

  public void disableNOTNULLColumns(Database database) {
    disableNOTNULLColumns(database, null);
  }

  public void disableNOTNULLColumns(Database database, OBDataset dataset) {

    Connection connection = borrowConnection();
    try {
      evaluateBatch(connection, disableNOTNULLColumnsSql(database, dataset), true);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      returnConnection(connection);
    }
  }

  public String disableNOTNULLColumnsSql(Database database, OBDataset dataset) {
    StringWriter buffer = new StringWriter();

    getSqlBuilder().setWriter(buffer);
    for (int i = 0; i < database.getTableCount(); i++) {
      Table table = database.getTable(i);
      boolean enable = false;
      if (dataset == null) {
        enable = true;
      } else {
        for (OBDatasetTable dsTable : dataset.getTableList()) {
          if (dsTable.getName().equalsIgnoreCase(table.getName())) {
            enable = true;
          }
        }
      }
      if (enable) {
        _log.debug("disabling notnulls for table " + table.getName());
        try {
          getSqlBuilder().disableAllNOTNULLColumns(database.getTable(i));
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return buffer.toString();
  }

  public void disableCheckConstraints(Database database) {
    disableCheckConstraints(database, null);
  }

  public void disableCheckConstraints(Database database, OBDataset dataset) {
    Connection connection = borrowConnection();
    try {
      disableCheckConstraints(connection, database, dataset);
    } finally {
      returnConnection(connection);
    }
  }

  public void disableCheckConstraints(Connection connection, Database database, OBDataset dataset) {

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      for (int i = 0; i < database.getTableCount(); i++) {
        Table table = database.getTable(i);
        boolean enable = false;
        if (dataset == null) {
          enable = true;
        } else {
          for (OBDatasetTable dsTable : dataset.getTableList()) {
            if (dsTable.getName().equalsIgnoreCase(table.getName())) {
              enable = true;
            }
          }
        }
        if (enable) {
          _log.debug("disabling check constraints for table " + table.getName());
          getSqlBuilder().disableAllChecks(database.getTable(i));
        }
      }
      evaluateBatchRealBatch(connection, buffer.toString(), true);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void enableNOTNULLColumns(Database database, OBDataset dataset) {

    Connection connection = borrowConnection();

    try {
      evaluateBatch(connection, enableNOTNULLColumnsSql(database, dataset), true);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      returnConnection(connection);
    }
  }

  public String enableNOTNULLColumnsSql(Database database, OBDataset dataset) {
    StringWriter buffer = new StringWriter();

    List<String> completelyEnabledTables = new ArrayList<String>();

    getSqlBuilder().setWriter(buffer);
    for (int i = 0; i < database.getTableCount(); i++) {
      Table table = database.getTable(i);
      boolean enable = false;
      if (dataset == null) {
        enable = true;
      } else {
        for (OBDatasetTable dsTable : dataset.getTableList()) {
          if (dsTable.getName().equalsIgnoreCase(table.getName())) {
            enable = true;
          }
        }
        if (!enable) {
          for (String recTable : getSqlBuilder().recreatedTables) {
            if (recTable.equalsIgnoreCase(table.getName())) {
              enable = true;
            }
          }
        }
      }
      if (enable) {
        _log.debug("enabling not nulls for table " + table.getName());
        completelyEnabledTables.add(table.getName());
        try {
          getSqlBuilder().enableAllNOTNULLColumns(table);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    Vector<AddColumnChange> columnsToSetNull = new Vector<AddColumnChange>();
    for (AddColumnChange deferredNullColumn : database.getDeferedNotNulls()) {
      if (completelyEnabledTables.contains(deferredNullColumn.getChangedTable().getName())) {
        // already set
        continue;
      }
      columnsToSetNull.add(deferredNullColumn);
    }

    for (AddColumnChange deferredDefault : database.getDeferredOnCreateDefault()) {
      try {
        getSqlBuilder().addDefault(deferredDefault);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    try {
      getSqlBuilder().enableNOTNULLColumns(columnsToSetNull);
    } catch (IOException e) {
      // TODO: handle this properly and other cases above
      e.printStackTrace();
    }
    return buffer.toString();
  }

  public void enableNOTNULLColumns(Database database) {
    enableNOTNULLColumns(database, null);

  }

  public void enableCheckConstraints(Database database, OBDataset dataset) {
    Connection connection = borrowConnection();
    try {
      enableCheckConstraints(connection, database, dataset);
    } finally {
      returnConnection(connection);
    }
  }

  public void enableCheckConstraints(Connection connection, Database database, OBDataset dataset) {

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      for (int i = 0; i < database.getTableCount(); i++) {
        Table table = database.getTable(i);
        boolean enable = false;
        if (dataset == null) {
          enable = true;
        } else {
          for (OBDatasetTable dsTable : dataset.getTableList()) {
            if (dsTable.getName().equalsIgnoreCase(table.getName())) {
              enable = true;
            }
          }
          if (!enable) {
            for (String recTable : getSqlBuilder().recreatedTables) {
              if (recTable.equalsIgnoreCase(table.getName())) {
                enable = true;
              }
            }
          }
        }
        if (enable) {
          _log.debug("enabling check constraints for table " + table.getName());
          getSqlBuilder().enableAllChecks(table);
        }
      }
      evaluateBatchRealBatch(connection, buffer.toString(), true);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void enableCheckConstraints(Database database) {
    enableCheckConstraints(database, null);

  }

  @Override
  public void executeOnCreateDefaultForMandatoryColumns(Database database, OBDataset ad) {
    Connection connection = borrowConnection();

    try {
      StringWriter buffer = new StringWriter();

      getSqlBuilder().setWriter(buffer);
      for (int i = 0; i < database.getTableCount(); i++) {
        Table table = database.getTable(i);

        // onCreateDefault are executed always for AD tables to cover the case where AD is older
        // than new column definition, then onCreateDefault should be executed.
        // On install.source ad is null, we execute onCreateDefault always in this case
        boolean isADTable = ad == null || ad.getTable(table.getName()) != null;
        for (int j = 0; j < table.getColumnCount(); j++) {
          Column column = table.getColumn(j);
          if (!isADTable && column.isRequired() && column.isSameDefaultAndOCD()) {
            continue;
          }

          if ((isADTable || database.isNewColumn(table, column)) && column.isRequired()
              && column.getOnCreateDefault() != null) {
            if (validateOnCreateDefault(connection, column.getOnCreateDefault(), table)) {
              getSqlBuilder().executeOnCreateDefault(table, null, column, false, isADTable);
            }
          }
        }
      }
      evaluateBatch(connection, buffer.toString(), true);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      returnConnection(connection);
    }
  }

  public String limitOneRow() {
    throw new UnsupportedOperationException(
        "limitOneRow method not yet implemented for this platform: " + getName());
  }

  public boolean validateOnCreateDefault(Connection connection, String onCreateDefault, Table table) {
    // short-circuit the most common/simple expression to skip execution of the validation-sql
    if (onCreateDefault.startsWith("'") || onCreateDefault.equals("get_uuid()")
        || onCreateDefault.equals("0")) {
      return true;
    }

    try {
      PreparedStatement st = connection.prepareStatement("SELECT (" + onCreateDefault + ") FROM "
          + table.getName() + limitOneRow());
      st.executeQuery();
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  public ArrayList checkTranslationConsistency(Database database, Database fullDatabase) {
    return new ArrayList();
  }

  protected void printDiff(String s1, String s2) {
    getLog().warn("********************************************************");
    diff_match_patch diffClass = new diff_match_patch();
    s1 = s1.replaceAll("\r\n", "\n");
    s2 = s2.replaceAll("\r\n", "\n");
    LinkedList<Diff> diffs = diffClass.diff_main(s1, s2);
    boolean initial = true;
    String fullDiff = "";
    for (Diff diff : diffs) {
      if (diff.operation.equals(Operation.EQUAL)) {
        String[] lines = diff.text.split("\n");
        if (lines.length > 0) {
          if (lines.length == 1) {
            fullDiff += lines[0];
          } else {
            if (initial) {
              initial = false;
              if (lines.length > 1)
                fullDiff += lines[lines.length - 2] + "\n";
              fullDiff += lines[lines.length - 1];
            } else {
              initial = true;
              fullDiff += "\n" + lines[0] + "\n";
              if (lines.length > 1)
                fullDiff += lines[1] + "\n";
            }
          }
        } else {
          fullDiff += diff.text;
        }
      } else if (diff.operation.equals(Operation.INSERT)) {
        fullDiff += "[" + diff.text + "+]";
      } else if (diff.operation.equals(Operation.DELETE)) {
        fullDiff += "[" + diff.text + "]";
      }
    }
    getLog().warn(fullDiff);
    getLog().warn("********************************************************");
  }

  @Override
  public boolean areWarnsIgnored() {
    return _ignoreWarns;
  }

  @Override
  public void setBatchEvaluator(SQLBatchEvaluator batchEvaluator) {
    this.batchEvaluator = batchEvaluator;
  }

  @Override
  public SQLBatchEvaluator getBatchEvaluator() {
    return batchEvaluator;
  }
}
