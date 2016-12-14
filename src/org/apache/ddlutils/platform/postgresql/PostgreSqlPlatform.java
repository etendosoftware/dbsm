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

import java.io.StringWriter;
import java.io.Writer;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.beanutils.DynaBean;
import org.apache.ddlutils.DatabaseOperationException;
import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.dynabean.SqlDynaProperty;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.StructureObject;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.PGStandardBatchEvaluator;
import org.apache.ddlutils.platform.PlatformImplBase;
import org.apache.ddlutils.util.ExtTypes;
import org.openbravo.ddlutils.util.OBDataset;

/**
 * The platform implementation for PostgresSql.
 * 
 * @version $Revision: 231306 $
 */
public class PostgreSqlPlatform extends PlatformImplBase {
  /** Database name of this platform. */
  public static final String DATABASENAME = "PostgreSql";
  /** The standard PostgreSQL jdbc driver. */
  public static final String JDBC_DRIVER = "org.postgresql.Driver";
  /** The subprotocol used by the standard PostgreSQL driver. */
  public static final String JDBC_SUBPROTOCOL = "postgresql";

  /**
   * Creates a new platform instance.
   */
  public PostgreSqlPlatform() {
    PlatformInfo info = getPlatformInfo();

    // this is the default length though it might be changed when building
    // PostgreSQL
    // in file src/include/postgres_ext.h
    info.setMaxIdentifierLength(30);

    info.addNativeTypeMapping(Types.CHAR, "CHAR");
    info.addNativeTypeMapping(ExtTypes.NCHAR, "CHAR");
    info.addNativeTypeMapping(Types.ARRAY, "BYTEA", Types.LONGVARBINARY);
    info.addNativeTypeMapping(Types.BINARY, "BYTEA", Types.LONGVARBINARY);
    info.addNativeTypeMapping(Types.BIT, "BOOLEAN");
    // info.addNativeTypeMapping(Types.BLOB, "BYTEA", Types.LONGVARBINARY);
    info.addNativeTypeMapping(Types.BLOB, "BYTEA", Types.LONGVARBINARY);
    info.addNativeTypeMapping(Types.CLOB, "TEXT", Types.LONGVARCHAR);
    info.addNativeTypeMapping(Types.DATE, "DATE");
    info.addNativeTypeMapping(Types.DECIMAL, "NUMERIC", Types.NUMERIC);
    info.addNativeTypeMapping(Types.DISTINCT, "BYTEA", Types.LONGVARBINARY);
    info.addNativeTypeMapping(Types.DOUBLE, "DOUBLE PRECISION");
    info.addNativeTypeMapping(Types.FLOAT, "DOUBLE PRECISION", Types.DOUBLE);
    info.addNativeTypeMapping(Types.JAVA_OBJECT, "BYTEA", Types.LONGVARBINARY);
    info.addNativeTypeMapping(Types.LONGVARBINARY, "BYTEA");
    info.addNativeTypeMapping(Types.LONGVARCHAR, "TEXT", Types.LONGVARCHAR);
    info.addNativeTypeMapping(Types.NULL, "BYTEA", Types.LONGVARBINARY);
    info.addNativeTypeMapping(Types.NUMERIC, "NUMERIC");
    info.addNativeTypeMapping(Types.OTHER, "OID", Types.OTHER);
    info.addNativeTypeMapping(Types.REF, "BYTEA", Types.LONGVARBINARY);
    info.addNativeTypeMapping(Types.STRUCT, "BYTEA", Types.LONGVARBINARY);
    info.addNativeTypeMapping(Types.TIME, "TIME");
    info.addNativeTypeMapping(Types.TIMESTAMP, "TIMESTAMP");
    info.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.SMALLINT);
    info.addNativeTypeMapping(Types.VARBINARY, "BYTEA", Types.LONGVARBINARY);
    info.addNativeTypeMapping(Types.VARCHAR, "VARCHAR");
    info.addNativeTypeMapping(ExtTypes.NVARCHAR, "VARCHAR");

    info.addNativeTypeMapping("BOOLEAN", "BOOLEAN", "BIT");
    info.addNativeTypeMapping("DATALINK", "BYTEA", "LONGVARBINARY");

    info.setDefaultSize(Types.CHAR, 254);
    info.setDefaultSize(Types.VARCHAR, 254);

    // no support for specifying the size for these types (because they are
    // mapped
    // to BYTEA which back-maps to BLOB)
    info.setHasSize(Types.BINARY, false);
    info.setHasSize(Types.VARBINARY, false);

    info.setNcharsupported(true);
    info.setOperatorClassesSupported(true);
    info.setPartialIndexesSupported(true);
    info.setContainsSearchIndexesSupported(true);

    info.setColumnOrderManaged(false);

    setSqlBuilder(new PostgreSqlBuilder(this));
    setModelReader(new PostgreSqlModelReader(this));
    setModelLoader(new PostgreSqlModelLoader());
    setBatchEvaluator(new PGStandardBatchEvaluator(this));
  }

  /**
   * {@inheritDoc}
   */
  public String getName() {
    return DATABASENAME;
  }

  /**
   * Creates or drops the database referenced by the given connection url.
   * 
   * @param jdbcDriverClassName
   *          The jdbc driver class name
   * @param connectionUrl
   *          The url to connect to the database if it were already created
   * @param username
   *          The username for creating the database
   * @param password
   *          The password for creating the database
   * @param parameters
   *          Additional parameters for the operation
   * @param createDb
   *          Whether to create or drop the database
   */
  private void createOrDropDatabase(String jdbcDriverClassName, String connectionUrl,
      String username, String password, Map parameters, boolean createDb)
      throws DatabaseOperationException, UnsupportedOperationException {
    if (JDBC_DRIVER.equals(jdbcDriverClassName)) {
      int slashPos = connectionUrl.lastIndexOf('/');

      if (slashPos < 0) {
        throw new DatabaseOperationException("Cannot parse the given connection url "
            + connectionUrl);
      }

      int paramPos = connectionUrl.lastIndexOf('?');
      String baseDb = connectionUrl.substring(0, slashPos + 1) + "template1";
      String dbName = (paramPos > slashPos ? connectionUrl.substring(slashPos + 1, paramPos)
          : connectionUrl.substring(slashPos + 1));
      Connection connection = null;
      Statement stmt = null;
      StringBuffer sql = new StringBuffer();

      sql.append(createDb ? "CREATE" : "DROP");
      sql.append(" DATABASE ");
      sql.append(dbName);
      if ((parameters != null) && !parameters.isEmpty()) {
        for (Iterator it = parameters.entrySet().iterator(); it.hasNext();) {
          Map.Entry entry = (Map.Entry) it.next();

          sql.append(" ");
          sql.append(entry.getKey().toString());
          if (entry.getValue() != null) {
            sql.append(" ");
            sql.append(entry.getValue().toString());
          }
        }
      }
      if (getLog().isDebugEnabled()) {
        getLog().debug(
            "About to create database via " + baseDb + " using this SQL: " + sql.toString());
      }
      try {
        Class.forName(jdbcDriverClassName);

        connection = DriverManager.getConnection(baseDb, username, password);
        stmt = connection.createStatement();
        stmt.execute(sql.toString());
        logWarnings(connection);
      } catch (Exception ex) {
        throw new DatabaseOperationException("Error while trying to "
            + (createDb ? "create" : "drop") + " a database: " + ex.getLocalizedMessage(), ex);
      } finally {
        if (stmt != null) {
          try {
            stmt.close();
          } catch (SQLException ex) {
          }
        }
        if (connection != null) {
          try {
            connection.close();
          } catch (SQLException ex) {
          }
        }
      }
    } else {
      throw new UnsupportedOperationException("Unable to " + (createDb ? "create" : "drop")
          + " a PostgreSQL database via the driver " + jdbcDriverClassName);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createDatabase(String jdbcDriverClassName, String connectionUrl, String username,
      String password, Map parameters) throws DatabaseOperationException,
      UnsupportedOperationException {
    // With PostgreSQL, you create a database by executing "CREATE DATABASE"
    // in an existing database (usually
    // the template1 database because it usually exists)
    createOrDropDatabase(jdbcDriverClassName, connectionUrl, username, password, parameters, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void dropDatabase(String jdbcDriverClassName, String connectionUrl, String username,
      String password) throws DatabaseOperationException, UnsupportedOperationException {
    // With PostgreSQL, you create a database by executing "DROP DATABASE"
    // in an existing database (usually
    // the template1 database because it usually exists)
    createOrDropDatabase(jdbcDriverClassName, connectionUrl, username, password, null, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void setObject(PreparedStatement statement, int sqlIndex, DynaBean dynaBean,
      SqlDynaProperty property) throws SQLException {
    int typeCode = property.getColumn().getTypeCode();
    Object value = dynaBean.get(property.getName());

    // Downgrade typecode
    if (typeCode == ExtTypes.NCHAR)
      typeCode = Types.CHAR;
    if (typeCode == ExtTypes.NVARCHAR)
      typeCode = Types.VARCHAR;

    // PostgreSQL doesn't like setNull for BYTEA columns
    if (value == null) {
      switch (typeCode) {
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        statement.setBlob(sqlIndex, (Blob) null);
        break;
      case Types.BLOB:
        statement.setNull(sqlIndex, Types.BINARY);
        break;
      default:
        statement.setNull(sqlIndex, typeCode);
        break;
      }
    } else {
      super.setObject(statement, sqlIndex, dynaBean, property);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void disableAllFK(Connection connection, Database model, boolean continueOnError)
      throws DatabaseOperationException {

    try {
      StringWriter buffer = new StringWriter();
      getSqlBuilder().setWriter(buffer);
      for (int i = 0; i < model.getTableCount(); i++) {
        getSqlBuilder().dropExternalForeignKeys(model.getTable(i));
      }
      evaluateBatchRealBatch(connection, buffer.toString(), continueOnError);
      /*
       * PreparedStatementpstmt=connection.prepareStatement(
       * "update pg_class set reltriggers=0 WHERE PG_CLASS.RELNAMESPACE IN (SELECT PG_NAMESPACE.OID FROM PG_NAMESPACE WHERE PG_NAMESPACE.NSPNAME = CURRENT_SCHEMA())"
       * ); pstmt.executeUpdate(); pstmt.close();
       */
    } catch (Exception e) {
      e.printStackTrace();
      throw new DatabaseOperationException("Error while disabling foreign key ", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void disableAllFkForTable(Connection connection, Table table, boolean continueOnError)
      throws DatabaseOperationException {
    try {
      StringWriter buffer = new StringWriter();
      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().dropExternalForeignKeys(table);
      evaluateBatchRealBatch(connection, buffer.toString(), continueOnError);
    } catch (Exception e) {
      e.printStackTrace();
      throw new DatabaseOperationException("Error while disabling foreign key ", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean enableAllFK(Connection connection, Database model, boolean continueOnError)
      throws DatabaseOperationException {

    try {
      StringWriter buffer = new StringWriter();
      getSqlBuilder().setWriter(buffer);
      for (int i = 0; i < model.getTableCount(); i++) {
        getSqlBuilder().createExternalForeignKeys(model, model.getTable(i));
      }
      int numErrors = evaluateBatchRealBatch(connection, buffer.toString(), continueOnError);
      if (numErrors > 0) {
        return false;
      }
      return true;
      /*
       * PreparedStatementpstmt=connection.prepareStatement(
       * "update pg_class set reltriggers = (SELECT count(*) from pg_trigger where pg_class.oid=tgrelid) WHERE PG_CLASS.RELNAMESPACE IN (SELECT PG_NAMESPACE.OID FROM PG_NAMESPACE WHERE PG_NAMESPACE.NSPNAME = CURRENT_SCHEMA())"
       * ); pstmt.executeUpdate(); pstmt.close();
       */
    } catch (Exception e) {
      e.printStackTrace();
      throw new DatabaseOperationException("Error while enabling foreign key ", e);
    }
  }

  public boolean enableAllFkForTable(Connection connection, Database model, Table table,
      boolean continueOnError) throws DatabaseOperationException {
    try {
      StringWriter buffer = new StringWriter();
      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().createExternalForeignKeys(model, table);
      int numErrors = evaluateBatchRealBatch(connection, buffer.toString(), continueOnError);
      if (numErrors > 0) {
        return false;
      }
      return true;
    } catch (Exception e) {
      throw new DatabaseOperationException("Error while enabling foreign key ", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void disableAllTriggers(Connection connection, Database model, boolean continueOnError)
      throws DatabaseOperationException {

    try {
      StringWriter buffer = new StringWriter();
      getSqlBuilder().setWriter(buffer);
      for (int i = 0; i < model.getTriggerCount(); i++) {
        getSqlBuilder().disableTrigger(model, model.getTrigger(i));
      }
      evaluateBatchRealBatch(connection, buffer.toString(), continueOnError);
      /*
       * PreparedStatementpstmt=connection.prepareStatement(
       * "update pg_class set reltriggers=0 WHERE PG_CLASS.RELNAMESPACE IN (SELECT PG_NAMESPACE.OID FROM PG_NAMESPACE WHERE PG_NAMESPACE.NSPNAME = CURRENT_SCHEMA())"
       * ); pstmt.executeUpdate(); pstmt.close();
       */
    } catch (Exception e) {
      e.printStackTrace();
      throw new DatabaseOperationException("Error while disabling triggers ", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean enableAllTriggers(Connection connection, Database model, boolean continueOnError)
      throws DatabaseOperationException {

    try {
      ((PostgreSqlBuilder) getSqlBuilder()).initializeTranslators(model);
      StringWriter buffer = new StringWriter();
      getSqlBuilder().setWriter(buffer);
      for (int i = 0; i < model.getTriggerCount(); i++) {
        ((PostgreSqlBuilder) getSqlBuilder()).enableTrigger(model, model.getTrigger(i));
      }
      int numErrors = evaluateBatchRealBatch(connection, buffer.toString(), continueOnError);
      if (numErrors > 0) {
        return false;
      }
      return true;
      /*
       * PreparedStatementpstmt=connection.prepareStatement(
       * "update pg_class set reltriggers = (SELECT count(*) from pg_trigger where pg_class.oid=tgrelid) WHERE PG_CLASS.RELNAMESPACE IN (SELECT PG_NAMESPACE.OID FROM PG_NAMESPACE WHERE PG_NAMESPACE.NSPNAME = CURRENT_SCHEMA())"
       * ); pstmt.executeUpdate(); pstmt.close();
       */
    } catch (Exception e) {
      e.printStackTrace();
      throw new DatabaseOperationException("Error while enabling triggers ", e);
    }
  }

  public void disableAllFK(Database model, boolean continueOnError, Writer writer)
      throws DatabaseOperationException {

    String current = null;
    try {

      StringWriter buffer = new StringWriter();
      getSqlBuilder().setWriter(buffer);
      for (int i = 0; i < model.getTableCount(); i++) {
        getSqlBuilder().dropExternalForeignKeys(model.getTable(i));
      }
      writer.append(buffer.toString());
    } catch (Exception e) {
      System.out.println("SQL command failed with " + e.getMessage());
      System.out.println(current);
      throw new DatabaseOperationException("Error while disabling foreign key ", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void enableAllFK(Database model, boolean continueOnError, Writer writer)
      throws DatabaseOperationException {

    String current = null;
    StringWriter buffer = new StringWriter();
    try {
      getSqlBuilder().setWriter(buffer);
      for (int i = 0; i < model.getTableCount(); i++) {

        getSqlBuilder().createExternalForeignKeys(model, model.getTable(i));
      }
      writer.append(buffer.toString());
    } catch (Exception e) {
      System.out.println("SQL command failed with " + e.getMessage());
      System.out.println(current);
      throw new DatabaseOperationException("Error while enabling foreign key ", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void disableAllTriggers(Database model, boolean continueOnError, Writer writer)
      throws DatabaseOperationException {

    String current = null;
    try {
      StringWriter buffer = new StringWriter();
      getSqlBuilder().setWriter(buffer);
      for (int i = 0; i < model.getTriggerCount(); i++) {
        getSqlBuilder().dropTrigger(model, model.getTrigger(i));
      }
      writer.append(buffer.toString());
    } catch (Exception e) {
      System.out.println("SQL command failed with " + e.getMessage());
      System.out.println(current);
      throw new DatabaseOperationException("Error while disabling triggers ", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void enableAllTriggers(Database model, boolean continueOnError, Writer writer)
      throws DatabaseOperationException {

    String current = null;
    try {
      StringWriter buffer = new StringWriter();
      getSqlBuilder().setWriter(buffer);
      for (int i = 0; i < model.getTriggerCount(); i++) {
        getSqlBuilder().createTrigger(model, model.getTrigger(i));
      }
      writer.append(buffer.toString());
    } catch (Exception e) {
      System.out.println("SQL command failed with " + e.getMessage());
      System.out.println(current);
      throw new DatabaseOperationException("Error while enabling triggers ", e);
    }
  }

  @Override
  public List<StructureObject> checkTranslationConsistency(Database database, Database fullDatabase) {
    List<PostgrePLSQLConsistencyChecker> tasks = new ArrayList<>();
    for (int i = 0; i < database.getFunctionCount(); i++) {
      tasks.add(new PostgrePLSQLConsistencyChecker(fullDatabase, database.getFunction(i)));
    }

    for (int i = 0; i < database.getTriggerCount(); i++) {
      tasks.add(new PostgrePLSQLConsistencyChecker(fullDatabase, database.getTrigger(i)));
    }

    List<StructureObject> inconsistentObjects = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(getMaxThreads());
    try {
      for (Future<StructureObject> result : executor.invokeAll(tasks)) {
        StructureObject inconsistentObject = result.get();
        if (inconsistentObject != null) {
          inconsistentObjects.add(inconsistentObject);
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      getLog().error("Error checking translation consistency", e);
    } finally {
      executor.shutdown();
      try {
        executor.awaitTermination(5L, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        getLog().error("Error shutting down thread pool", e);
      }
    }
    return inconsistentObjects;
  }

  public void disableNOTNULLColumns(Database database, OBDataset dataset) {

    Connection connection = borrowConnection();
    try {
      evaluateBatchRealBatch(connection, disableNOTNULLColumnsSql(database, dataset), true);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      returnConnection(connection);
    }
  }

  public void enableNOTNULLColumns(Database database, OBDataset dataset) {

    Connection connection = borrowConnection();

    try {
      evaluateBatchRealBatch(connection, enableNOTNULLColumnsSql(database, dataset), true);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      returnConnection(connection);
    }
  }

  @Override
  public String limitOneRow() {
    return " LIMIT 1";
  }

  protected int handleFailedBatchExecution(Connection connection, List<String> sql,
      boolean continueOnError, long indexFailedStatement) {
    try {
      connection.rollback();
      connection.setAutoCommit(true);
    } catch (SQLException e) {
      getLog().error("Error while handling a failed batch execution ins postgreSql.");
    }
    getLog()
        .info(
            "Batch statement failed. Rolling back and retrying all the statements in a non-batched connection.");
    // The batch failed. We will execute all commands again using the old method
    return evaluateBatch(connection, sql, continueOnError);
  }

  public int evaluateBatchRealBatch(Connection connection, List<String> sql, boolean continueOnError)
      throws DatabaseOperationException {
    try {
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      // will not happen
    }
    return super.evaluateBatchRealBatch(connection, sql, continueOnError);
  }

}
