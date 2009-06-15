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
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.beanutils.DynaBean;
import org.apache.ddlutils.DatabaseOperationException;
import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.dynabean.SqlDynaProperty;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.platform.PlatformImplBase;
import org.apache.ddlutils.util.ExtTypes;

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

    setSqlBuilder(new PostgreSqlBuilder(this));
    setModelReader(new PostgreSqlModelReader(this));
    setModelLoader(new PostgreSqlModelLoader());
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
      evaluateBatch(connection, buffer.toString(), continueOnError);
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
  public void enableAllFK(Connection connection, Database model, boolean continueOnError)
      throws DatabaseOperationException {

    try {
      StringWriter buffer = new StringWriter();
      getSqlBuilder().setWriter(buffer);
      for (int i = 0; i < model.getTableCount(); i++) {
        getSqlBuilder().createExternalForeignKeys(model, model.getTable(i));
      }
      evaluateBatch(connection, buffer.toString(), continueOnError);
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
        getSqlBuilder().dropTrigger(model, model.getTrigger(i));
      }
      evaluateBatch(connection, buffer.toString(), continueOnError);
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
  public void enableAllTriggers(Connection connection, Database model, boolean continueOnError)
      throws DatabaseOperationException {

    try {
      ((PostgreSqlBuilder) getSqlBuilder()).initializeTranslators(model);
      StringWriter buffer = new StringWriter();
      getSqlBuilder().setWriter(buffer);
      for (int i = 0; i < model.getTriggerCount(); i++) {
        ((PostgreSqlBuilder) getSqlBuilder()).createTrigger(model, model.getTrigger(i));
      }
      evaluateBatch(connection, buffer.toString(), continueOnError);
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
  // /**
  // * {@inheritDoc}
  // */
  // public void deleteAllDataFromTables(Connection connection, Database
  // model, boolean continueOnError) throws DatabaseOperationException {
  //        
  // try {
  // PreparedStatement pstmt =
  // connection.prepareStatement("SELECT 'DELETE FROM '|| TABLENAME  as SQL_STR FROM PG_TABLES WHERE TABLEOWNER="+connection.getMetaData().getUserName()
  // );
  // ResultSet rs=pstmt.executeQuery();
  // while (rs.next ()) {
  // // System.out.println (rs.getString("SQL_STR"));
  // PreparedStatement
  // pstmtd=connection.prepareStatement(rs.getString("SQL_STR"));
  // pstmtd.executeQuery();
  // pstmtd.close();
  // }
  // rs.close();
  // pstmt.close();
  // } catch (SQLException e) {
  // e.printStackTrace();
  // throw new DatabaseOperationException("Error while truncating tables ",
  // e);
  // }
  // }
}
