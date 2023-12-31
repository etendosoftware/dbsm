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

import java.io.StringWriter;
import java.io.Writer;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.DatabaseOperationException;
import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.StructureObject;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.Trigger;
import org.apache.ddlutils.platform.OracleStandardBatchEvaluator;
import org.apache.ddlutils.platform.PlatformImplBase;
import org.apache.ddlutils.platform.postgresql.PostgrePLSQLFunctionStandarization;
import org.apache.ddlutils.platform.postgresql.PostgrePLSQLFunctionTranslation;
import org.apache.ddlutils.platform.postgresql.PostgrePLSQLStandarization;
import org.apache.ddlutils.platform.postgresql.PostgrePLSQLTriggerStandarization;
import org.apache.ddlutils.platform.postgresql.PostgrePLSQLTriggerTranslation;
import org.apache.ddlutils.translation.CommentFilter;
import org.apache.ddlutils.translation.LiteralFilter;
import org.apache.ddlutils.util.ExtTypes;

/**
 * The platform for Oracle 8.
 * 
 * TODO: We might support the
 * {@link org.apache.ddlutils.Platform#createDatabase(String, String, String, String, Map)}
 * functionality via "CREATE SCHEMA"/"CREATE USER" or "CREATE TABLESPACE" ?
 * 
 * @version $Revision: 231306 $
 */
public class OraclePlatform extends PlatformImplBase {
  /** Database name of this platform. */
  public static final String DATABASENAME = "Oracle";
  /** The standard Oracle jdbc driver. */
  public static final String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
  /** The old Oracle jdbc driver. */
  public static final String JDBC_DRIVER_OLD = "oracle.jdbc.dnlddriver.OracleDriver";
  /** The thin subprotocol used by the standard Oracle driver. */
  public static final String JDBC_SUBPROTOCOL_THIN = "oracle:thin";
  /** The thin subprotocol used by the standard Oracle driver. */
  public static final String JDBC_SUBPROTOCOL_OCI8 = "oracle:oci8";
  /** The old thin subprotocol used by the standard Oracle driver. */
  public static final String JDBC_SUBPROTOCOL_THIN_OLD = "oracle:dnldthin";

  public static final String JDBC_DRIVER_DATADIRECT_ORACLE = "com.ddtek.jdbc.oracle.OracleDriver";
  public static final String JDBC_DRIVER_INET_ORACLE = "com.inet.ora.OraDriver";

  public static final String JDBC_SUBPROTOCOL_DATADIRECT_ORACLE = "datadirect:oracle";
  public static final String JDBC_SUBPROTOCOL_INET_ORACLE = "inetora";

  /**
   * Creates a new platform instance.
   */
  public OraclePlatform() {
    PlatformInfo info = getPlatformInfo();

    info.setMaxIdentifierLength(30);
    info.setIdentityStatusReadingSupported(false);

    // Note that the back-mappings are partially done by the model reader,
    // not the driver
    info.addNativeTypeMapping(Types.CHAR, "CHAR");
    info.addNativeTypeMapping(ExtTypes.NCHAR, "NCHAR");
    info.addNativeTypeMapping(Types.ARRAY, "BLOB", Types.BLOB);
    info.addNativeTypeMapping(Types.BIGINT, "NUMBER(38)");
    info.addNativeTypeMapping(Types.BINARY, "RAW", Types.VARBINARY);
    info.addNativeTypeMapping(Types.BIT, "NUMBER(1)");
    info.addNativeTypeMapping(Types.DATE, "DATE", Types.TIMESTAMP);
    info.addNativeTypeMapping(Types.DECIMAL, "NUMBER");
    info.addNativeTypeMapping(Types.DISTINCT, "BLOB", Types.BLOB);
    info.addNativeTypeMapping(Types.DOUBLE, "DOUBLE PRECISION");
    info.addNativeTypeMapping(Types.FLOAT, "FLOAT", Types.DOUBLE);
    info.addNativeTypeMapping(Types.JAVA_OBJECT, "BLOB", Types.BLOB);
    info.addNativeTypeMapping(Types.LONGVARBINARY, "BLOB", Types.BLOB);
    info.addNativeTypeMapping(Types.LONGVARCHAR, "CLOB", Types.CLOB);
    info.addNativeTypeMapping(Types.NULL, "BLOB", Types.BLOB);
    info.addNativeTypeMapping(Types.NUMERIC, "NUMBER", Types.DECIMAL);
    info.addNativeTypeMapping(Types.OTHER, "BLOB", Types.BLOB);
    info.addNativeTypeMapping(Types.REF, "BLOB", Types.BLOB);
    info.addNativeTypeMapping(Types.SMALLINT, "NUMBER(5)");
    info.addNativeTypeMapping(Types.STRUCT, "BLOB", Types.BLOB);
    info.addNativeTypeMapping(Types.TIME, "DATE", Types.TIMESTAMP);
    info.addNativeTypeMapping(Types.TIMESTAMP, "DATE");
    info.addNativeTypeMapping(Types.TINYINT, "NUMBER(3)");
    info.addNativeTypeMapping(Types.VARBINARY, "RAW");
    info.addNativeTypeMapping(Types.VARCHAR, "VARCHAR2");
    info.addNativeTypeMapping(ExtTypes.NVARCHAR, "NVARCHAR2");

    info.addNativeTypeMapping("BOOLEAN", "NUMBER(1)", "BIT");
    info.addNativeTypeMapping("DATALINK", "BLOB", "BLOB");

    info.setDefaultSize(Types.CHAR, 254);
    info.setDefaultSize(Types.VARCHAR, 254);
    info.setDefaultSize(Types.BINARY, 254);
    info.setDefaultSize(Types.VARBINARY, 254);

    info.setColumnOrderManaged(false);
    info.setEmptyStringIsNull(true);

    setSqlBuilder(new OracleBuilder(this));
    setModelReader(new OracleModelReader(this));
    setModelLoader(new OracleModelLoader());
    setBatchEvaluator(new OracleStandardBatchEvaluator(this));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getCreateTablesSqlScript(Database model, boolean dropTablesFirst,
      boolean continueOnError) {
    return "SET DEFINE OFF\n/\n" + getCreateTablesSql(model, dropTablesFirst, continueOnError);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return DATABASENAME;
  }

  @Override
  public void disableAllFkForTable(Connection connection, Table table, boolean continueOnError)
      throws DatabaseOperationException {
    disableAllFK(connection, null, table, continueOnError);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void disableAllFK(Connection connection, Database model, boolean continueOnError)
      throws DatabaseOperationException {
    Table table = null;
    disableAllFK(connection, model, table, continueOnError);
  }

  /**
   * {@inheritDoc}
   */
  public void disableAllFK(Connection connection, Database model, Table table,
      boolean continueOnError) throws DatabaseOperationException {

    String current = null;
    try {
      connection.prepareCall("PURGE RECYCLEBIN").execute();
      current = "SELECT 'ALTER TABLE'|| ' ' || TABLE_NAME || ' ' || 'DISABLE CONSTRAINT' || ' ' || CONSTRAINT_NAME  SQL_STR FROM USER_CONSTRAINTS WHERE  CONSTRAINT_TYPE='R' ";
      PreparedStatement pstmt = null;
      if (table != null) {
        current = current + " AND UPPER(TABLE_NAME) = ?";
        pstmt = connection.prepareStatement(current);
        pstmt.setString(1, table.getName());
      } else {
        pstmt = connection.prepareStatement(current);
      }

      ResultSet rs = pstmt.executeQuery();

      while (rs.next()) {
        current = rs.getString("SQL_STR");
        PreparedStatement pstmtd = connection.prepareStatement(current);
        pstmtd.executeUpdate();
        pstmtd.close();
      }
      rs.close();
      pstmt.close();
    } catch (SQLException e) {
      System.out.println("SQL command failed with " + e.getMessage());
      System.out.println(current);
      throw new DatabaseOperationException("Error while disabling foreign key ", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean enableAllFK(Connection connection, Database model, boolean continueOnError)
      throws DatabaseOperationException {

    String current = null;
    try {
      current = "SELECT 'ALTER TABLE'|| ' ' || TABLE_NAME || ' ' || 'ENABLE CONSTRAINT' || ' ' || CONSTRAINT_NAME  SQL_STR FROM USER_CONSTRAINTS WHERE  CONSTRAINT_TYPE='R' ";
      PreparedStatement pstmt = connection.prepareStatement(current);
      ResultSet rs = pstmt.executeQuery();

      while (rs.next()) {
        current = rs.getString("SQL_STR");
        PreparedStatement pstmtd = connection.prepareStatement(current);
        pstmtd.executeUpdate();
        pstmtd.close();
      }
      rs.close();
      pstmt.close();
      return true;
    } catch (SQLException e) {
      System.out.println("SQL command failed with " + e.getMessage());
      System.out.println(current);
      throw new DatabaseOperationException("Error while enabling foreign key ", e);
    }
  }

  @Override
  public boolean enableAllFkForTable(Connection connection, Database model, Table table,
      boolean continueOnError) throws DatabaseOperationException {
    String current = null;
    try {
      current = "SELECT 'ALTER TABLE'|| ' ' || TABLE_NAME || ' ' || 'ENABLE CONSTRAINT' || ' ' || CONSTRAINT_NAME  SQL_STR FROM USER_CONSTRAINTS WHERE  CONSTRAINT_TYPE='R' AND UPPER(TABLE_NAME) = UPPER('"
          + table.getName() + "')";
      PreparedStatement pstmt = connection.prepareStatement(current);
      ResultSet rs = pstmt.executeQuery();

      while (rs.next()) {
        current = rs.getString("SQL_STR");
        PreparedStatement pstmtd = connection.prepareStatement(current);
        pstmtd.executeUpdate();
        pstmtd.close();
      }
      rs.close();
      pstmt.close();
      return true;
    } catch (SQLException e) {
      System.out.println("SQL command failed with " + e.getMessage());
      System.out.println(current);
      throw new DatabaseOperationException("Error while enabling foreign key ", e);
    }
  }

  @Override
  protected String getQueryToBuildTriggerDisablementQuery() {
    return "SELECT 'ALTER TRIGGER'|| ' ' || TRIGGER_NAME || ' ' || 'DISABLE' SQL_STR FROM USER_TRIGGERS";
  }

  @Override
  protected String getQueryToBuildTriggerEnablementQuery() {
    return "SELECT 'ALTER TRIGGER'|| ' ' || TRIGGER_NAME || ' ' || 'ENABLE' SQL_STR FROM USER_TRIGGERS";
  }

  @Override
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
  @Override
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
  @Override
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
  @Override
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
  public void dropTrigger(Connection connection, Database model, Trigger trigger)
      throws DatabaseOperationException {
    try {
      StringWriter buffer = new StringWriter();
      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().dropTrigger(model, trigger);
      evaluateBatchRealBatch(connection, buffer.toString(), true);
    } catch (Exception e) {
      System.out.println("SQL command failed with " + e.getMessage());
      throw new DatabaseOperationException("Error while dropping a trigger", e);
    }
  }

  @Override
  public void createTrigger(Connection connection, Database model, Trigger trigger,
      List<String> triggersToFollow) throws DatabaseOperationException {
    try {
      StringWriter buffer = new StringWriter();
      getSqlBuilder().setWriter(buffer);
      getSqlBuilder().createTrigger(model, trigger, triggersToFollow);
      evaluateBatchRealBatch(connection, buffer.toString(), true);
    } catch (Exception e) {
      System.out.println("SQL command failed with " + e.getMessage());
      throw new DatabaseOperationException("Error while createing a trigger", e);
    }
  }

  @Override
  public List<StructureObject> checkTranslationConsistency(Database database,
      Database fullDatabase) {
    List<StructureObject> inconsistentObjects = new ArrayList<>();
    PostgrePLSQLStandarization.generateOutPatterns(fullDatabase);
    PostgrePLSQLFunctionTranslation funcTrans = new PostgrePLSQLFunctionTranslation(fullDatabase);
    for (int i = 0; i < database.getFunctionCount(); i++) {
      int indF = -1;
      Function f = database.getFunction(i);
      for (int j = 0; indF == -1 && j < fullDatabase.getFunctionCount(); j++) {
        if (fullDatabase.getFunction(j).equals(f)) {
          indF = j;
          break;
        }
      }
      PostgrePLSQLFunctionStandarization funcStand = new PostgrePLSQLFunctionStandarization(
          fullDatabase, indF);
      LiteralFilter litFilter1 = new LiteralFilter();
      CommentFilter comFilter1 = new CommentFilter();
      LiteralFilter litFilter2 = new LiteralFilter();
      CommentFilter comFilter2 = new CommentFilter();
      String s1 = litFilter1.removeLiterals(f.getBody());
      s1 = comFilter1.removeComments(s1);
      s1 = funcTrans.exec(s1);
      s1 = comFilter1.restoreComments(s1);
      s1 = litFilter1.restoreLiterals(s1);
      s1 = litFilter2.removeLiterals(s1);
      s1 = comFilter2.removeComments(s1);
      s1 = funcStand.exec(s1);
      s1 = comFilter2.restoreComments(s1);
      s1 = litFilter2.restoreLiterals(s1);
      String s2 = f.getBody();
      String s1R = s1.replaceAll("\\s", "");
      String s2R = s2.replaceAll("\\s", "");

      if (!s1R.equals(s2R)) {
        getLog().warn("Found differences in " + f.getName());
        printDiff(s1, s2);
        inconsistentObjects.add(f);
      }
    }

    for (int i = 0; i < database.getTriggerCount(); i++) {
      PostgrePLSQLTriggerTranslation triggerTrans = new PostgrePLSQLTriggerTranslation(
          fullDatabase);
      int indF = -1;
      Trigger trg = database.getTrigger(i);
      for (int j = 0; indF == -1 && j < fullDatabase.getTriggerCount(); j++) {
        if (fullDatabase.getTrigger(j).equals(trg)) {
          indF = j;
          break;
        }
      }
      PostgrePLSQLTriggerStandarization triggerStand = new PostgrePLSQLTriggerStandarization(
          fullDatabase, indF);
      if (trg.getOriginalBody() != null) {
        LiteralFilter litFilter1 = new LiteralFilter();
        CommentFilter comFilter1 = new CommentFilter();
        LiteralFilter litFilter2 = new LiteralFilter();
        CommentFilter comFilter2 = new CommentFilter();
        String s1 = litFilter1.removeLiterals(trg.getBody());
        s1 = comFilter1.removeComments(s1);
        s1 = triggerTrans.exec(s1);
        s1 = comFilter1.restoreComments(s1);
        s1 = litFilter1.restoreLiterals(s1);
        s1 = litFilter2.removeLiterals(s1);
        s1 = comFilter2.removeComments(s1);
        s1 = triggerStand.exec(s1);
        s1 = comFilter2.restoreComments(s1);
        s1 = litFilter2.restoreLiterals(s1);
        String s2 = trg.getBody();
        String s1R = s1.replaceAll("\\s", "");
        String s2R = s2.replaceAll("\\s", "");
        if (s1R.equals(s2R)) {
          trg.setBody(s1);
        } else {
          getLog().warn("Found differences in " + trg.getName());
          printDiff(s1, s2);
          inconsistentObjects.add(trg);
        }
      }
    }

    return inconsistentObjects;
  }

  @Override
  public String limitOneRow() {
    return " where rownum<2";
  }

  @Override
  public void updateDBStatistics() {
    long t = System.currentTimeMillis();
    DataSource ds = getDataSource();
    String userName;
    if (ds instanceof BasicDataSource) {
      userName = ((BasicDataSource) ds).getUsername();
    } else {
      userName = getUsername();
    }
    getLog().info("Updating statistics for " + userName + "...");

    Connection con = null;
    CallableStatement cst = null;
    String st = "{call dbms_stats.gather_schema_stats('" + userName + "', cascade=>TRUE)}";
    try {
      con = borrowConnection();

      cst = con.prepareCall(st);
      cst.execute();
    } catch (SQLException e) {
      getLog().error("Couldn't execute " + st, e);
    } finally {
      if (cst != null) {
        try {
          cst.close();
        } catch (SQLException ignored) {
        }
      }
      returnConnection(con);
    }

    getLog().info("...statistics updated in " + (System.currentTimeMillis() - t) + " ms");
  }
}
