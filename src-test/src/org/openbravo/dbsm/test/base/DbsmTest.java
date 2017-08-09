/*
 ************************************************************************************
 * Copyright (C) 2015-2017 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.dbsm.test.base;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.AddRowChange;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.ColumnDataChange;
import org.apache.ddlutils.alteration.DataChange;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.alteration.ModelChange;
import org.apache.ddlutils.alteration.ModelComparator;
import org.apache.ddlutils.alteration.RemoveRowChange;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.apache.ddlutils.platform.Oracle8StandardBatchEvaluator;
import org.apache.ddlutils.platform.PGStandardBatchEvaluator;
import org.apache.ddlutils.platform.SQLBatchEvaluator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openbravo.ddlutils.task.DatabaseUtils;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.OBDataset;
import org.openbravo.ddlutils.util.OBDatasetTable;

/**
 * Base class to for test cases for DBSM. Classes extending this one, should be run as
 * Parameterized.
 * 
 * <ul>
 * <li>Reads DB definitions in config/db-config.json file
 * <li>Provides common utilities
 * </ul>
 * 
 * @author alostale
 *
 */
@RunWith(Parameterized.class)
public class DbsmTest {
  private String password;
  private String user;
  private String driver;
  private String url;
  protected Logger log;
  protected String modelPath;
  private static BasicDataSource ds;
  private static String previousDB;
  private Rdbms rdbms;
  private Platform platform;
  private SQLBatchEvaluator evaluator;
  private ExcludeFilter excludeFilter;
  protected RecreationMode recreationMode = RecreationMode.standard;
  private boolean logErrorsAllowed = false;
  private int threads = 0;

  public enum Rdbms {
    PG, ORA
  }

  public enum RecreationMode {
    standard, forced
  }

  @Rule
  public TestWatcher watcher = new TestWatcher() {
    @Override
    protected void starting(Description description) {
      log.info("*** Starting test case: " + description.getClassName() + "."
          + description.getMethodName());
    }
  };

  public DbsmTest(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    initLogging();

    String workingDir = System.getProperty("user.dir");
    log.info("Working directory: " + workingDir);

    String ownerUrl;
    if ("POSTGRE".equals(rdbms)) {
      ownerUrl = url + "/" + sid;
      this.rdbms = Rdbms.PG;
    } else {
      ownerUrl = url;
      this.rdbms = Rdbms.ORA;
    }

    log.info("db: " + ownerUrl);

    this.password = password;
    this.user = user;
    this.driver = driver;
    this.url = ownerUrl;

    if (previousDB == null || !previousDB.equals(ownerUrl)) {
      if (ds != null) {
        try {
          log.info("Closing datasource to switch DB from " + previousDB + " to " + ownerUrl);
          ds.close();
        } catch (SQLException e) {
          log.error("Error closing ds", e);
        }
      }

      ds = null;
      previousDB = ownerUrl;
    }

    if (ds == null) {
      ds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getUser(), getPassword());
    }
  }

  /**
   * Reads parameters in "config/db-config.json" to invoke the test cases
   */
  @Parameters(name = "DB: {6}")
  public static Collection<String[]> params() throws IOException, JSONException {
    JSONObject config = new JSONObject(FileUtils.readFileToString(
        new File("config/db-config.json"), "utf-8"));

    List<String[]> configs = new ArrayList<String[]>();
    JSONArray dbs = config.getJSONArray("testDBs");
    for (int i = 0; i < dbs.length(); i++) {
      JSONObject jsonDb = dbs.getJSONObject(i);
      if (jsonDb.has("skip") && jsonDb.getBoolean("skip")) {
        continue;
      }
      String dbName = jsonDb.has("name") ? jsonDb.getString("name") : (jsonDb.getString("rdbms")
          + " " + jsonDb.getString("url") + " - " + jsonDb.getString("user"));
      configs.add(new String[] {//
          jsonDb.getString("rdbms"), //
              jsonDb.getString("driver"), //
              jsonDb.getString("url"), //
              jsonDb.getString("sid"), //
              jsonDb.getString("user"), //
              jsonDb.getString("password"), //
              dbName });
    }

    return configs;
  }

  @Before
  public void resetLogAppender() {
    logErrorsAllowed = false;
    TestLogAppender.reset();
  }

  @After
  public void thereShouldNoBeErrorsInLog() {
    if (!logErrorsAllowed) {
      assertThat(TestLogAppender.getWarnAndErrors(), is(empty()));
    }
  }

  /** do not fail if this test cases logs errors or warnings */
  protected void allowLogErrorsForThisTest() {
    logErrorsAllowed = true;
  }

  private void initLogging() {
    final Properties props = new Properties();
    final String level = Level.DEBUG.toString();

    props.setProperty("log4j.rootCategory", level + ",A, T");
    props.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");

    props.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
    props.setProperty("log4j.appender.A.layout.ConversionPattern", "%-4r %-5p %c - %m%n");

    props.setProperty("log4j.appender.T", "org.openbravo.dbsm.test.base.TestLogAppender");

    props.setProperty("log4j.logger.org.apache.commons", "WARN");
    props.setProperty("log4j.logger.org.apache.ddlutils.util.JdbcSupport", "WARN");

    LogManager.resetConfiguration();
    PropertyConfigurator.configure(props);
    log = Logger.getLogger(getClass());
  }

  protected String getPassword() {
    return password;
  }

  protected String getUser() {
    return user;
  }

  protected String getDriver() {
    return driver;
  }

  protected String getUrl() {
    return url;
  }

  protected File getModel() {
    return new File("model/" + modelPath);
  }

  protected ExcludeFilter getExcludeFilter() {
    if (excludeFilter == null) {
      excludeFilter = new ExcludeFilter() {
        @Override
        public String[] getExcludedViews() {
          return new String[] { "DUAL" };
        }
      };
    }
    return excludeFilter;
  }

  public void setExcludeFilter(ExcludeFilter excludeFilter) {
    this.excludeFilter = excludeFilter;
  }

  protected BasicDataSource getDataSource() {
    return ds;
  }

  public Rdbms getRdbms() {
    return rdbms;
  }

  protected void setNumberOfThreads(int threads) {
    this.threads = threads;
  }

  protected int getNumberOfThreads() {
    return this.threads;
  }

  protected List<String> sqlStatmentsForUpdate(String dbModelPath) {
    evaluator = new TestBatchEvaluator();
    updateDatabase(dbModelPath, false);
    List<String> res = ((TestBatchEvaluator) evaluator).getSQLStatements();
    evaluator = null;
    return res;
  }

  protected List<String> sqlStatmentsForUpdate(String dbModelPath, String adDirectoryName,
      List<String> adTableNames) {
    evaluator = new TestBatchEvaluator();
    updateDatabase(dbModelPath, adDirectoryName, adTableNames, false);
    List<String> res = ((TestBatchEvaluator) evaluator).getSQLStatements();
    evaluator = null;
    return res;
  }

  protected List<String> sqlStatmentsForUpdate(String dbModelPath, List<String> configScripts) {
    evaluator = new TestBatchEvaluator();
    updateDatabase(dbModelPath, null, null, false, configScripts);
    List<String> res = ((TestBatchEvaluator) evaluator).getSQLStatements();
    evaluator = null;
    return res;
  }

  protected Database updateDatabase(String dbModelPath) {
    return updateDatabase(dbModelPath, null, null);
  }

  protected Database updateDatabase(String dbModelPath, List<String> configScripts) {
    return updateDatabase(dbModelPath, null, null, false, configScripts);
  }

  protected Database updateDatabase(String dbModelPath, boolean assertDBisCorrect) {
    return updateDatabase(dbModelPath, null, null, assertDBisCorrect);
  }

  protected Database updateDatabase(String dbModelPath, String adDirectoryName,
      List<String> adTableNames) {
    return updateDatabase(dbModelPath, adDirectoryName, adTableNames, true);
  }

  protected Database updateDatabase(String dbModelPath, String adDirectoryName,
      List<String> adTableNames, boolean assertDBisCorrect) {
    return updateDatabase(dbModelPath, adDirectoryName, adTableNames, assertDBisCorrect, null);
  }

  /**
   * Utility method to to update current DB to model defined in dbModelPath
   */
  protected Database updateDatabase(String dbModelPath, String adDirectoryName,
      List<String> adTableNames, boolean assertDBisCorrect, List<String> configScripts) {
    Connection connection = null;
    try {
      File dbModel = new File("model", dbModelPath);
      final Platform platform = getPlatform();
      platform.setMaxThreads(threads);
      log.info("Max threads " + platform.getMaxThreads());

      if (recreationMode == RecreationMode.forced) {
        platform.getSqlBuilder().setForcedRecreation("all");
      }

      // TODO: Check boolean. Before was true
      Database originalDB = platform.loadModelFromDatabase(getExcludeFilter(), false);
      Database newDB = DatabaseUtils.readDatabaseWithConfigScripts(dbModel, platform, dbModelPath,
          true, false, true, true);
      final DatabaseData databaseOrgData = new DatabaseData(newDB);
      databaseOrgData.setStrictMode(false);

      if (adDirectoryName != null) {
        DBSMOBUtil.getInstance().loadDataStructures(platform, databaseOrgData, originalDB, newDB,
            new File(adDirectoryName).getAbsolutePath(), "none", new File(adDirectoryName), false,
            false);
      }
      if (configScripts != null) {
        applyConfigScripts(configScripts, platform, databaseOrgData, newDB, false);
      }

      OBDataset ad = new OBDataset(databaseOrgData);

      if (adDirectoryName != null && adTableNames != null) {
        Vector<OBDatasetTable> adTables = new Vector<OBDatasetTable>();
        for (String tName : adTableNames) {
          OBDatasetTable t = new OBDatasetTable();
          t.setName(tName);
          t.setIncludeAllColumns(true);
          adTables.add(t);
        }
        ad.setTables(adTables);
      }

      Database oldModel;
      try {
        oldModel = (Database) originalDB.clone();
      } catch (CloneNotSupportedException e1) {
        throw new RuntimeException(e1);
      }
      log.info("Updating database model...");
      platform.alterTables(originalDB, newDB, false);
      log.info("Model update complete.");

      if (false) {
        DBSMOBUtil.getInstance().moveModuleDataFromInstTables(platform, newDB, null);
      }
      connection = platform.borrowConnection();
      // Now we apply the data part of the configuration scripts
      if (configScripts != null) {
        applyConfigScripts(configScripts, platform, databaseOrgData, newDB, true);
      }
      log.info("Comparing databases to find differences");
      final DataComparator dataComparator = new DataComparator(platform.getSqlBuilder()
          .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
      try {
        dataComparator.compareToUpdate(newDB, platform, databaseOrgData, ad, null);
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      Set<String> adTablesWithRemovedOrInsertedRecords = new HashSet<String>();
      Set<String> adTablesWithRemovedRecords = new HashSet<String>();
      for (Change dataChange : dataComparator.getChanges()) {
        if (dataChange instanceof RemoveRowChange) {
          Table table = ((RemoveRowChange) dataChange).getTable();
          String tableName = table.getName();
          if (ad.getTable(tableName) != null) {
            adTablesWithRemovedOrInsertedRecords.add(tableName);
            adTablesWithRemovedRecords.add(tableName);
          }
        } else if (dataChange instanceof AddRowChange) {
          Table table = ((AddRowChange) dataChange).getTable();
          String tableName = table.getName();
          if (ad.getTable(tableName) != null) {
            adTablesWithRemovedOrInsertedRecords.add(tableName);
          }
        }
      }
      log.info("Disabling foreign keys");
      platform.disableDatasetFK(connection, originalDB, ad, false,
          adTablesWithRemovedOrInsertedRecords);
      log.info("Disabling triggers");
      platform.disableAllTriggers(connection, newDB, false);
      platform.disableNOTNULLColumns(newDB, ad);

      // // Executing modulescripts
      //
      // ModuleScriptHandler hd = new ModuleScriptHandler();
      // hd.setBasedir(new File(basedir + "/../"));
      // hd.execute();
      //

      log.info("Updating Application Dictionary data...");
      platform.alterData(connection, newDB, dataComparator.getChanges());
      log.info("Removing invalid rows.");
      platform.deleteInvalidConstraintRows(newDB, ad, adTablesWithRemovedRecords, false);
      log.info("Recreating Primary Keys");
      List changes = platform.alterTablesRecreatePKs(oldModel, newDB, false);
      log.info("Executing oncreatedefault statements for mandatory columns");
      platform.executeOnCreateDefaultForMandatoryColumns(newDB, ad);
      log.info("Recreating not null constraints");
      platform.enableNOTNULLColumns(newDB, ad);
      log.info("Executing update final script (dropping temporary tables)");
      boolean postscriptCorrect = platform.alterTablesPostScript(oldModel, newDB, false, changes,
          null, ad);

      // assertThat("Postscript should be correct", postscriptCorrect, is(true));

      log.info("Enabling Foreign Keys and Triggers");
      boolean fksEnabled = platform.enableDatasetFK(connection, originalDB, ad,
          adTablesWithRemovedOrInsertedRecords, true);
      boolean triggersEnabled = platform.enableAllTriggers(connection, newDB, false);

      // Now check the new model updated in db is as it should
      if (assertDBisCorrect) {
        ModelComparator comparator = new ModelComparator(platform.getPlatformInfo(),
            platform.isDelimitedIdentifierModeOn());
        @SuppressWarnings("unchecked")
        List<ModelChange> newChanges = comparator.compare(
            DatabaseUtils.readDatabaseWithConfigScripts(dbModel, platform, dbModelPath, true,
                false, true, true), platform.loadModelFromDatabase(getExcludeFilter()));
        assertThat("changes between updated db and target db", newChanges, is(empty()));
      }

      return newDB;
    } finally {
      platform.returnConnection(connection);
    }
  }

  private void applyConfigScripts(List<String> templates, Platform currentPlatform,
      DatabaseData databaseOrgData, Database db, boolean applyConfigScriptData) {

    log.info("Loading and applying configuration scripts");
    Vector<File> configScripts = new Vector<File>();
    for (String template : templates) {
      File configScript = new File(template);
      if (configScript.exists()) {
        configScripts.add(configScript);
        DatabaseIO dbIO = new DatabaseIO();
        log.info("Loading configuration script: " + configScript.getAbsolutePath());
        Vector<Change> changes = dbIO.readChanges(configScript);
        for (Change change : changes) {
          if (change instanceof ModelChange)
            ((ModelChange) change).apply(db, currentPlatform.isDelimitedIdentifierModeOn());
          else if (change instanceof DataChange && applyConfigScriptData) {
            if (isValidChange(change)) {
              ((DataChange) change).apply(databaseOrgData,
                  currentPlatform.isDelimitedIdentifierModeOn());
            }
          }
          log.info("Change: " + change);
        }
      } else {
        log.info("Couldn't find configuration script for template: " + template + " (file: "
            + configScript.getAbsolutePath() + ")");
      }
    }
  }

  private boolean isValidChange(Change change) {
    if (!(change instanceof ColumnDataChange)) {
      return true;
    }
    return !(((ColumnDataChange) change).getColumnname().equals("SEQNO") && ((ColumnDataChange) change)
        .getTablename().equalsIgnoreCase("AD_FIELD"));
  }

  protected Database createDatabase(String dbModelPath) {
    File dbModel = new File("model", dbModelPath);
    final Platform platform = getPlatform();

    Database newDB = DatabaseUtils.readDatabaseWithConfigScripts(dbModel, platform, dbModelPath,
        true, true, true, true);
    platform.createTables(newDB, false, true);

    platform.enableNOTNULLColumns(newDB);

    ModelComparator comparator = new ModelComparator(platform.getPlatformInfo(),
        platform.isDelimitedIdentifierModeOn());
    List<ModelChange> newChanges = comparator.compare(DatabaseUtils.readDatabaseWithConfigScripts(
        dbModel, platform, dbModelPath, true, true, true, true), platform
        .loadModelFromDatabase(getExcludeFilter()));
    assertThat("changes between updated db and target db", newChanges, is(empty()));
    return newDB;
  }

  /** Reads current DB model */
  protected Database readModelFromDB() {
    return getPlatform().loadModelFromDatabase(getExcludeFilter());
  }

  /**
   * Utility method to update current DB to model defined in modelPath field
   */
  protected Database updateDatabase() {
    return updateDatabase(modelPath);
  }

  /** Utility method to reset current test DB removing all objects it might have */
  protected void resetDB() {
    updateDatabase("empty");
    if (rdbms == Rdbms.PG) {
      Connection cn = null;
      PreparedStatement st = null;
      try {
        cn = getDataSource().getConnection();

        st = cn.prepareStatement("CREATE OR REPLACE VIEW DUAL AS SELECT 'X'::text AS dummy");
        st.execute();
      } catch (Exception e) {
        log.error("Error creating DUAL in PG");
      } finally {
        if (st != null) {
          try {
            st.close();
          } catch (SQLException e) {
            log.error("Error closing statement", e);
          }
        }
        if (cn != null) {
          try {
            cn.close();
          } catch (SQLException e) {
            log.error("Error closing connection", e);
          }
        }
      }
    }
  }

  /** Exports current test DB to xml files within path directory */
  protected void exportDatabase(String path) {
    boolean doPlSqlStandardization = true;
    exportDatabase(path, doPlSqlStandardization);
  }

  /** Exports current test DB to xml files within path directory */
  protected void exportDatabase(String path, boolean doPlSqlStandardization) {
    final DatabaseIO io = new DatabaseIO();
    if (platform == null) {
      platform = getPlatform();
    }
    platform.setMaxThreads(threads);

    Database originalDB = platform
        .loadModelFromDatabase(getExcludeFilter(), doPlSqlStandardization);
    io.writeToDir(originalDB, new File(path));
  }

  /** Reads a Configuration Script and returns the changes it contains */
  protected Vector<Change> readConfigScript(String configScriptPath) {
    File configScriptFile = new File(configScriptPath);
    if (configScriptFile.exists()) {
      log.info("Loading configuration script " + configScriptFile.getAbsolutePath());
      final DatabaseIO dbIO = new DatabaseIO();
      return dbIO.readChanges(configScriptFile);
    }
    log.error("Couldn't find configuration script " + configScriptFile.getAbsolutePath());
    return null;
  }

  /** Exports the changes found between two models into a Configuration Script */
  protected Map<Change, Boolean> exportToConfigScript(Database xmlModel, Database currentDB,
      String exportPath) {
    log.info("Loading model from XML files");
    final DatabaseData databaseOrgData = new DatabaseData(xmlModel);
    log.info("Loading complete model from current database");
    File configScript = new File(exportPath, "configScript.xml");
    Map<Change, Boolean> allChanges = new HashMap<Change, Boolean>();
    try {
      log.info("Comparing models");
      final DataComparator dataComparator = new DataComparator(platform.getSqlBuilder()
          .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
      OBDataset ad = new OBDataset(databaseOrgData);
      dataComparator.compare(xmlModel, currentDB, platform, databaseOrgData, ad, null);
      Vector<Change> changes = new Vector<Change>();
      Vector<Change> notExportedChanges = new Vector<Change>();
      dataComparator.generateConfigScript(changes, notExportedChanges);
      if (!configScript.exists()) {
        configScript.createNewFile();
      }
      log.info("Exporting changes to " + configScript.getAbsolutePath());
      DatabaseIO dbIO = new DatabaseIO();
      dbIO.write(configScript, changes);
      for (final Change c : changes) {
        allChanges.put(c, Boolean.TRUE);
      }
      if (notExportedChanges.size() > 0) {
        log.info("Changes that couldn't be exported to the config script:");
        log.info("*******************************************************");
        for (final Change c : notExportedChanges) {
          allChanges.put(c, Boolean.FALSE);
          log.info(c);
        }
      }
    } catch (Exception ex) {
      log.error("Error exporting changes to config script " + configScript.getAbsolutePath(), ex);
    }
    return allChanges;
  }

  /** returns platform for current execution */
  protected Platform getPlatform() {
    platform = PlatformFactory.createNewPlatformInstance(getDataSource());
    if (evaluator == null) {
      switch (rdbms) {
      case PG:
        evaluator = new PGStandardBatchEvaluator(platform);
        break;
      case ORA:
        evaluator = new Oracle8StandardBatchEvaluator(platform);
        break;
      }
    }
    platform.setBatchEvaluator(evaluator);

    return platform;
  }

  /**
   * Generates dummy data for the model
   * 
   * @param model
   *          model to generate data for
   * @param rows
   *          number of rows to generate
   */
  protected void generateData(Database model, int rows) throws SQLException {
    for (Table table : model.getTables()) {
      if (table.getForeignKeyCount() > 0) {
        // not generating FK data for now
        continue;
      }
      for (int i = 0; i < rows; i++) {
        generateRow(table, false);
      }
    }
  }

  protected String generateRow(Database model, String tableName) throws SQLException {
    for (Table t : model.getTables()) {
      if (t.getName().equalsIgnoreCase(tableName)) {
        return generateRow(t, false);
      }
    }
    return null;
  }

  protected String generateRow(Table table, boolean skipNullables) throws SQLException {
    String tableName = table.getName();
    String sql = "insert into " + tableName + " (";
    boolean first = true;
    String values = "";
    String pkValue = "";
    for (Column col : table.getColumns()) {
      if (skipNullables && !col.isRequired()) {
        continue;
      }
      String colName = col.getName();
      if (!first) {
        sql += ", ";
        values += ", ";
      }
      System.out.println(col.getType() + " - " + col.getTypeCode() + " - " + col.getSize());
      first = false;
      sql += colName;
      boolean isPK = colName.equalsIgnoreCase(tableName + "_id");
      if (isPK) {
        pkValue = RandomStringUtils.random(col.getSizeAsInt(), "0123456789ABCDEF");
        values += "'" + pkValue + "'";
      } else if ("VARCHAR".equals(col.getType()) || "NVARCHAR".equals(col.getType())
          || "CHAR".equals(col.getType()) || "NCHAR".equals(col.getType())) {
        values += "'" + RandomStringUtils.randomAlphanumeric(col.getSizeAsInt()) + "'";
      } else if ("DECIMAL".equals(col.getType())) {
        int scale = col.getScaleAsInt();
        int precision = col.getPrecisionRadix();
        if (scale == 0 && precision == 0) {
          values += RandomStringUtils.randomNumeric(10);
        } else {
          values += RandomStringUtils.randomNumeric(precision - scale)
              + (scale == 0 ? "" : ("." + RandomStringUtils.randomNumeric(scale)));
        }
      } else if ("TIMESTAMP".equals(col.getType())) {

        Date date = new Date((long) (new Random().nextDouble() * (System.currentTimeMillis())));
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");

        values += "to_date('" + sdf.format(date) + "', 'DD/MM/YYYY')";
      } else if ("CLOB".equals(col.getType())) {
        values += "'" + RandomStringUtils.randomAlphanumeric(10) + "'";
      }
    }
    sql += ") values (" + values + ")";
    System.out.println(sql);
    Connection cn = null;
    PreparedStatement st = null;
    try {
      cn = getDataSource().getConnection();

      st = cn.prepareStatement(sql);
      st.execute();
    } finally {
      if (st != null) {
        try {
          st.close();
        } catch (SQLException e) {
          log.error("Error closing statement", e);
        }
      }
      if (cn != null) {
        try {
          cn.close();
        } catch (SQLException e) {
          log.error("Error closing connection", e);
        }
      }
    }

    return pkValue;
  }

  /** gets first value in DB for given column */
  protected String getActualValue(String tableName, String columnName) throws SQLException {
    Connection cn = null;
    PreparedStatement st = null;
    try {
      cn = getDataSource().getConnection();

      st = cn.prepareStatement("select " + columnName + " from " + tableName);

      ResultSet rs = st.executeQuery();

      rs.next();
      return rs.getString(1);
    } finally {
      if (st != null) {
        try {
          st.close();
        } catch (SQLException e) {
          log.error("Error closing statement", e);
        }
      }
      if (cn != null) {
        cn.close();
      }
    }
  }

  protected Row getRowValues(String tableName, String pk) throws SQLException {
    Connection cn = null;
    try {
      cn = getDataSource().getConnection();

      Statement st = cn.createStatement();

      ResultSet rs = st.executeQuery("select *  from " + tableName + " where " + tableName
          + "_id='" + pk + "'");
      ResultSetMetaData md = rs.getMetaData();
      rs.next();
      Row row = new Row();
      for (int i = 1; i <= md.getColumnCount(); i++) {
        row.addColumn(md.getColumnName(i), rs.getString(i));
      }

      return row;
    } finally {
      if (cn != null) {
        cn.close();
      }
    }
  }

  protected void installPgTrgmExtension() {
    if (getRdbms() != Rdbms.PG) {
      return;
    }
    Connection connection = null;
    try {
      connection = getDataSource().getConnection();
      StringBuilder query = new StringBuilder();
      query.append("CREATE EXTENSION IF NOT EXISTS \"pg_trgm\"");
      PreparedStatement st = connection.prepareStatement(query.toString());
      st.execute();
    } catch (SQLException e) {
      log.error("Error while creating pg_trgm extension");
    } finally {
      getPlatform().returnConnection(connection);
    }
    // Configure the exclude filter
    ExcludeFilter localExcludeFilter = new ExcludeFilter();
    localExcludeFilter.fillFromFile(new File("model/excludeFilter/excludePgTrgmFunctions.xml"));
    setExcludeFilter(localExcludeFilter);
  }

  protected void uninstallPgTrgmExtension() {
    if (getRdbms() != Rdbms.PG) {
      return;
    }
    Connection connection = null;
    try {
      connection = getDataSource().getConnection();
      StringBuilder query = new StringBuilder();
      query.append("DROP EXTENSION \"pg_trgm\" CASCADE");
      PreparedStatement st = connection.prepareStatement(query.toString());
      st.execute();
    } catch (SQLException e) {
      log.error("Error while deleting pg_trgm extension");
    } finally {
      getPlatform().returnConnection(connection);
    }
  }

  /** Represents a DB row with its values */
  public class Row {
    private Map<String, String> cols = new HashMap<String, String>();

    public void addColumn(String name, String value) {
      cols.put(name, value);
    }

    @Override
    public String toString() {
      String v = "";
      for (String colName : cols.keySet()) {
        v += " " + colName + " [" + cols.get(colName) + "]";
      }
      return v;
    }

    public Set<String> getColumnNames() {
      return cols.keySet();
    }

    public String getValue(String colName) {
      return cols.get(colName);
    }
  }
}
