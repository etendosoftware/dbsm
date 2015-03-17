/*
 ************************************************************************************
 * Copyright (C) 2015 Openbravo S.L.U.
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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.alteration.ModelComparator;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
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
  private BasicDataSource ds;
  private Rdbms rdbms;

  public enum Rdbms {
    PG, ORA
  }

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

    this.ds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getUser(), getPassword());
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

  private void initLogging() {
    final Properties props = new Properties();
    final String level = Level.DEBUG.toString();

    props.setProperty("log4j.rootCategory", level + ",A");
    props.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");

    props.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
    props.setProperty("log4j.appender.A.layout.ConversionPattern", "%-4r %-5p %c - %m%n");

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
    return new ExcludeFilter();
  }

  protected BasicDataSource getDataSource() {
    return ds;
  }

  public Rdbms getRdbms() {
    return rdbms;
  }

  protected Database updateDatabase(String dbModelPath) {
    return updateDatabase(dbModelPath, null, null);
  }

  /**
   * Utility method to to update current DB to model defined in dbModelPath
   */
  protected Database updateDatabase(String dbModelPath, String adDirectoryName,
      List<String> adTableNames) {

    boolean failOnError = true;
    File dbModel = new File("model", dbModelPath);
    final Platform platform = getPlatform();
    Database originalDB = platform.loadModelFromDatabase(getExcludeFilter());
    Database newDB = DatabaseUtils.readDatabase(dbModel);

    final DatabaseData databaseOrgData = new DatabaseData(newDB);
    databaseOrgData.setStrictMode(false);

    if (adDirectoryName != null) {
      DBSMOBUtil.getInstance().loadDataStructures(platform, databaseOrgData, originalDB, newDB,
          new File(adDirectoryName).getAbsolutePath(), "none", new File(adDirectoryName), false,
          false);
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
    log.info("Disabling foreign keys");
    final Connection connection = platform.borrowConnection();
    platform.disableDatasetFK(connection, originalDB, ad, false);
    log.info("Disabling triggers");
    platform.disableAllTriggers(connection, newDB, false);
    platform.disableNOTNULLColumns(newDB, ad);

    // // Executing modulescripts
    //
    // ModuleScriptHandler hd = new ModuleScriptHandler();
    // hd.setBasedir(new File(basedir + "/../"));
    // hd.execute();
    //
    // // Now we apply the configuration scripts
    // DBSMOBUtil.getInstance().applyConfigScripts(platform, databaseOrgData, db, basedir, false,
    // true);

    log.info("Comparing databases to find differences");
    final DataComparator dataComparator = new DataComparator(platform.getSqlBuilder()
        .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
    try {
      dataComparator.compareToUpdate(newDB, platform, databaseOrgData, ad, null);
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    log.info("Updating Application Dictionary data...");
    platform.alterData(connection, newDB, dataComparator.getChanges());
    log.info("Removing invalid rows.");
    platform.deleteInvalidConstraintRows(newDB, ad, false);
    log.info("Recreating Primary Keys");
    List changes = platform.alterTablesRecreatePKs(oldModel, newDB, false);
    log.info("Executing oncreatedefault statements for mandatory columns");
    platform.executeOnCreateDefaultForMandatoryColumns(newDB);
    log.info("Recreating not null constraints");
    platform.enableNOTNULLColumns(newDB, ad);
    log.info("Executing update final script (dropping temporary tables)");
    boolean postscriptCorrect = platform.alterTablesPostScript(oldModel, newDB, false, changes,
        null);

    assertThat("Postscript should be correct", postscriptCorrect, is(true));

    log.info("Enabling Foreign Keys and Triggers");
    boolean fksEnabled = platform.enableDatasetFK(connection, originalDB, ad, true);
    boolean triggersEnabled = platform.enableAllTriggers(connection, newDB, false);

    // Now check the new model updated in db is as it should
    ModelComparator comparator = new ModelComparator(platform.getPlatformInfo(),
        platform.isDelimitedIdentifierModeOn());
    @SuppressWarnings("unchecked")
    List<Change> newChanges = comparator.compare(DatabaseUtils.readDatabase(dbModel),
        platform.loadModelFromDatabase(getExcludeFilter()));
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
  }

  /** Exports current test DB to xml files within path directory */
  protected void exportDatabase(String path) {
    final DatabaseIO io = new DatabaseIO();
    final Platform platform = getPlatform();
    Database originalDB = platform.loadModelFromDatabase(getExcludeFilter());
    io.writeToDir(originalDB, new File(path));
  }

  /** returns platform for current execution */
  protected Platform getPlatform() {
    return PlatformFactory.createNewPlatformInstance(getDataSource());
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
        String sql = "insert into " + table.getName() + " (";
        boolean first = true;
        String values = "";
        for (Column col : table.getColumns()) {
          col.getName();
          if (!first) {
            sql += ", ";
            values += ", ";
          }
          System.out.println(col.getType() + " - " + col.getTypeCode() + " - " + col.getSize());
          first = false;
          sql += col.getName();
          if ("VARCHAR".equals(col.getType()) || "NVARCHAR".equals(col.getType())) {
            values += "'" + RandomStringUtils.randomAlphanumeric(col.getSizeAsInt()) + "'";
          } else if ("DECIMAL".equals(col.getType())) {
            values += RandomStringUtils.randomNumeric(col.getSizeAsInt());
          }
        }
        sql += ") values (" + values + ")";
        System.out.println(sql);
        Connection cn = null;
        try {
          cn = getDataSource().getConnection();

          PreparedStatement st = cn.prepareStatement(sql);
          st.execute();
        } finally {
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
  }
}
