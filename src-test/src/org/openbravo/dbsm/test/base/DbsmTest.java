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

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
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
    props.setProperty("log4j.logger.org.hibernate", "WARN");

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

  /**
   * Utility method to to update current DB to model defined in dbModelPath
   */
  protected Database updateDatabase(String dbModelPath) {
    File dbModel = new File("model", dbModelPath);
    final Platform platform = getPlatform();
    Database originalDB = platform.loadModelFromDatabase(getExcludeFilter());
    Database newDB = DatabaseUtils.readDatabase(dbModel);
    platform.alterTables(originalDB, newDB, false);
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
