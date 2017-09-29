/*
 ************************************************************************************
 * Copyright (C) 2001-2017 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.ddlutils.task;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.apache.ddlutils.task.VerbosityLevel;
import org.openbravo.ddlutils.process.DBUpdater;
import org.openbravo.ddlutils.task.DatabaseUtils.ConfigScriptConfig;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.service.system.SystemService;

/**
 * 
 * @author adrian
 */
public class AlterDatabaseDataAll extends BaseDatabaseTask {

  protected String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

  protected File input;
  protected String encoding = "UTF-8";
  protected File originalmodel;
  protected File model;
  protected boolean failonerror = false;

  protected String object = null;

  protected String basedir;
  protected String dirFilter;
  protected String datadir;
  protected String datafilter;
  protected boolean force = false;
  protected boolean onlyIfModified = false;
  protected boolean strict;

  protected ExcludeFilter excludeFilter;

  private String forcedRecreation = "";

  private boolean executeModuleScripts = true;

  private int threads = 0;

  @Override
  protected void doExecute() {
    DBUpdater dbUpdater = getDBUpater();

    dbUpdater.update();
  }

  protected DBUpdater getDBUpater() {
    getLog().info("Database connection: " + getUrl() + ". User: " + getUser());
    BasicDataSource ds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getUser(), getPassword());
    Platform platform = PlatformFactory.createNewPlatformInstance(ds);
    platform.setMaxThreads(threads);
    if (!StringUtils.isEmpty(forcedRecreation)) {
      getLog().info("Forced recreation: " + forcedRecreation);
    }
    platform.getSqlBuilder().setForcedRecreation(forcedRecreation);

    DBUpdater dbUpdater = new DBUpdater();
    dbUpdater.setLog(getLog());
    dbUpdater.setExcludeFilter(DBSMOBUtil.getInstance().getExcludeFilter(
        new File(model.getAbsolutePath() + "/../../../")));
    dbUpdater.setPlatform(platform);
    dbUpdater.setModel(model);
    dbUpdater.setBasedir(basedir);
    dbUpdater.setStrict(strict);
    dbUpdater.setFailonerror(failonerror);
    dbUpdater.setForce(force);
    dbUpdater.setExecuteModuleScripts(executeModuleScripts);
    dbUpdater.setDatafilter(datafilter);
    dbUpdater.setBaseSrcAD(input);
    dbUpdater.setDirFilter(dirFilter);
    dbUpdater.setDatasetName("AD");
    dbUpdater.setCheckDBModified(true);
    dbUpdater.setCheckFormalChanges(true);
    dbUpdater.setUpdateModuleInstallTables(true);
    return dbUpdater;
  }

  public static void main(String[] args) throws FileNotFoundException, IOException {
    String workingDir = System.getProperty("user.dir");
    System.out.println("Working directory: " + workingDir);

    Properties props = new Properties();
    props.load(new FileInputStream(workingDir + "/config/Openbravo.properties"));
    AlterDatabaseDataAll task = new AlterDatabaseDataAll();
    String baseDir = workingDir + "/src-db/database";

    task.setDriver(props.getProperty("bbdd.driver"));

    String ownerUrl;
    if ("POSTGRE".equals(props.getProperty("bbdd.rdbms"))) {
      ownerUrl = props.getProperty("bbdd.url") + "/" + props.getProperty("bbdd.sid");
    } else {
      ownerUrl = props.getProperty("bbdd.url");
    }

    task.setUrl(ownerUrl);
    task.setUser(props.getProperty("bbdd.user"));
    task.setPassword(props.getProperty("bbdd.password"));
    task.setExcludeobjects(props.getProperty("com.openbravo.db.OpenbravoExcludeFilter"));
    task.setModel(new File(baseDir + "/model"));

    task.setInput(new File(baseDir + "/sourcedata"));
    task.setObject(props.getProperty("bbdd.object"));
    task.setBasedir(workingDir + "/modules");
    task.setDirFilter("*/src-db/database/model");
    task.setDatadir(workingDir + "/modules");
    task.setDatafilter("*/src-db/database/sourcedata");
    task.setForce(false);
    task.setFailonerror(false);

    task.setVerbosity(new VerbosityLevel("DEBUG"));

    task.execute();
  }

  /**
   * This method is only invoked from GenerateProcess task defined in ezattributes module and it is
   * required to maintain backwards compatibility and to ensures that the API is not broken.
   * 
   * Avoid uses loadDataStructures because process that invokes this method executes it.
   */
  protected Database readDatabaseModel() {
    // Set input file and datafilter needed in loadDataStructures
    input = new File(basedir + "/../src-db/database/sourcedata");
    datafilter = "*/src-db/database/sourcedata";

    boolean applyConfigScriptData = false;
    ConfigScriptConfig config = new ConfigScriptConfig(SystemService.getInstance().getPlatform(),
        basedir + "/../", strict, applyConfigScriptData);

    Database db = null;
    String modulesBaseDir = config.getBasedir() + "modules/";
    if (config.getBasedir() == null) {
      getLog()
          .info("Basedir for additional files not specified. Updating database with just Core.");

      modulesBaseDir = null;
      db = DatabaseUtils.readDatabase(getModel(), config);
    } else {
      final File[] fileArray = getDBUpater().readModelFiles(modulesBaseDir);
      getLog().info("Reading model files...");
      db = DatabaseUtils.readDatabase(fileArray, config);
    }

    return db;
  }

  public String getExcludeobjects() {
    return excludeobjects;
  }

  public void setExcludeobjects(String excludeobjects) {
    this.excludeobjects = excludeobjects;
  }

  public File getOriginalmodel() {
    return originalmodel;
  }

  public void setOriginalmodel(File input) {
    this.originalmodel = input;
  }

  public File getModel() {
    return model;
  }

  public void setModel(File model) {
    this.model = model;
  }

  public String getBasedir() {
    return basedir;
  }

  public void setBasedir(String basedir) {
    this.basedir = basedir;
  }

  public String getDirFilter() {
    return dirFilter;
  }

  public void setDirFilter(String dirFilter) {
    this.dirFilter = dirFilter;
  }

  public boolean isFailonerror() {
    return failonerror;
  }

  public void setFailonerror(boolean failonerror) {
    this.failonerror = failonerror;
  }

  /**
   * Functionality for deleting data during create.database was removed. Function is kept to not
   * require lock-step update of dbsm.jar & build-create.xml
   */
  @Deprecated
  public void setFilter(String filter) {
  }

  public File getInput() {
    return input;
  }

  public void setInput(File input) {
    this.input = input;
  }

  public String getEncoding() {
    return encoding;
  }

  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  public void setObject(String object) {
    if (object == null || object.trim().startsWith("$") || object.trim().equals("")) {
      this.object = null;
    } else {
      this.object = object;
    }
  }

  public String getObject() {
    return object;
  }

  public String getDatadir() {
    return datadir;
  }

  public void setDatadir(String datadir) {
    this.datadir = datadir;
  }

  public String getDatafilter() {
    return datafilter;
  }

  public void setDatafilter(String datafilter) {
    this.datafilter = datafilter;
  }

  public boolean isForce() {
    return force;
  }

  public void setForce(boolean force) {
    this.force = force;
  }

  public boolean isOnlyIfModified() {
    return onlyIfModified;
  }

  public void setOnlyIfModified(boolean onlyIfModified) {
    this.onlyIfModified = onlyIfModified;
  }

  public boolean isStrict() {
    return strict;
  }

  public void setStrict(boolean strict) {
    this.strict = strict;
  }

  public void setForcedRecreation(String forcedRecreation) {
    this.forcedRecreation = forcedRecreation;
  }

  public void setExecuteModuleScripts(boolean executeModuleScripts) {
    this.executeModuleScripts = executeModuleScripts;
  }

  /** Defines how many threads can be used to execute parallelizable tasks */
  public void setThreads(int threads) {
    this.threads = threads;
  }

}
