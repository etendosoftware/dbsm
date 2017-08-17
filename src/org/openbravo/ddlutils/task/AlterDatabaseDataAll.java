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
import java.sql.Connection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.AddRowChange;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.alteration.RemoveRowChange;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.apache.ddlutils.task.VerbosityLevel;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.openbravo.ddlutils.task.DatabaseUtils.ConfigScriptConfig;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.OBDataset;
import org.openbravo.modulescript.ModuleScriptHandler;
import org.openbravo.utils.CheckSum;

/**
 * 
 * @author adrian
 */
public class AlterDatabaseDataAll extends BaseDatabaseTask {

  protected String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

  protected File prescript = null;
  protected File postscript = null;

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

  public AlterDatabaseDataAll() {
    super();
  }

  @Override
  protected void doExecute() {
    excludeFilter = DBSMOBUtil.getInstance().getExcludeFilter(
        new File(model.getAbsolutePath() + "/../../../"));
    if (onlyIfModified) {
      getLog().info("Checking if database files where modified after last build");
      CheckSum cs = new CheckSum(basedir + "/../");
      String oldStructCS = cs.getCheckSumDBSTructure();
      String newStructCS = cs.calculateCheckSumDBStructure();
      String oldDataCS = cs.getCheckSumDBSourceData();
      String newDataCS = cs.calculateCheckSumDBSourceData();
      if (oldStructCS.equals(newStructCS) && oldDataCS.equals(newDataCS)) {
        getLog().info("Database files didn't change. No update process required.");
        return;
      } else {
        getLog().info("Database files were changed. Initiating database update process.");
      }
    }

    getLog().info("Database connection: " + getUrl() + ". User: " + getUser());

    final BasicDataSource ds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getUser(),
        getPassword());

    final Platform platform = PlatformFactory.createNewPlatformInstance(ds);
    platform.setMaxThreads(threads);
    getLog().info("Max threads " + platform.getMaxThreads());

    if (!StringUtils.isEmpty(forcedRecreation)) {
      getLog().info("Forced recreation: " + forcedRecreation);
    }

    platform.getSqlBuilder().setForcedRecreation(forcedRecreation);
    DBSMOBUtil
        .writeCheckSumInfo(new File(model.getAbsolutePath() + "/../../../").getAbsolutePath());

    getLog().info("Executing full update.database");

    try {

      Database originaldb;
      if (getOriginalmodel() == null) {
        originaldb = platform.loadModelFromDatabase(excludeFilter, true);
        getLog().info("Checking datatypes from the model loaded from the database");
        if (originaldb == null) {
          originaldb = new Database();
          getLog().info("Original model considered empty.");
        } else {
          getLog().info("Original model loaded from database.");
        }
      } else {
        // Load the model from the file
        ConfigScriptConfig config = new ConfigScriptConfig(platform, basedir, true, false, false);
        originaldb = DatabaseUtils.readDatabase(getModel(), config);
        getLog().info("Original model loaded from file.");
      }
      DatabaseInfo databaseInfo = readDatabaseModel(platform, null, originaldb, basedir,
          datafilter, input, strict, true);
      Database db = databaseInfo.getDatabase();
      getLog().info("Checking datatypes from the model loaded from XML files");
      db.checkDataTypes();
      final DatabaseData databaseOrgData = databaseInfo.getDatabaseData();
      databaseOrgData.setStrictMode(strict);
      OBDataset ad = new OBDataset(databaseOrgData, "AD");
      boolean hasBeenModified = DBSMOBUtil.getInstance().hasBeenModified(platform, ad, false);
      if (hasBeenModified) {
        if (force)
          getLog()
              .info(
                  "Database was modified locally, but as update.database command is forced, the database will be updated anyway.");
        else {
          getLog()
              .error(
                  "Database has local changes. Update.database will not be done. You should export your changed modules before doing update.database, so that your Application Dictionary changes are preserved.");
          throw new BuildException("Database has local changes. Update.database not done.");
        }
      }

      // execute the pre-script
      if (getPrescript() == null) {
        // try to execute the default prescript
        final File fpre = new File(getModel(), "prescript-" + platform.getName() + ".sql");
        if (fpre.exists()) {
          getLog().info("Executing default prescript");
          platform.evaluateBatch(DatabaseUtils.readFile(fpre), true);
        }
      } else {
        platform.evaluateBatch(DatabaseUtils.readFile(getPrescript()), true);
      }
      final Database oldModel = (Database) originaldb.clone();
      getLog().info("Updating database model...");
      platform.alterTables(originaldb, db, !isFailonerror());
      getLog().info("Model update complete.");

      // Initialize the ModuleScriptHandler that we will use later, to keep the current module
      // versions, prior to the update
      ModuleScriptHandler hd = new ModuleScriptHandler();
      hd.setModulesVersionMap(DBSMOBUtil.getModulesVersion(platform));

      DBSMOBUtil.getInstance().moveModuleDataFromInstTables(platform, db, null);
      final Connection connection = platform.borrowConnection();
      getLog().info("Comparing databases to find differences");
      final DataComparator dataComparator = new DataComparator(platform.getSqlBuilder()
          .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
      Set<String> adTablesWithRemovedOrInsertedRecords = new HashSet<String>();
      Set<String> adTablesWithRemovedRecords = new HashSet<String>();
      dataComparator.compareToUpdate(db, platform, databaseOrgData, ad, null);
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
      getLog().info("Disabling foreign keys");
      platform.disableDatasetFK(connection, originaldb, ad, !isFailonerror(),
          adTablesWithRemovedOrInsertedRecords);
      getLog().info("Disabling triggers");
      platform.disableAllTriggers(connection, db, !isFailonerror());
      platform.disableNOTNULLColumns(db, ad);

      if (executeModuleScripts) {
        getLog().info("Running modulescripts...");
        // Executing modulescripts
        hd.setBasedir(new File(basedir + "/../"));
        hd.execute();
      } else {
        getLog().info("Skipping modulescripts...");
      }
      getLog().info("Updating Application Dictionary data...");
      platform.alterData(connection, db, dataComparator.getChanges());
      getLog().info("Removing invalid rows.");
      platform.deleteInvalidConstraintRows(db, ad, adTablesWithRemovedRecords, !isFailonerror());
      getLog().info("Recreating Primary Keys");
      List changes = platform.alterTablesRecreatePKs(oldModel, db, !isFailonerror());
      getLog().info("Executing oncreatedefault statements for mandatory columns");
      platform.executeOnCreateDefaultForMandatoryColumns(db, ad);
      getLog().info("Recreating not null constraints");
      platform.enableNOTNULLColumns(db, ad);
      getLog().info("Executing update final script (dropping temporary tables)");
      boolean postscriptCorrect = platform.alterTablesPostScript(oldModel, db, !isFailonerror(),
          changes, null, ad);

      getLog().info("Enabling Foreign Keys and Triggers");
      boolean fksEnabled = platform.enableDatasetFK(connection, originaldb, ad,
          adTablesWithRemovedOrInsertedRecords, true);
      boolean triggersEnabled = platform.enableAllTriggers(connection, db, !isFailonerror());

      // execute the post-script
      if (getPostscript() == null) {
        // try to execute the default prescript
        final File fpost = new File(getModel(), "postscript-" + platform.getName() + ".sql");
        if (fpost.exists()) {
          getLog().info("Executing default postscript");
          platform.evaluateBatch(DatabaseUtils.readFile(fpost), true);
        }
      } else {
        platform.evaluateBatch(DatabaseUtils.readFile(getPostscript()), true);
      }
      DBSMOBUtil.getInstance().updateCRC(platform);
      if (!triggersEnabled) {
        getLog()
            .error(
                "Not all the triggers were correctly activated. The most likely cause of this is that the XML file of the trigger is not correct. If that is the case, please remove/uninstall its module, or recover the sources backup and initiate the rebuild again");
      }
      if (!fksEnabled) {
        getLog()
            .error(
                "Not all the foreign keys were correctly activated. Please review which ones were not, and fix the missing references, or recover the backup of your sources.");
      }
      if (!postscriptCorrect) {
        getLog()
            .error(
                "Not all the commands in the final update step were executed correctly. This likely means at least one foreign key was not activated successfully. Please review which one, and fix the missing references, or recover the backup of your sources.");
      }
      if (!triggersEnabled || !fksEnabled || !postscriptCorrect) {
        throw new Exception(
            "There were serious problems while updating the database. Please review and fix them before continuing with the application rebuild");

      }

      final DataComparator dataComparator2 = new DataComparator(platform.getSqlBuilder()
          .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
      dataComparator2.compare(db, db, platform, databaseOrgData, ad, null);
      Vector<Change> finalChanges = new Vector<Change>();
      Vector<Change> notExportedChanges = new Vector<Change>();
      dataComparator2.generateConfigScript(finalChanges, notExportedChanges);

      final DatabaseIO dbIO = new DatabaseIO();

      final File configFile = new File("formalChangesScript.xml");
      dbIO.write(configFile, finalChanges);
    } catch (final Exception e) {
      // log(e.getLocalizedMessage());
      e.printStackTrace();
      throw new BuildException(e);
    }
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

  protected Database readDatabaseModel() {
    return new Database();
  }

  protected DatabaseInfo readDatabaseModel(Platform platform, DatabaseData databaseOrgData,
      Database originaldb, String basedir, String datafilter, File input, boolean strict,
      boolean applyConfigScriptData) {
    Database db = null;
    if (basedir == null) {
      getLog()
          .info("Basedir for additional files not specified. Updating database with just Core.");
      ConfigScriptConfig config = new ConfigScriptConfig(platform, basedir, strict,
          applyConfigScriptData, false);
      db = DatabaseUtils.readDatabase(getModel(), config);
    } else {
      // We read model files using the filter, obtaining a file array.The models will be merged to
      // create a final target model.
      final Vector<File> dirs = new Vector<File>();
      dirs.add(getModel());
      final DirectoryScanner dirScanner = new DirectoryScanner();
      dirScanner.setBasedir(new File(basedir));
      final String[] dirFilterA = { dirFilter };
      dirScanner.setIncludes(dirFilterA);
      dirScanner.scan();
      final String[] incDirs = dirScanner.getIncludedDirectories();
      for (int j = 0; j < incDirs.length; j++) {
        final File dirF = new File(basedir, incDirs[j]);
        dirs.add(dirF);
      }
      final File[] fileArray = new File[dirs.size()];
      for (int i = 0; i < dirs.size(); i++) {
        fileArray[i] = dirs.get(i);
      }
      getLog().info("Reading model files...");
      ConfigScriptConfig config = new ConfigScriptConfig(platform, basedir, strict,
          applyConfigScriptData, false);
      db = DatabaseUtils.readDatabase(fileArray, config);
    }
    DatabaseData dbData = new DatabaseData(db);
    DBSMOBUtil.getInstance().loadDataStructures(platform, dbData, originaldb, db, basedir,
        datafilter, input, strict, false);

    return new DatabaseInfo(db, dbData);
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

  public File getPrescript() {
    return prescript;
  }

  public void setPrescript(File prescript) {
    this.prescript = prescript;
  }

  public File getPostscript() {
    return postscript;
  }

  public void setPostscript(File postscript) {
    this.postscript = postscript;
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

  /**
   * Helper class that contains the database and the databaseData information.
   */
  protected static class DatabaseInfo {

    private Database database;
    private DatabaseData databaseData;

    private DatabaseInfo(Database database, DatabaseData databaseData) {
      this.database = database;
      this.databaseData = databaseData;
    }

    protected Database getDatabase() {
      return database;
    }

    protected void setDatabase(Database database) {
      this.database = database;
    }

    protected DatabaseData getDatabaseData() {
      return databaseData;
    }

    protected void setDatabaseData(DatabaseData databaseData) {
      this.databaseData = databaseData;
    }

  }

}
