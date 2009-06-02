/*
 ************************************************************************************
 * Copyright (C) 2001-2006 Openbravo S.L.
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
import java.sql.Connection;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.DataChange;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.alteration.ModelChange;
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DataToArraySink;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.openbravo.dal.core.DalLayerInitializer;
import org.openbravo.dal.service.OBDal;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.utils.CheckSum;

/**
 * 
 * @author adrian
 */
public class AlterDatabaseDataAll extends BaseDalInitializingTask {

  protected String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

  protected File prescript = null;
  protected File postscript = null;

  protected String filter = "org.apache.ddlutils.io.NoneDatabaseFilter";

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

  /** Creates a new instance of ReadDataXML */
  public AlterDatabaseDataAll() {
    super();
  }

  public void execute() {
    if (!onlyIfModified) {
      System.out
          .println("Executing database update process without checking changes in local files.");
    } else {
      CheckSum cs = new CheckSum(basedir + "/../");
      String oldStructCS = cs.getCheckSumDBSTructure();
      String newStructCS = cs.calculateCheckSumDBStructure();
      String oldDataCS = cs.getCheckSumDBSourceData();
      String newDataCS = cs.calculateCheckSumDBSourceData();
      if (oldStructCS.equals(newStructCS) && oldDataCS.equals(newDataCS)) {
        System.out.println("Database files didn't change. No update process required.");
        return;
      } else {
        System.out.println("Database files were changed. Initiating database update process.");
      }
    }
    super.execute();
  }

  @Override
  public void doExecute() {

    getLog().info("Database connection: " + getUrl() + ". User: " + getUser());

    final BasicDataSource ds = new BasicDataSource();
    ds.setDriverClassName(getDriver());
    ds.setUrl(getUrl());
    ds.setUsername(getUser());
    ds.setPassword(getPassword());
    if (getDriver().contains("Oracle"))
      ds.setValidationQuery("SELECT 1 FROM DUAL");
    else
      ds.setValidationQuery("SELECT 1");
    ds.setTestOnBorrow(true);

    final Platform platform = PlatformFactory.createNewPlatformInstance(ds);
    // platform.setDelimitedIdentifierModeOn(true);

    boolean hasBeenModified = DBSMOBUtil.getInstance().hasBeenModified(platform, false);
    if (hasBeenModified) {
      if (force)
        getLog()
            .info(
                "Database was modified locally, but as update.database command is forced, the database will be updated anyway.");
      else {
        getLog()
            .info(
                "Database has local changes. Update.database will not be done. If you want to force the update.database, do: ant update.database -Dforce=yes (you will lose all your changes in the application dictionary if you do it)");
        throw new BuildException("Database has local changes. Update.database not done.");
      }
    }

    try {
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

      Database originaldb;
      if (getOriginalmodel() == null) {
        originaldb = platform.loadModelFromDatabase(DatabaseUtils.getExcludeFilter(excludeobjects));
        if (originaldb == null) {
          originaldb = new Database();
          getLog().info("Original model considered empty.");
        } else {
          getLog().info("Original model loaded from database.");
        }
      } else {
        // Load the model from the file
        originaldb = DatabaseUtils.readDatabase(getModel());
        getLog().info("Original model loaded from file.");
      }

      Database db = null;
      db = readDatabaseModel();
      final DatabaseData databaseOrgData = new DatabaseData(db);
      DBSMOBUtil.getInstance().deleteInstallTables(platform, db);
      loadDataStructures(platform, databaseOrgData, originaldb, db);

      getLog().info("Comparing databases to find differences");
      final DataComparator dataComparatorDS = new DataComparator(platform.getSqlBuilder()
          .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
      dataComparatorDS.compareUsingDALToUpdate(db, platform, databaseOrgData, "DS", null);

      OBDal.getInstance().commitAndClose();

      final Database oldModel = (Database) originaldb.clone();
      getLog().info("Updating database model...");
      platform.alterTables(originaldb, db, !isFailonerror());
      getLog().info("Model update complete.");

      getLog().info("Disabling foreign keys");
      final Connection connection = platform.borrowConnection();
      platform.disableAllFK(connection, originaldb, !isFailonerror());
      getLog().info("Disabling triggers");
      platform.disableAllTriggers(connection, db, !isFailonerror());

      if (dataComparatorDS.getChanges().size() > 0) {
        getLog().info("Dataset DS has changed. We need to update it.");
        platform.alterData(connection, db, dataComparatorDS.getChanges());
        getLog().info("Dataset DS updated succesfully. Reinitializing DAL");
        DalLayerInitializer.getInstance().initialize(true);

      }

      final DataComparator dataComparator = new DataComparator(platform.getSqlBuilder()
          .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
      dataComparator.compareUsingDALToUpdate(db, platform, databaseOrgData, "ADCS", null);
      OBDal.getInstance().commitAndClose();
      getLog().info("Data changes we will perform: ");
      for (final Change change : dataComparator.getChanges())
        getLog().info(change);
      getLog().info("Updating database data...");
      platform.alterData(connection, db, dataComparator.getChanges());
      getLog().info("Removing invalid rows.");
      platform.deleteInvalidConstraintRows(db, !isFailonerror());
      getLog().info("Executing update final script (NOT NULLs and dropping temporal tables");
      platform.alterTablesPostScript(oldModel, db, !isFailonerror());

      getLog().info("Enabling Foreign Keys and Triggers");
      platform.enableAllFK(connection, originaldb, !isFailonerror());
      platform.enableAllTriggers(connection, db, !isFailonerror());

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

    } catch (final Exception e) {
      // log(e.getLocalizedMessage());
      e.printStackTrace();
      throw new BuildException(e);
    }
  }

  protected Database readDatabaseModel() {
    Database db = null;
    if (basedir == null) {
      getLog()
          .info("Basedir for additional files not specified. Updating database with just Core.");
      db = DatabaseUtils.readDatabase(getModel());
    } else {
      // We read model files using the filter, obtaining a file array.
      // The models will be merged
      // to create a final target model.
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
      db = DatabaseUtils.readDatabase(fileArray);
    }
    return db;
  }

  protected void loadDataStructures(Platform platform, DatabaseData databaseOrgData,
      Database originaldb, Database db) {
    final DatabaseDataIO dbdio = new DatabaseDataIO();
    dbdio.setEnsureFKOrder(false);
    dbdio.setDatabaseFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), originaldb));

    final Vector<File> files = new Vector<File>();
    final File[] sourceFiles = DatabaseUtils.readFileArray(getInput());
    for (int i = 0; i < sourceFiles.length; i++) {
      files.add(sourceFiles[i]);
    }

    final String token = datafilter;
    final DirectoryScanner dirScanner = new DirectoryScanner();
    dirScanner.setBasedir(new File(basedir));
    final String[] dirFilterA = { token };
    dirScanner.setIncludes(dirFilterA);
    dirScanner.scan();
    final String[] incDirs = dirScanner.getIncludedDirectories();
    Vector<File> configScripts = new Vector<File>();
    for (int j = 0; j < incDirs.length; j++) {
      final File dirFolder = new File(basedir, incDirs[j] + "/");
      final File[] fileArray = DatabaseUtils.readFileArray(dirFolder);
      for (int i = 0; i < fileArray.length; i++) {
        files.add(fileArray[i]);
      }
      File configScript = new File(dirFolder.getParentFile(), "configScript.xml");
      if (configScript.exists()) {
        configScripts.add(configScript);
      }
    }
    final DataReader dataReader = dbdio.getConfiguredCompareDataReader(db);
    getLog().info("Loading data from XML files");
    for (int i = 0; i < files.size(); i++) {
      try {
        dataReader.getSink().start();
        final String tablename = files.get(i).getName().substring(0,
            files.get(i).getName().length() - 4);
        final Vector<DynaBean> vectorDynaBeans = ((DataToArraySink) dataReader.getSink())
            .getVector();
        dataReader.parse(files.get(i));
        databaseOrgData.insertDynaBeansFromVector(tablename, vectorDynaBeans);
        dataReader.getSink().end();
      } catch (final Exception e) {
        e.printStackTrace();
      }
    }

    getLog().info("Loading and applying configuration scripts");
    DatabaseIO dbIO = new DatabaseIO();
    for (File f : configScripts) {
      getLog().info("Loading configuration script: " + f.getAbsolutePath());
      Vector<Change> changes = dbIO.readChanges(f);
      for (Change change : changes) {
        if (change instanceof ModelChange)
          ((ModelChange) change).apply(db, platform.isDelimitedIdentifierModeOn());
        else if (change instanceof DataChange)
          ((DataChange) change).apply(databaseOrgData, platform.isDelimitedIdentifierModeOn());
        getLog().debug(change);
      }
    }
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

  public void setFilter(String filter) {
    this.filter = filter;
  }

  public String getFilter() {
    return filter;
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
}
