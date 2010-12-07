/*
 ************************************************************************************
 * Copyright (C) 2001-2006 Openbravo S.L.U.
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
import java.util.List;
import java.util.Vector;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
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
  protected boolean strict;

  protected ExcludeFilter excludeFilter;

  public AlterDatabaseDataAll() {
    super();
  }

  @Override
  protected void doExecute() {
    excludeFilter = DBSMOBUtil.getInstance().getExcludeFilter(
        new File(model.getAbsolutePath() + "/../../../"));

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

    getLog().info("Database connection: " + getUrl() + ". User: " + getUser());

    final BasicDataSource ds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getUser(),
        getPassword());

    final Platform platform = PlatformFactory.createNewPlatformInstance(ds);
    // platform.setDelimitedIdentifierModeOn(true);
    DBSMOBUtil
        .writeCheckSumInfo(new File(model.getAbsolutePath() + "/../../../").getAbsolutePath());

    getLog().info("Executing full update.database");

    try {

      Database originaldb;
      if (getOriginalmodel() == null) {
        originaldb = platform.loadModelFromDatabase(excludeFilter);
        log.info("Checking datatypes from the model loaded from the database");
        originaldb.checkDataTypes();
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
      log.info("Checking datatypes from the model loaded from XML files");
      db.checkDataTypes();
      final DatabaseData databaseOrgData = new DatabaseData(db);
      databaseOrgData.setStrictMode(strict);
      DBSMOBUtil.getInstance().deleteInstallTables(platform, db);
      DBSMOBUtil.getInstance().loadDataStructures(platform, databaseOrgData, originaldb, db,
          basedir, datafilter, input, strict);
      OBDataset ad = new OBDataset(databaseOrgData, "AD");
      boolean hasBeenModified = DBSMOBUtil.getInstance().hasBeenModified(platform, ad, false);
      if (hasBeenModified) {
        if (force)
          getLog()
              .info(
                  "Database was modified locally, but as update.database command is forced, the database will be updated anyway.");
        else {
          getLog()
              .info(
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

      getLog().info("Disabling foreign keys");
      final Connection connection = platform.borrowConnection();
      platform.disableAllFK(connection, originaldb, !isFailonerror());
      getLog().info("Disabling triggers");
      platform.disableAllTriggers(connection, db, !isFailonerror());
      platform.disableNOTNULLColumns(db, ad);

      // Executing modulescripts

      ModuleScriptHandler hd = new ModuleScriptHandler();
      hd.setBasedir(new File(basedir + "/../"));
      hd.execute();

      getLog().info("Comparing databases to find differences");
      final DataComparator dataComparator = new DataComparator(platform.getSqlBuilder()
          .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
      dataComparator.compareToUpdate(db, platform, databaseOrgData, ad, null);
      getLog().info("Updating Application Dictionary data...");
      platform.alterData(connection, db, dataComparator.getChanges());
      getLog().info("Removing invalid rows.");
      platform.deleteInvalidConstraintRows(db, !isFailonerror());
      getLog().info("Recreating Primary Keys");
      List changes = platform.alterTablesRecreatePKs(oldModel, db, !isFailonerror());
      getLog().info("Executing update final script (NOT NULLs and dropping temporary tables)");
      platform.executeOnCreateDefaultForMandatoryColumns(db);
      platform.enableNOTNULLColumns(db, ad);
      platform.alterTablesPostScript(oldModel, db, !isFailonerror(), changes, null);

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

  public boolean isStrict() {
    return strict;
  }

  public void setStrict(boolean strict) {
    this.strict = strict;
  }
}
