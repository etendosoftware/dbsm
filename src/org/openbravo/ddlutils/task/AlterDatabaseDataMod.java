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
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.AddForeignKeyChange;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.alteration.ModelComparator;
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DataToArraySink;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.openbravo.ddlutils.task.DatabaseUtils.ConfigScriptConfig;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.ModuleRow;
import org.openbravo.ddlutils.util.OBDataset;
import org.openbravo.modulescript.ModuleScriptHandler;

/**
 * 
 * @author adrian
 */
public class AlterDatabaseDataMod extends BaseDatabaseTask {

  private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

  private File prescript = null;
  private File postscript = null;

  private File input;
  private String encoding = "UTF-8";
  private File originalmodel;
  private File model;
  private boolean failonerror = false;

  private String object = null;
  private String baseConfig;
  private String basedir;
  private String dirFilter;
  private String datadir;
  private String datafilter;
  private String module;
  private boolean force;
  protected boolean strict;

  private boolean customLogging = true;

  public AlterDatabaseDataMod() {
    super();
  }

  @Override
  protected void doExecute() {
    getLog()
        .info(
            "Note: this task doesn't work with modules with more than one dbprefix. You should be using the normal update.database task instead.");
    getLog().info("Database connection: " + getUrl() + ". User: " + getUser());

    if (module == null || module.equals("")) {
      getLog()
          .error(
              "This task requires a module name to be passed as parameter. Example: ant update.database.mod -Dmodule=modulename");
      throw new BuildException("No module name provided.");
    }
    final BasicDataSource ds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getUser(),
        getPassword());

    final Platform platform = PlatformFactory.createNewPlatformInstance(ds);

    getLog().info("Creating submodel for application dictionary");
    Database dbXML = null;
    if (basedir == null) {
      getLog()
          .info("Basedir for additional files not specified. Updating database with just Core.");
      dbXML = DatabaseUtils.readDatabaseWithoutConfigScript(getModel());
    } else {
      final Vector<File> dirs = new Vector<File>();
      dirs.add(model);
      final DirectoryScanner dirScanner = new DirectoryScanner();
      dirScanner.setBasedir(new File(basedir));
      final String[] dirFilterA = { dirFilter };
      dirScanner.setIncludes(dirFilterA);
      dirScanner.scan();
      final String[] incDirs = dirScanner.getIncludedDirectories();
      for (int j = 0; j < incDirs.length; j++) {
        final File dirF = new File(basedir, incDirs[j]);
        if (dirF.exists())
          dirs.add(dirF);
        else
          getLog().warn("Directory " + dirF.getAbsolutePath() + " doesn't exist.");
      }
      final File[] fileArray = new File[dirs.size()];
      for (int i = 0; i < dirs.size(); i++) {
        fileArray[i] = dirs.get(i);
      }
      dbXML = DatabaseUtils.readDatabaseWithoutConfigScript(fileArray);
    }

    DatabaseData databaseFullData = new DatabaseData(dbXML);
    databaseFullData.setStrictMode(strict);
    DBSMOBUtil.getInstance().loadDataStructures(platform, databaseFullData, dbXML, dbXML, basedir,
        "*/src-db/database/sourcedata", input, strict);
    OBDataset ad = new OBDataset(databaseFullData, "AD");
    boolean hasBeenModified = DBSMOBUtil.getInstance().hasBeenModified(platform, ad, false);
    if (hasBeenModified) {
      if (force)
        getLog()
            .info(
                "Database was modified locally, but as update.database command is forced, the database will be updated anyway.");
      else {
        getLog()
            .info(
                "Database has local changes. Update.database will not be done. You should export your modules before doing update.database, so that your Application Dictionary changes are preserved. If you don't mind losing them, you can force the update.database by doing: ant update.database -Dforce=true");
        throw new BuildException("Database has local changes. Update.database not done.");
      }
    }
    DBSMOBUtil.resetInstance();
    // Initialize the ModuleScriptHandler that we will use later, to keep the current module
    // versions, prior to the update
    ModuleScriptHandler hd = new ModuleScriptHandler();
    hd.setModulesVersionMap(DBSMOBUtil.getModulesVersion(platform));

    DBSMOBUtil.getInstance().moveModuleDataFromInstTables(platform, dbXML, module);
    ExcludeFilter excludeFilter = DBSMOBUtil.getInstance().getExcludeFilter(
        new File(model.getAbsolutePath() + "/../../../"));
    DBSMOBUtil.getInstance().getModules(platform, excludeFilter);

    Database completedb = null;
    Database dbAD = null;
    try {
      completedb = (Database) dbXML.clone();
      dbAD = (Database) dbXML.clone();
      dbAD.filterByDataset(ad);
    } catch (final Exception e) {
      e.printStackTrace();
    }

    final Vector<Vector<Change>> dataChanges = new Vector<Vector<Change>>();
    final Vector<Database> moduleModels = new Vector<Database>();
    final Vector<Database> moduleOldModels = new Vector<Database>();
    final Vector<ModuleRow> moduleRows = new Vector<ModuleRow>();

    boolean fullUpdate = false;

    if (module.toUpperCase().contains("CORE") || module.equals("%")) {
      getLog()
          .info(
              "You've either specified a list that contains Core, or module specified is %. Complete update.database will be performed.");
      fullUpdate = true;
    }
    if (DBSMOBUtil.getInstance().listDependsOnTemplate(module)) {
      getLog()
          .info(
              "One of the modules you've specified either is an industry template or depends on an industry template. Complete update.database will be performed.");
      fullUpdate = true;
    }
    if (fullUpdate) {
      final AlterDatabaseDataAll ada = new AlterDatabaseDataAll();
      ada.setDriver(getDriver());
      ada.setUrl(getUrl());
      ada.setUser(getUser());
      ada.setPassword(getPassword());
      ada.setExcludeobjects(excludeobjects);
      ada.setModel(model);
      ada.setInput(input);
      ada.setObject(object);
      ada.setFailonerror(failonerror);
      ada.setVerbosity(getVerbosity());
      ada.setBasedir(basedir);
      ada.setDirFilter(dirFilter);
      ada.setDatadir(datadir);
      ada.setDatafilter(datafilter);
      ada.setLog(getLog());
      ada.setForce(isForce());
      ada.execute();
      return;
    }
    Database originaldb = null;
    DBSMOBUtil.resetInstance();
    DBSMOBUtil.getInstance().getModules(platform, excludeFilter);
    final StringTokenizer st = new StringTokenizer(module, ",");
    try {
      while (st.hasMoreElements()) {
        final String modName = st.nextToken().trim();
        getLog().info("Updating module: " + modName);
        final ModuleRow row = DBSMOBUtil.getInstance().getRowFromDir(modName);
        moduleRows.add(row);
        if (row == null)
          throw new BuildException("Module " + modName + " not found in AD_MODULE table.");
        Database db = null;
        if (row.prefixes.size() == 0) {
          getLog().info("Module doesn't have dbprefix. We will not update database model.");
          moduleModels.add(new Database());
          moduleOldModels.add(new Database());
        } else {
          getLog().info("Loading submodel from database...");
          originaldb = platform.loadModelFromDatabase(row.filter, row.prefixes.get(0), true,
              row.idMod);
          originaldb.moveModifiedToTables();
          getLog().info("Submodel loaded");

          db = (Database) dbXML.clone();
          db.applyNamingConventionToUpdate(row.filter);

          platform.insertNonModuleTablesFromXML(originaldb, db);
          platform.insertNonModuleTablesFromDatabase(originaldb, dbXML, db);
          platform.insertFunctionsInBothModels(originaldb, dbXML, db);
          platform.insertViewsInBothModels(originaldb, dbXML, db);
          final Database olddb = (Database) originaldb.clone();
          moduleModels.add(db);
          moduleOldModels.add(olddb);
          log.info("Checking datatypes from the model loaded from the database");
          originaldb.checkDataTypes();
          log.info("Checking datatypes from the model loaded from XML files");
          db.checkDataTypes();
        }

        if (row.prefixes.size() > 0) {
          getLog().info("Updating database model...");
          ModelComparator comparator = new ModelComparator(platform.getPlatformInfo(),
              platform.isDelimitedIdentifierModeOn());
          List changes = comparator.compare(originaldb, db);
          for (int i = 0; i < changes.size(); i++) {
            if (changes.get(i) instanceof AddForeignKeyChange) {
              AddForeignKeyChange change = (AddForeignKeyChange) changes.get(i);
              Table table = dbAD.findTable(change.getChangedTable().getName());
              if (table != null) {
                ForeignKey fk = null;
                for (int j = 0; j < table.getForeignKeyCount() && fk == null; j++) {
                  if (table.getForeignKey(j).getName().equals(change.getNewForeignKey().getName())) {
                    fk = table.getForeignKey(j);
                  }
                }
                if (fk != null) {
                  table.removeForeignKey(fk);
                }
              }
            }
          }
          platform.alterTables(originaldb, db, !isFailonerror());
          getLog().info("Model update complete.");

          if (originaldb != null) {
            // First we fix the dbAD model, removing the foreign keys that have been removed in the
            // database by the model upgrade process
            platform.removeDeletedFKTriggers(originaldb, dbAD);
          }
        }
      }

      final DatabaseDataIO dbdio = new DatabaseDataIO();
      dbdio.setEnsureFKOrder(false);
      for (ModuleRow row : moduleRows) {
        getLog().info("Loading data from XML files");
        final Vector<File> files = new Vector<File>();
        final File fsourcedata = new File(basedir, "/" + row.dir + "/src-db/database/sourcedata/");
        final File[] datafiles = DatabaseUtils.readFileArray(fsourcedata);
        for (int i = 0; i < datafiles.length; i++)
          if (datafiles[i].exists() && datafiles[i].getName().endsWith(".xml"))
            files.add(datafiles[i]);

        final DataReader dataReader = dbdio.getConfiguredCompareDataReader(dbAD);
        final DatabaseData databaseOrgData = new DatabaseData(dbAD);
        for (int i = 0; i < files.size(); i++) {
          try {
            dataReader.getSink().start();
            final String tablename = files.get(i).getName()
                .substring(0, files.get(i).getName().length() - 4);
            final Vector<DynaBean> vectorDynaBeans = ((DataToArraySink) dataReader.getSink())
                .getVector();
            dataReader.parse(files.get(i));
            databaseOrgData.insertDynaBeansFromVector(tablename, vectorDynaBeans);
            dataReader.getSink().end();
          } catch (final Exception e) {
            e.printStackTrace();
          }
        }

        final DataComparator dataComparator = new DataComparator(platform.getSqlBuilder()
            .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
        dataComparator.compareToUpdate(dbXML, platform, databaseOrgData, ad, row.idMod);
        getLog().info("Comparing databases to find data differences");
        dataChanges.add(dataComparator.getChanges());
      }
      getLog().info("Updating database data...");

      getLog().info("Disabling foreign keys");
      final Connection connection = platform.borrowConnection();
      platform.disableAllFK(connection, dbAD, !isFailonerror());
      getLog().info("Disabling triggers");
      platform.disableAllTriggers(connection, dbAD, !isFailonerror());
      platform.disableNOTNULLColumns(dbXML, ad);

      // Executing ModuleScripts
      List<String> sortedModRows = new ArrayList<String>();
      for (ModuleRow row : moduleRows) {
        sortedModRows.add(row.dir);
      }
      Collections.sort(sortedModRows);
      for (String row : sortedModRows) {
        hd.setBasedir(new File(basedir + "/../"));
        hd.setModuleJavaPackage(row);
        hd.execute();
      }

      ArrayList<List> changes = new ArrayList<List>();
      for (int i = 0; i < dataChanges.size(); i++) {
        getLog().info("Updating database data for module " + moduleRows.get(i).name);
        platform.alterData(connection, dbAD, dataChanges.get(i));
        getLog().info("Recreating Primary Keys");
        changes.add(platform.alterTablesRecreatePKs(moduleOldModels.get(i), moduleModels.get(i),
            !isFailonerror()));
      }
      getLog().info("Removing invalid rows.");
      platform.deleteInvalidConstraintRows(completedb, null, !isFailonerror());
      for (int i = 0; i < dataChanges.size(); i++) {
        getLog().info("Executing update final script (NOT NULLs and dropping temporary tables)");
        platform.alterTablesPostScript(moduleOldModels.get(i), moduleModels.get(i),
            !isFailonerror(), changes.get(i), dbXML, ad);
      }
      platform.executeOnCreateDefaultForMandatoryColumns(dbXML, ad);
      getLog().info("Enabling Foreign Keys and Triggers");
      platform.enableNOTNULLColumns(dbXML, ad);
      platform.enableAllFK(connection, dbAD, !isFailonerror());
      platform.enableAllTriggers(connection, dbAD, !isFailonerror());

      try {
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
      } catch (Exception e) {
        log.error("Error while executing postscript: ", e);
      }
      DBSMOBUtil.getInstance().updateCRC(platform);
      DatabaseData databaseOrgData2 = new DatabaseData(dbAD);
      DBSMOBUtil.getInstance().loadDataStructures(platform, databaseOrgData2, dbAD, dbAD, basedir,
          datafilter, input);
      final DataComparator dataComparator2 = new DataComparator(platform.getSqlBuilder()
          .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
      dataComparator2.compare(dbXML, dbXML, platform, databaseOrgData2, ad, null);
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

  public String getModule() {
    return module;
  }

  public void setModule(String module) {
    this.module = module;
  }

  public boolean isCustomLogging() {
    return customLogging;
  }

  public void setCustomLogging(boolean customLogging) {
    this.customLogging = customLogging;
  }

  public String getBaseConfig() {
    return baseConfig;
  }

  public void setBaseConfig(String baseConfig) {
    this.baseConfig = baseConfig;
  }

  public boolean isForce() {
    return force;
  }

  public void setForce(boolean force) {
    this.force = force;
  }

  public boolean isStrict() {
    return strict;
  }

  public void setStrict(boolean strict) {
    this.strict = strict;
  }
}
