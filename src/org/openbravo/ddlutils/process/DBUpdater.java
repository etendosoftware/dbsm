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

package org.openbravo.ddlutils.process;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.alteration.AddRowChange;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.alteration.RemoveRowChange;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.apache.log4j.Logger;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.openbravo.ddlutils.task.DatabaseUtils;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.ModulesUtil;
import org.openbravo.ddlutils.util.OBDataset;
import org.openbravo.ddlutils.util.OBDatasetTable;
import org.openbravo.modulescript.ModuleScriptHandler;

/**
 * Compares two databases, one of them an actual DB and the other defined in XML files to obtain and
 * apply the scripts to update actual DB to the model and data defined in XML.
 */
public class DBUpdater {
  private Logger log;
  private ExcludeFilter excludeFilter;
  private Platform platform;
  private boolean updateCheckSums = true;
  private File model;
  private String basedir;
  private boolean strict;
  private boolean failonerror = false;
  private boolean force;
  private boolean executeModuleScripts;
  private String datafilter;
  private File baseSrcAD;
  private String dirFilter;
  private String datasetName;
  private boolean checkDBModified;
  private List<String> adTableNames;
  private List<String> configScripts;
  private boolean checkFormalChanges;
  private boolean updateModuleInstallTables;

  private enum ScriptType {
    DEFAULT, SYSTEM;
  }

  /** Compares and updates database according to set parameters */
  public Database update() {
    log.info("Executing full update.database");
    log.info("Max threads " + platform.getMaxThreads());

    if (updateCheckSums) {
      // TODO: Check the path when core in JAR
      DBSMOBUtil
          .writeCheckSumInfo(new File(model.getAbsolutePath() + "/../../../").getAbsolutePath());
    }

    Connection connection = null;
    try {
      Database originaldb = platform.loadModelFromDatabase(excludeFilter, true);
      Database db = readDatabaseModelWithoutConfigScript();
      DatabaseData newData = readADData(db);
      applyConfigScripts(db, newData);
      OBDataset ad = getADDataset(newData);
      checkIfDBWasModified(ad);
      executeSystemPreScript();
      executePreScript();
      platform.alterTables(originaldb, db, !failonerror);

      // Initialize the ModuleScriptHandler that we will use later to keep the current module
      // versions, prior to the update
      ModuleScriptHandler hd = new ModuleScriptHandler();
      hd.setModulesVersionMap(DBSMOBUtil.getModulesVersion(platform));

      if (updateModuleInstallTables) {
        DBSMOBUtil.getInstance().moveModuleDataFromInstTables(platform, db, null);
      }

      final DataComparator dataComparator = compareData(db, newData, ad);

      Set<String> adTablesWithRemovedOrInsertedRecords = new HashSet<>();
      Set<String> adTablesWithRemovedRecords = new HashSet<>();
      computeTablesWithRemovedOrInsertedRecords(dataComparator.getChanges(), ad,
          adTablesWithRemovedOrInsertedRecords, adTablesWithRemovedRecords);

      connection = platform.borrowConnection();

      platform.disableDatasetFK(connection, originaldb, ad, !failonerror,
          adTablesWithRemovedOrInsertedRecords);
      platform.disableAllTriggers(connection, db, !failonerror);
      platform.disableNOTNULLColumns(db, ad);
      executeModuleScripts(hd);
      platform.alterData(connection, db, dataComparator.getChanges());
      platform.deleteInvalidConstraintRows(db, ad, adTablesWithRemovedRecords, !failonerror);

      final Database oldModel = (Database) originaldb.clone();
      @SuppressWarnings("rawtypes")
      List changes = platform.alterTablesRecreatePKs(oldModel, db, !failonerror);

      platform.executeOnCreateDefaultForMandatoryColumns(db, ad);
      platform.enableNOTNULLColumns(db, ad);
      boolean postscriptCorrect = platform.alterTablesPostScript(oldModel, db, !failonerror,
          changes, null, ad);
      boolean fksEnabled = platform.enableDatasetFK(connection, originaldb, ad,
          adTablesWithRemovedOrInsertedRecords, true);
      boolean triggersEnabled = platform.enableAllTriggers(connection, db, !failonerror);

      executePostScript();

      if (updateCheckSums) {
        DBSMOBUtil.getInstance().updateCRC();
      }

      checkErrors(postscriptCorrect, fksEnabled, triggersEnabled);
      checkFormalChanges(db, newData, ad);
      return db;
    } catch (final Exception e) {
      e.printStackTrace();
      throw new BuildException(e);
    } finally {
      platform.returnConnection(connection);
    }
  }

  private void computeTablesWithRemovedOrInsertedRecords(Vector<Change> changes, OBDataset ad,
      Set<String> adTablesWithRemovedOrInsertedRecords, Set<String> adTablesWithRemovedRecords) {
    for (Change dataChange : changes) {
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
  }

  private void executeModuleScripts(ModuleScriptHandler hd) {
    if (executeModuleScripts) {
      log.info("Running modulescripts...");
      // Executing modulescripts
      hd.setBasedir(new File(basedir + "/../"));
      hd.execute();
    } else {
      log.info("Skipping modulescripts...");
    }
  }

  private OBDataset getADDataset(DatabaseData databaseOrgData) {
    OBDataset ad = new OBDataset(databaseOrgData, datasetName);

    if (adTableNames != null) {
      Database db = databaseOrgData.getDatabase();
      Vector<OBDatasetTable> adTables = new Vector<>(adTableNames.size());
      for (String tName : adTableNames) {
        OBDatasetTable t = new OBDatasetTable();
        t.setName(tName);
        Vector<String> cols = new Vector<>();
        Table table = db.findTable(tName);
        for (Column col : table.getColumns()) {
          cols.add(col.getName());
        }
        t.setIncludedColumns(cols);
        adTables.add(t);
      }
      ad.setTables(adTables);
    }
    return ad;
  }

  private Database readDatabaseModelWithoutConfigScript() throws SQLException {
    Database db = null;
    if (basedir == null) {
      log.info("Basedir for additional files not specified. Updating database with just Core.");
      db = DatabaseUtils.readDatabaseWithoutConfigScript(model);
    } else {

      // TODO: Check that the model is in correct location
      // When the core is in JAR the model should be read from root/build/etendo/src-db/database/model
      // When the core is in SOURCES the model should be read from root/src-db/database/model
      File[] fileArray = new File[] {model};

      // This updates the modules dirs to use
      ModulesUtil.checkCoreInSources(ModulesUtil.coreInSources());

      String auxBasedir = basedir + "../";

      //Core in JAR
      if (!ModulesUtil.coreInSources) {
        auxBasedir = ModulesUtil.getProjectRootDir() + "/";
      }

      log.info("Basedir 'Read Database Model Without ConfigScript':" + auxBasedir);

      for (String moduleDir : ModulesUtil.moduleDirs) {
        log.debug("Reading model files... " + moduleDir);
        String modulesBaseDir = auxBasedir + moduleDir + "/";
        fileArray = ModulesUtil.union(fileArray, readModelFiles(modulesBaseDir));
      }
      log.debug("Found " + fileArray.length + " module files ");
      db = DatabaseUtils.readDatabaseWithoutConfigScript(fileArray);
    }
    db.checkDataTypes();
    return db;
  }

  private DatabaseData readADData(Database db) {
    DatabaseData dbData = new DatabaseData(db);
    if (baseSrcAD != null) {
      // This updates the modules dirs to use
      ModulesUtil.checkCoreInSources(ModulesUtil.coreInSources());

      String auxBasedir = basedir + "../";

      //Core in JAR
      if (!ModulesUtil.coreInSources) {
        auxBasedir = ModulesUtil.getProjectRootDir() + "/";
      }

      log.info("Basedir 'Read ADData':" + auxBasedir);

      String[] modulesBaseDir = ModulesUtil.moduleDirs;
      for (int i = 0; i < modulesBaseDir.length; i++) {
        modulesBaseDir[i] = basedir == null ? null : auxBasedir + modulesBaseDir[i] + "/";
      }
      DBSMOBUtil.getInstance()
          .loadDataStructures(dbData, db, modulesBaseDir, datafilter, baseSrcAD);
    }
    dbData.setStrictMode(strict);
    return dbData;
  }

  private void applyConfigScripts(Database db, DatabaseData newData) {
    if (configScripts == null) {
      DBSMOBUtil.getInstance().applyConfigScripts(platform, newData, db, basedir, strict, true);
    } else {
      DBSMOBUtil.getInstance().applyConfigScripts(configScripts, platform, newData, db);
    }
  }

  private void checkIfDBWasModified(OBDataset ad) {
    if (!checkDBModified) {
      return;
    }
    boolean hasBeenModified = DBSMOBUtil.getInstance().hasBeenModified(ad);
    if (hasBeenModified) {
      if (force) {
        log.info(
            "Database was modified locally, but as update.database command is forced, the database will be updated anyway.");
      } else {
        log.error(
            "Database has local changes. Update.database will not be done. You should export your changed modules before doing update.database, so that your Application Dictionary changes are preserved.");
        throw new BuildException("Database has local changes. Update.database not done.");
      }
    }
  }

  private void executeSystemPreScript() throws IOException {
    File script = new File(model, "prescript-systemuser-" + platform.getName() + ".sql");
    executeScript(script, ScriptType.SYSTEM);
  }

  private void executePreScript() throws IOException {
    File script = new File(model, "prescript-" + platform.getName() + ".sql");
    executeScript(script, ScriptType.DEFAULT);
  }

  private void executePostScript() throws IOException {
    File script = new File(model, "postscript-" + platform.getName() + ".sql");
    executeScript(script, ScriptType.DEFAULT);
  }

  private void executeScript(File script, ScriptType type) throws IOException {
    if (!script.exists()) {
      return;
    }
    log.info("Executing script " + script.getName());
    String sql = new String(Files.readAllBytes(script.toPath()));
    int errors;
    if (type == ScriptType.SYSTEM) {
      errors = platform.evaluateBatchWithSystemUser(sql, true);
    } else {
      errors = platform.evaluateBatch(sql, true);
    }
    if (errors > 0) {
      log.warn("Script " + script.getName() + " executed with " + errors + " error(s)");
    }
  }

  private DataComparator compareData(Database db, DatabaseData newData, OBDataset ad)
      throws SQLException {
    log.info("Comparing databases to find data differences...");
    final DataComparator dataComparator = new DataComparator(
        platform.getSqlBuilder().getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
    dataComparator.compareToUpdate(db, platform, newData, ad, null);
    return dataComparator;
  }

  private void checkFormalChanges(Database db, DatabaseData newData, OBDataset ad)
      throws SQLException {
    if (!checkFormalChanges) {
      return;
    }
    final DataComparator dataComparator2 = new DataComparator(
        platform.getSqlBuilder().getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
    dataComparator2.compare(db, db, platform, newData, ad, null);
    Vector<Change> finalChanges = new Vector<Change>();
    Vector<Change> notExportedChanges = new Vector<Change>();
    dataComparator2.generateConfigScript(finalChanges, notExportedChanges);

    final DatabaseIO dbIO = new DatabaseIO();

    final File configFile = new File("formalChangesScript.xml");
    dbIO.write(configFile, finalChanges);
  }

  private void checkErrors(boolean postscriptCorrect, boolean fksEnabled, boolean triggersEnabled)
      throws Exception {
    if (!triggersEnabled) {
      log.error(
          "Not all the triggers were correctly activated. The most likely cause of this is that the XML file of the trigger is not correct. If that is the case, please remove/uninstall its module, or recover the sources backup and initiate the rebuild again");
    }
    if (!fksEnabled) {
      log.error(
          "Not all the foreign keys were correctly activated. Please review which ones were not, and fix the missing references, or recover the backup of your sources.");
    }
    if (!postscriptCorrect) {
      log.error(
          "Not all the commands in the final update step were executed correctly. This likely means at least one foreign key was not activated successfully. Please review which one, and fix the missing references, or recover the backup of your sources.");
    }
    if (!triggersEnabled || !fksEnabled || !postscriptCorrect) {
      throw new Exception(
          "There were serious problems while updating the database. Please review and fix them before continuing with the application rebuild");
    }
  }

  /**
   * This method reads model files using the filter, obtaining a file array.The models will be
   * merged to create a final target model.
   */
  public File[] readModelFiles(String modulesBaseDir) throws IllegalStateException {
    final Vector<File> dirs = new Vector<File>();
    final DirectoryScanner dirScanner = new DirectoryScanner();
    dirScanner.setBasedir(new File(modulesBaseDir));
    final String[] dirFilterA = { dirFilter };
    dirScanner.setIncludes(dirFilterA);
    dirScanner.scan();
    final String[] incDirs = dirScanner.getIncludedDirectories();
    for (int j = 0; j < incDirs.length; j++) {
      final File dirF = new File(modulesBaseDir, incDirs[j]);
      dirs.add(dirF);
    }
    final File[] fileArray = new File[dirs.size()];
    log.debug("readModelFiles");
    for (int i = 0; i < dirs.size(); i++) {
      log.debug(dirs.get(i));
      fileArray[i] = dirs.get(i);
    }
    return fileArray;
  }

  public void setExcludeFilter(ExcludeFilter excludeFilter) {
    this.excludeFilter = excludeFilter;
  }

  public void setLog(Logger log) {
    this.log = log;
  }

  public void setPlatform(Platform platform) {
    this.platform = platform;
  }

  public Platform getPlatform() {
    return platform;
  }

  public void setUpdateCheckSums(boolean updateCheckSums) {
    this.updateCheckSums = updateCheckSums;
  }

  public void setModel(File model) {
    this.model = model;
  }

  public void setBasedir(String basedir) {
    this.basedir = basedir;
  }

  public void setStrict(boolean strict) {
    this.strict = strict;
  }

  public void setFailonerror(boolean failonerror) {
    this.failonerror = failonerror;
  }

  public void setForce(boolean force) {
    this.force = force;
  }

  public void setDatafilter(String datafilter) {
    this.datafilter = datafilter;
  }

  public void setBaseSrcAD(File baseSrcAD) {
    this.baseSrcAD = baseSrcAD;
  }

  public void setExecuteModuleScripts(boolean executeModuleScripts) {
    this.executeModuleScripts = executeModuleScripts;
  }

  public void setDirFilter(String dirFilter) {
    this.dirFilter = dirFilter;
  }

  public void setDatasetName(String datasetName) {
    this.datasetName = datasetName;
  }

  public void setCheckDBModified(boolean checkDBModified) {
    this.checkDBModified = checkDBModified;
  }

  public void setAdTableNames(List<String> adTableNames) {
    this.adTableNames = adTableNames;
  }

  public void setConfigScripts(List<String> configScripts) {
    this.configScripts = configScripts;
  }

  public void setCheckFormalChanges(boolean checkFormalChanges) {
    this.checkFormalChanges = checkFormalChanges;
  }

  public void setUpdateModuleInstallTables(boolean updateModuleInstallTables) {
    this.updateModuleInstallTables = updateModuleInstallTables;
  }
}
