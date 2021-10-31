/*
 ************************************************************************************
 * Copyright (C) 2001-2019 Openbravo S.L.U.
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
import java.io.IOException;
import java.sql.Connection;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.ModulesUtil;

/**
 *
 * @author Adrian
 */
public class CreateDatabase extends BaseDatabaseTask {

  private File prescript = null;
  private File postscript = null;

  private File model;
  private boolean dropfirst = false;
  private boolean failonerror = false;

  private String object = null;

  private String basedir;
  private String modulesDir;

  private String dirFilter;
  private String input;

  private Boolean isCoreInSources = true;

  private static final String MSG_ERROR = "There were serious problems while creating the database. Please review and fix them before continuing with the creation of the database.";

  public CreateDatabase() {
    doOBRebuildAppender = false;
  }

  @Override
  public void doExecute() {
    getLog().info("Database connection: " + getUrl() + ". User: " + getUser() + ". System User: "
            + getSystemUser());
    final Platform platform = getPlatformInstance();
    try {

      preCreateModel(platform);

      ModulesUtil.checkCoreInSources(getCoreInSources());

      Database db = createModel(platform);

      postCreateModel(platform, db);

    } catch (final Exception e) {
      getLog().error("Error creating database", e);
      throw new BuildException(e);
    }
  }

  protected void preCreateModel(final Platform platform) throws IOException {
    executePrescripts(platform);
  }

  protected void postCreateModel(final Platform platform, Database db) throws Exception {
    executePostscript(platform);
    writeChecksumInfo();
    insertSourceData(platform, db);
  }

  private void writeChecksumInfo() {
    getLog().info("Writing checksum info");
    // TODO: Check path when core in JAR
    DBSMOBUtil
            .writeCheckSumInfo(new File(model.getAbsolutePath() + "/../../../").getAbsolutePath());
  }

  private Database createModel(final Platform platform) throws Exception {
    Database db = null;
    if (modulesDir == null) {
      getLog()
              .info("modulesDir for additional files not specified. Creating database with just Core.");
      db = DatabaseUtils.readDatabaseWithoutConfigScript(getModel());
    } else {
      // We read model files using the filter, obtaining a file array. The models will be merged
      // to create a final target model.
      File[] fileArray = new File[] {};
      if (model.exists()) {
        fileArray = new File[] {model};
        log.info("Model = " + model.getAbsolutePath());
      }
      String workDir = ModulesUtil.getProjectRootDir();

      log.info("Working Directory = " + workDir);
      for (String modDir : ModulesUtil.moduleDirs) {
        modDir = workDir + "/" + modDir;
        log.info("Scanning " + modDir);
        final Vector<File> dirs = new Vector<>();
        final DirectoryScanner dirScanner = new DirectoryScanner();
        // dirs like modules_core may not exists when core is a (class) jar dependency
        dirScanner.setErrorOnMissingDir(false);
        dirScanner.setBasedir(new File(modDir));
        final String[] dirFilterA = { dirFilter };
        dirScanner.setIncludes(dirFilterA);
        dirScanner.scan();
        final String[] incDirs = dirScanner.getIncludedDirectories();
        for (int j = 0; j < incDirs.length; j++) {
          final File dirF = new File(modDir, incDirs[j]);
          dirs.add(dirF);
        }
        final File[] dirArray = new File[dirs.size()];
        for (int i = 0; i < dirs.size(); i++) {
          dirArray[i] = dirs.get(i);
        }
        fileArray = ModulesUtil.union(fileArray, dirArray);
      }
      db = DatabaseUtils.readDatabaseWithoutConfigScript(fileArray);
    }

    // Create database
    getLog().info("Executing creation script");
    // crop database if needed
    if (object != null) {
      final Database empty = new Database();
      empty.setName("empty");
      db = DatabaseUtils.cropDatabase(empty, db, object);
      getLog().info("for database object " + object);
    } else {
      getLog().info("for the complete database");
    }

    if (!platform.createTables(db, isDropfirst(), !isFailonerror())) {
      throw new Exception(MSG_ERROR);
    }
    return db;
  }

  private void executePrescripts(final Platform platform) throws IOException {
    executeSystemPreScript(platform);
    executePreScript(platform);
  }

  private void executePostscript(final Platform platform) throws IOException {
    // execute the post-script
    if (getPostscript() == null) {
      // try to execute the default prescript
      final File fpost = new File(getModel(), "postscript-" + platform.getName() + ".sql");
      if (fpost.exists()) {
        getLog().info("Executing default postscript");
        platform.evaluateBatch(DatabaseUtils.readFile(fpost), !isFailonerror());
      }
    } else {
      platform.evaluateBatch(DatabaseUtils.readFile(getPostscript()), !isFailonerror());
    }
  }

  private Platform getPlatformInstance() {
    BasicDataSource ds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getUser(), getPassword());
    Platform platform = PlatformFactory.createNewPlatformInstance(ds);
    if (getSystemUser() != null && getSystemPassword() != null) {
      // Create the data source used to execute statements with the system user
      BasicDataSource systemds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getSystemUser(),
              getSystemPassword());
      platform.setSystemDataSource(systemds);
    }
    return platform;
  }

  private void executeSystemPreScript(Platform platform) throws IOException {
    File script = new File(getModel(), "prescript-systemuser-" + platform.getName() + ".sql");
    if (script.exists()) {
      getLog().info("Executing script " + script.getName());
      platform.evaluateBatchWithSystemUser(DatabaseUtils.readFile(script), !isFailonerror());
    }
  }

  private void executePreScript(Platform platform) throws IOException {
    File preScript = getPrescript();
    if (preScript == null) {
      // try to execute the default prescript
      preScript = new File(getModel(), "prescript-" + platform.getName() + ".sql");
    }
    if (preScript.exists()) {
      getLog().info("Executing script " + preScript.getName());
      platform.evaluateBatch(DatabaseUtils.readFile(preScript), !isFailonerror());
    }
  }

  private void insertSourceData(final Platform platform, Database db) throws Exception {
    // Now we insert sourcedata into the database first we load the data files
    final String folders = getInput();
    final StringTokenizer strTokFol = new StringTokenizer(folders, ",");
    final Vector<File> files = new Vector<>();

    String workDir = ModulesUtil.getProjectRootDir();

    while (strTokFol.hasMoreElements()) {
      if (basedir == null) {
        getLog().info("Basedir not specified, will insert just Core data files.");
        final String folder = strTokFol.nextToken();
        final File[] fileArray = DatabaseUtils.readFileArray(new File(folder));
        for (int i = 0; i < fileArray.length; i++) {
          files.add(fileArray[i]);
        }
      } else {

        String auxBaseDir = basedir;

        /**
         * When the core is in JAR the basedir will be in 'build/etendo'
         * and the scanner will not be able to get the 'modules' folder in the root project.
         *
         * Setting temporary the basedir to the root projects allows to filter the necessary files.
         *
         * The scanner will use the following filters passed by parameters:
         * root/modules/_/src-db/database/sourcedata
         * root/modules_core/_/src-db/database/sourcedata
         * root/build/etendo/modules/_/src-db/database/sourcedata
         *
         */
        if (!getCoreInSources()) {
          auxBaseDir = workDir;
        }

        final String token = strTokFol.nextToken();
        final DirectoryScanner dirScanner = new DirectoryScanner();
        // dirs like modules_core may not exists when core is a (class) jar dependency
        dirScanner.setErrorOnMissingDir(false);
        dirScanner.setBasedir(new File(auxBaseDir));
        final String[] dirFilterA = { token };
        dirScanner.setIncludes(dirFilterA);
        dirScanner.scan();
        final String[] incDirs = dirScanner.getIncludedDirectories();
        for (int j = 0; j < incDirs.length; j++) {
          final File dirFolder = new File(auxBaseDir, incDirs[j] + "/");
          final File[] fileArray = DatabaseUtils.readFileArray(dirFolder);
          for (int i = 0; i < fileArray.length; i++) {
            files.add(fileArray[i]);
          }
        }
      }
    }

    getLog().info("Disabling triggers");
    Connection connection = platform.borrowConnection();
    platform.disableAllTriggers(connection, db, false);
    platform.returnConnection(connection);

    getLog().info("Inserting data into the database.");
    // Now we insert the data into the database
    final DatabaseDataIO dbdio = new DatabaseDataIO();
    dbdio.setEnsureFKOrder(false);
    dbdio.setUseBatchMode(true);
    DataReader dataReader = dbdio.getConfiguredDataReader(platform, db);
    dataReader.getSink().start();

    for (int i = 0; i < files.size(); i++) {
      getLog().debug("Importing data from file: " + files.get(i).getName());
      dbdio.writeDataToDatabase(dataReader, files.get(i));
    }

    /*
     * end method does final inserts when using batching, so call it after importing data files but
     * before doing anything using those data (template, fk's, not null, ... )
     */
    dataReader.getSink().end();

    DatabaseData dbData = new DatabaseData(db);
    DatabaseUtils.readDataModuleInfo(db, dbData, basedir);
    for (String template : DBSMOBUtil.getInstance().getSortedTemplates(dbData)) {
      File configScript = null;
      getLog().info("Checking template: " + template);
      for (String moduleDir : ModulesUtil.moduleDirs) {
        configScript = new File(new File(workDir + "/" + moduleDir),
                template + "/src-db/database/configScript.xml");
        if(configScript.exists()) {
          break;
        }
      }
      if(configScript != null) {
        getLog().info("Loading config script for module from path " + configScript.getAbsolutePath());
        if (configScript.exists()) {
          final DatabaseIO dbIO = new DatabaseIO();
          final Vector<Change> changesConfigScript = dbIO.readChanges(configScript);
          platform.applyConfigScript(db, changesConfigScript);
        } else {
          getLog().error("Error. We couldn't find configuration script for template "
                  + configScript.getName() + ". Path: " + configScript.getAbsolutePath());
        }
      }
    }

    getLog().info("Executing onCreateDefault statements");
    platform.executeOnCreateDefaultForMandatoryColumns(db, null);
    getLog().info("Enabling notnull constraints");
    platform.enableNOTNULLColumns(db);

    boolean continueOnError = false;
    getLog().info("Creating foreign keys");
    boolean fksEnabled = platform.createAllFKs(db, continueOnError);

    connection = platform.borrowConnection();
    getLog().info("Enabling triggers");
    boolean triggersEnabled = platform.enableAllTriggers(connection, db, false);
    platform.returnConnection(connection);

    executePostscript(platform);

    if (!triggersEnabled) {
      getLog().error(
              "Not all the triggers were correctly activated. The most likely cause of this is that the XML file of the trigger is not correct.");
    }
    if (!fksEnabled) {
      getLog().error(
              "Not all the foreign keys were correctly activated. Please review which ones were not, and fix the missing references.");
    }
    if (!triggersEnabled || !fksEnabled) {
      throw new Exception(MSG_ERROR);
    }
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

  public boolean isDropfirst() {
    return dropfirst;
  }

  public void setDropfirst(boolean dropfirst) {
    this.dropfirst = dropfirst;
  }

  public boolean isFailonerror() {
    return failonerror;
  }

  public void setFailonerror(boolean failonerror) {
    this.failonerror = failonerror;
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

  /**
   * Functionality for deleting data during create.database was removed. Function is kept to not
   * require lock-step update of dbsm.jar & build-create.xml
   *
   * @deprecated
   */
  @Deprecated
  public void setFilter(String filter) {
  }

  public String getInput() {
    return input;
  }

  public void setInput(String input) {
    this.input = input;
  }

  public String getModulesDir() {
    return modulesDir;
  }

  public void setModulesDir(String modulesDir) {
    this.modulesDir = modulesDir;
  }

  public Boolean getCoreInSources() {
    return isCoreInSources;
  }

  public void setCoreInSources(Boolean coreInSources) {
    isCoreInSources = coreInSources;
  }

}
