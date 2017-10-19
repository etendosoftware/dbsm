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
import org.apache.ddlutils.platform.ExcludeFilter;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.openbravo.ddlutils.util.DBSMOBUtil;

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
  private ExcludeFilter excludeFilter;

  private static final String MSG_ERROR = "There were serious problems while creating the database. Please review and fix them before continuing with the creation of the database.";

  /** Creates a new instance of CreateDatabase */
  public CreateDatabase() {
    doOBRebuildAppender = false;
  }

  @Override
  public void doExecute() {
    excludeFilter = DBSMOBUtil.getInstance().getExcludeFilter(
        new File(model.getAbsolutePath() + "/../../../"));
    getLog().info("Database connection: " + getUrl() + ". User: " + getUser());
    final BasicDataSource ds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getUser(),
        getPassword());

    final Platform platform = PlatformFactory.createNewPlatformInstance(ds);
    try {

      // execute the pre-script
      if (getPrescript() == null) {
        // try to execute the default prescript
        final File fpre = new File(getModel(), "prescript-" + platform.getName() + ".sql");
        if (fpre.exists()) {
          getLog().info("Executing default prescript");
          platform.evaluateBatch(DatabaseUtils.readFile(fpre), !isFailonerror());
        }
      } else {
        platform.evaluateBatch(DatabaseUtils.readFile(getPrescript()), !isFailonerror());
      }

      Database db = null;
      if (modulesDir == null) {
        getLog().info(
            "modulesDir for additional files not specified. Creating database with just Core.");
        db = DatabaseUtils.readDatabaseWithoutConfigScript(getModel());
      } else {
        // We read model files using the filter, obtaining a file array. The models will be merged
        // to create a final target model.
        final Vector<File> dirs = new Vector<File>();
        dirs.add(model);
        final DirectoryScanner dirScanner = new DirectoryScanner();
        dirScanner.setBasedir(new File(modulesDir));
        final String[] dirFilterA = { dirFilter };
        dirScanner.setIncludes(dirFilterA);
        dirScanner.scan();
        final String[] incDirs = dirScanner.getIncludedDirectories();
        for (int j = 0; j < incDirs.length; j++) {
          final File dirF = new File(modulesDir, incDirs[j]);
          dirs.add(dirF);
        }
        final File[] fileArray = new File[dirs.size()];
        for (int i = 0; i < dirs.size(); i++) {
          fileArray[i] = dirs.get(i);
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

      getLog().info("Writing checksum info");
      DBSMOBUtil.writeCheckSumInfo(new File(model.getAbsolutePath() + "/../../../")
          .getAbsolutePath());

      // Now we insert sourcedata into the database first we load the data files
      final String folders = getInput();
      final StringTokenizer strTokFol = new StringTokenizer(folders, ",");
      final Vector<File> files = new Vector<File>();
      while (strTokFol.hasMoreElements()) {
        if (basedir == null) {
          getLog().info("Basedir not specified, will insert just Core data files.");
          final String folder = strTokFol.nextToken();
          final File[] fileArray = DatabaseUtils.readFileArray(new File(folder));
          for (int i = 0; i < fileArray.length; i++)
            files.add(fileArray[i]);
        } else {
          final String token = strTokFol.nextToken();
          final DirectoryScanner dirScanner = new DirectoryScanner();
          dirScanner.setBasedir(new File(basedir));
          final String[] dirFilterA = { token };
          dirScanner.setIncludes(dirFilterA);
          dirScanner.scan();
          final String[] incDirs = dirScanner.getIncludedDirectories();
          for (int j = 0; j < incDirs.length; j++) {
            final File dirFolder = new File(basedir, incDirs[j] + "/");
            final File[] fileArray = DatabaseUtils.readFileArray(dirFolder);
            for (int i = 0; i < fileArray.length; i++) {
              files.add(fileArray[i]);
            }
          }
        }
      }

      getLog().info("Disabling triggers");
      Connection _connection = platform.borrowConnection();
      platform.disableAllTriggers(_connection, db, false);
      platform.returnConnection(_connection);

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
       * end method does final inserts when using batching, so call it after importing data files
       * but before doing anything using those data (template, fk's, not null, ... )
       */
      dataReader.getSink().end();

      DatabaseData dbData = new DatabaseData(db);
      DatabaseUtils.readDataModuleInfo(db, dbData, basedir);
      for (String template : DBSMOBUtil.getInstance().getSortedTemplates(dbData)) {
        File configScript = new File(new File(modulesDir), template
            + "/src-db/database/configScript.xml");
        getLog().info(
            "Loading config script for module from path " + configScript.getAbsolutePath());
        if (configScript.exists()) {
          final DatabaseIO dbIO = new DatabaseIO();
          final Vector<Change> changesConfigScript = dbIO.readChanges(configScript);
          platform.applyConfigScript(db, changesConfigScript);
        } else {
          getLog().error(
              "Error. We couldn't find configuration script for template " + configScript.getName()
                  + ". Path: " + configScript.getAbsolutePath());
        }
      }

      getLog().info("Executing onCreateDefault statements");
      platform.executeOnCreateDefaultForMandatoryColumns(db, null);
      getLog().info("Enabling notnull constraints");
      platform.enableNOTNULLColumns(db);

      boolean continueOnError = false;
      getLog().info("Creating foreign keys");
      boolean fksEnabled = platform.createAllFKs(db, continueOnError);

      _connection = platform.borrowConnection();
      getLog().info("Enabling triggers");
      boolean triggersEnabled = platform.enableAllTriggers(_connection, db, false);
      platform.returnConnection(_connection);

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

      if (!triggersEnabled) {
        getLog()
            .error(
                "Not all the triggers were correctly activated. The most likely cause of this is that the XML file of the trigger is not correct.");
      }
      if (!fksEnabled) {
        getLog()
            .error(
                "Not all the foreign keys were correctly activated. Please review which ones were not, and fix the missing references.");
      }
      if (!triggersEnabled || !fksEnabled) {
        throw new Exception(MSG_ERROR);
      }

    } catch (final Exception e) {
      e.printStackTrace();
      throw new BuildException(e);
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
}
