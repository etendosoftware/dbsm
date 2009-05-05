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
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DataToArraySink;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.openbravo.dal.service.OBDal;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.ModuleRow;

/**
 * 
 * @author adrian
 */
public class AlterDatabaseDataMod extends BaseDalInitializingTask {

  private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

  private File prescript = null;
  private File postscript = null;

  private String filter = "org.apache.ddlutils.io.NoneDatabaseFilter";

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

  private boolean customLogging = true;

  /** Creates a new instance of ReadDataXML */
  public AlterDatabaseDataMod() {
    super();
  }

  @Override
  public void doExecute() {

    getLog().info("Database connection: " + getUrl() + ". User: " + getUser());

    if (module == null || module.equals("")) {
      getLog()
          .error(
              "This task requires a module name to be passed as parameter. Example: ant update.database.mod -Dmodule=modulename");
      throw new BuildException("No module name provided.");
    }
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
    boolean hasBeenModified = DBSMOBUtil.getInstance().hasBeenModified(platform, false);
    if (hasBeenModified) {
      if (force)
        getLog()
            .info(
                "Database was modified locally, but as update.database command is forced, the database will be updated anyway.");
      else {
        getLog()
            .info(
                "Database has local changes. Update.database will not be done. If you want to force the update.database, do: ant update.database -Dforce=yes (you will lose all your changes if you do it)");
        throw new BuildException("Database has local changes. Update.database not done.");
      }
    }
    // platform.setDelimitedIdentifierModeOn(true);

    getLog().info("Creating submodel for application dictionary");
    Database dbXML = null;
    if (basedir == null) {
      getLog()
          .info("Basedir for additional files not specified. Updating database with just Core.");
      dbXML = DatabaseUtils.readDatabase(getModel());
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
      dbXML = DatabaseUtils.readDatabase(fileArray);
    }

    Database completedb = null;
    Database dbAD = null;
    try {
      completedb = (Database) dbXML.clone();
      dbAD = (Database) dbXML.clone();
      dbAD.filterByDataset("ADCS");
    } catch (final Exception e) {
      e.printStackTrace();
    }

    final Vector<Vector<Change>> dataChanges = new Vector<Vector<Change>>();
    final Vector<Database> moduleModels = new Vector<Database>();
    final Vector<Database> moduleOldModels = new Vector<Database>();
    final Vector<ModuleRow> moduleRows = new Vector<ModuleRow>();

    final DBSMOBUtil util = DBSMOBUtil.getInstance();
    DBSMOBUtil.getInstance().moveModuleDataFromInstTables(platform, completedb, module);
    util.getModules(platform, excludeobjects);
    boolean fullUpdate = false;

    if (module.toUpperCase().contains("CORE") || module.equals("%")) {
      getLog()
          .info(
              "You've either specified a list that contains Core, or module specified is %. Complete update.database will be performed.");
      fullUpdate = true;
    }
    if (util.listDependsOnTemplate(module)) {
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
      ada.setFilter(filter);
      ada.setInput(input);
      ada.setObject(object);
      ada.setFailonerror(failonerror);
      ada.setVerbosity(getVerbosity());
      ada.setBasedir(basedir);
      ada.setDirFilter(dirFilter);
      ada.setDatadir(datadir);
      ada.setDatafilter(datafilter);
      ada.setUserId(userId);
      ada.setPropertiesFile(propertiesFile);
      ada.setLog(getLog());
      ada.doExecute();
      return;
    }
    final StringTokenizer st = new StringTokenizer(module, ",");
    while (st.hasMoreElements()) {
      final String modName = st.nextToken().trim();
      getLog().info("Updating module: " + modName);
      final ModuleRow row = util.getRowFromDir(modName);
      moduleRows.add(row);
      if (row == null)
        throw new BuildException("Module " + modName + " not found in AD_MODULE table.");
      Database originaldb = null;
      Database db = null;
      try {
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
          final Database olddb = (Database) originaldb.clone();
          moduleModels.add(db);
          moduleOldModels.add(olddb);
        }
        final DatabaseDataIO dbdio = new DatabaseDataIO();
        dbdio.setEnsureFKOrder(false);
        dbdio.setDatabaseFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), dbAD));

        getLog().info("Loading data from XML files");
        final Vector<File> files = new Vector<File>();
        final File fsourcedata = new File(basedir, "/" + row.dir + "/src-db/database/sourcedata/");
        final File[] datafiles = DatabaseUtils.readFileArray(fsourcedata);
        for (int i = 0; i < datafiles.length; i++)
          if (datafiles[i].exists())
            files.add(datafiles[i]);

        final DataReader dataReader = dbdio.getConfiguredCompareDataReader(dbAD);
        final DatabaseData databaseOrgData = new DatabaseData(dbAD);
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

        final DataComparator dataComparator = new DataComparator(platform.getSqlBuilder()
            .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
        dataComparator.compareUsingDALToUpdate(dbAD, platform, databaseOrgData, "AD", row.idMod);
        getLog().info("Data changes we will perform: ");
        for (final Change change : dataComparator.getChanges())
          getLog().info(change);
        getLog().info("Comparing databases to find data differences");
        dataChanges.add(dataComparator.getChanges());
        OBDal.getInstance().commitAndClose();

        getLog().info("Updating database model...");

        if (row.prefixes.size() > 0)
          platform.alterTables(originaldb, db, !isFailonerror());
        /*
         * StringWriter sw=new StringWriter(); platform.getSqlBuilder().setWriter(sw);
         * platform.getSqlBuilder().alterDatabase(originaldb, db, null);
         * System.out.println(sw.toString());
         */
        getLog().info("Model update complete.");

      } catch (final Exception e) {
        // log(e.getLocalizedMessage());
        e.printStackTrace();
        throw new BuildException(e);
      }
    }

    getLog().info("Updating database data...");

    getLog().info("Disabling foreign keys");
    final Connection connection = platform.borrowConnection();
    platform.disableAllFK(connection, dbAD, !isFailonerror());
    getLog().info("Disabling triggers");
    platform.disableAllTriggers(connection, dbAD, !isFailonerror());
    for (int i = 0; i < dataChanges.size(); i++) {
      getLog().info("Updating database data for module " + moduleRows.get(i).name);
      platform.alterData(connection, dbAD, dataChanges.get(i));
      getLog().info("Removing invalid rows.");
      platform.deleteInvalidConstraintRows(completedb, !isFailonerror());
      getLog().info("Executing update final script (NOT NULLs and dropping temporary tables)");
      platform.alterTablesPostScript(moduleOldModels.get(i), moduleModels.get(i), !isFailonerror());
    }
    getLog().info("Enabling Foreign Keys and Triggers");
    platform.enableAllFK(connection, dbAD, !isFailonerror());
    platform.enableAllTriggers(connection, dbAD, !isFailonerror());

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
}
