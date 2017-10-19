/*
 *************************************************************************
 * The contents of this file are subject to the Openbravo  Public  License
 * Version  1.1  (the  "License"),  being   the  Mozilla   Public  License
 * Version 1.1  with a permitted attribution clause; you may not  use this
 * file except in compliance with the License. You  may  obtain  a copy of
 * the License at http://www.openbravo.com/legal/license.html 
 * Software distributed under the License  is  distributed  on  an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific  language  governing  rights  and  limitations
 * under the License. 
 * The Original Code is Openbravo ERP. 
 * The Initial Developer of the Original Code is Openbravo SLU 
 * All portions are Copyright (C) 2009-2017 Openbravo SLU 
 * All Rights Reserved. 
 * Contributor(s):  ______________________________________.
 ************************************************************************
 */
package org.openbravo.ddlutils.task;

import java.io.File;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.openbravo.base.exception.OBException;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.OBDataset;
import org.openbravo.ddlutils.util.ValidateAPIData;
import org.openbravo.ddlutils.util.ValidateAPIModel;

/**
 * This class receives two xml models and performs the checks for their APIs, both in model and
 * application dictionary.
 * 
 * Change compared to the CheckAPI class is that this class does the check for a distribution of
 * modules like 3.0 so reads core + all modules instead of looking at core only.
 * 
 */
public class CheckAPIDistribution extends BaseDatabaseTask {
  File stableDBdir;
  File testDBdir;
  String modules;

  public String getModules() {
    return modules;
  }

  public void setModules(String modules) {
    this.modules = modules;
  }

  public File getStableDBdir() {
    return stableDBdir;
  }

  public void setStableDBdir(File stableDBdir) {
    this.stableDBdir = stableDBdir;
  }

  public File getTestDBdir() {
    return testDBdir;
  }

  public void setTestDBdir(File testDBdir) {
    this.testDBdir = testDBdir;
  }

  @Override
  public void doExecute() {
    BasicDataSource ds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getUser(), getPassword());
    Platform platform = PlatformFactory.createNewPlatformInstance(ds);

    getLog().info("Using database platform: " + platform.getName());

    log.info("Using stableDBdir: " + stableDBdir);
    log.info("Using testDBDir:   " + testDBdir);

    Database dbModelStable;
    DatabaseData dbDataStable;
    Database dbModelTest;
    DatabaseData dbDataTest;

    getLog().info("Reading full stable reference model (core+all modules) ...");
    dbModelStable = readModelRecursiveHelper(platform, stableDBdir, stableDBdir);
    getLog().info("Reading model to be tested (core+all modules) ...");
    dbModelTest = readModelRecursiveHelper(platform, testDBdir, testDBdir);
    if (getModules() != null && !getModules().equals("")) {
      String modulesList = getModules();
      // In case of API check for modules, we use the testDBDir for the data loading, for the
      // ERP part, when building the stable databasedata.
      //
      // In this way, no API changes in data in Core will be detected, only
      // changes in modules will be detected
      String[] modulePackages = modulesList.split(",");
      dbDataStable = readDataRecursiveHelper(platform, testDBdir, stableDBdir, dbModelStable,
          modulePackages);
      dbDataTest = readDataRecursiveHelper(platform, testDBdir, testDBdir, dbModelTest,
          modulePackages);
      dbModelStable = getDatabaseForModules(dbModelStable, dbDataStable, modulePackages);
      dbModelTest = getDatabaseForModules(dbModelTest, dbDataTest, modulePackages);
    } else {
      dbDataStable = readDataRecursiveHelper(platform, stableDBdir, stableDBdir, dbModelStable,
          null);
      dbDataTest = readDataRecursiveHelper(platform, testDBdir, testDBdir, dbModelTest, null);
    }

    getLog().info("Comparing data models");
    final DataComparator dataComparator = new DataComparator(platform.getSqlBuilder()
        .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
    dataComparator.compare(dbDataStable, dbDataTest);

    getLog().info("Validating model API");
    ValidateAPIModel validateModel = new ValidateAPIModel(platform, dbModelStable, dbModelTest,
        new OBDataset(dbDataTest, "AD"));
    validateModel.execute();

    getLog().info("Validating data API");
    ValidateAPIData validateData = new ValidateAPIData(dataComparator.getChanges());
    validateData.execute();

    validateModel.printErrors(getLog());
    validateModel.printWarnings(getLog());
    validateModel.printInfos(getLog());
    validateData.printErrors(getLog());
    validateData.printWarnings(getLog());

    if (validateModel.hasErrors() || validateData.hasErrors()) {
      throw new RuntimeException("Test did not validate API");
    }

    if (validateModel.hasWarnings() || validateData.hasWarnings()) {
      throw new RuntimeException("Test validate API with warnings");
    }
  }

  /**
   * Returns a filtered model containing only the database objects belonging to the specified
   * modules
   * */
  private Database getDatabaseForModules(Database dbModelStable, DatabaseData databaseData,
      String[] modulePackages) {
    Database db = new Database();
    for (String modulePackage : modulePackages) {
      ExcludeFilter filterForModule = getFilterForModule(databaseData, modulePackage);
      Database dbI = null;
      try {
        dbI = (Database) dbModelStable.clone();
      } catch (final Exception e) {
        throw new OBException("Error while cloning the database model", e);
      }
      dbI.applyNamingConventionFilter(filterForModule);
      db.mergeWith(dbI);
      db.moveModifiedToTables();
    }
    return db;
  }

  /**
   * Computes a valid excludeFilter for the specified module, by reading the data XML file
   * information and retrieving the needed dbprefixes from there
   */
  private ExcludeFilter getFilterForModule(DatabaseData dbDataTest, String modulePackage) {
    ExcludeFilter filterForModule = DBSMOBUtil.getInstance().getExcludeFilter(
        new File(getTestDBdir().getAbsolutePath()));

    Vector<DynaBean> moduleDbs = dbDataTest.getRowsFromTable("AD_MODULE");
    for (DynaBean module : moduleDbs) {
      if (module.get("JAVAPACKAGE").equals(modulePackage)) {
        Vector<DynaBean> modulePrefixDbs = dbDataTest.getRowsFromTable("AD_MODULE_DBPREFIX");
        for (DynaBean prefixDb : modulePrefixDbs) {
          if (prefixDb.get("AD_MODULE_ID").equals(module.get("AD_MODULE_ID"))) {
            filterForModule.addPrefix(prefixDb.get("NAME").toString());
          }
        }
      }
    }

    return filterForModule;
  }

  private Database readModelRecursiveHelper(Platform platform, File erpBaseDir, File modulesBaseDir) {
    String modelFilter = "*/src-db/database/model";
    File modelFolder = new File(erpBaseDir, "src-db/database/model");
    String basedir = modulesBaseDir + "/modules/";

    return DatabaseUtils.readDatabaseModel(platform, modelFolder, basedir, modelFilter);
  }

  private DatabaseData readDataRecursiveHelper(Platform platform, File erpBaseDir,
      File baseDirForModules, Database model, String[] modulesArray) {
    final boolean strict = false;

    DatabaseData databaseOrgData = new DatabaseData(model);
    databaseOrgData.setStrictMode(strict);

    String basedir = baseDirForModules + "/modules/";
    String dataFilter;
    if (modulesArray == null) {
      dataFilter = "*/src-db/database/sourcedata";
    } else {
      StringBuilder dataFilterBuilder = new StringBuilder("");
      for (String module : modulesArray) {
        if (!dataFilterBuilder.toString().equals("")) {
          dataFilterBuilder.append(",");
        }
        dataFilterBuilder.append(module + "/src-db/database/sourcedata");
      }
      dataFilter = dataFilterBuilder.toString();
    }
    File dataFolder = new File(erpBaseDir, "src-db/database/sourcedata");

    log.info("Reading dataset AD sourcedata"); // to find prefixes
    DBSMOBUtil.getInstance().loadDataStructures(platform, databaseOrgData, model, model, basedir,
        dataFilter, dataFolder, strict);
    DBSMOBUtil.getInstance().applyConfigScripts(platform, databaseOrgData, model, basedir, strict,
        true);

    return databaseOrgData;
  }
}
