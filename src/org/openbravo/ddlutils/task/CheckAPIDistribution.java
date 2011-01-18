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
 * All portions are Copyright (C) 2009-2010 Openbravo SLU 
 * All Rights Reserved. 
 * Contributor(s):  ______________________________________.
 ************************************************************************
 */
package org.openbravo.ddlutils.task;

import java.io.File;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.PlatformUtils;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.openbravo.base.exception.OBException;
import org.openbravo.ddlutils.util.DBSMOBUtil;
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
    // create platform object without access to database (as not needed here)
    final String databaseName = new PlatformUtils().determineDatabaseType(getDriver(), getUrl());
    final Platform platform = PlatformFactory.createNewPlatformInstance(databaseName);

    getLog().info("Using database platform: " + databaseName);

    log.info("Using stableDBdir: " + stableDBdir);
    log.info("Using testDBDir:   " + testDBdir);

    getLog().info("Reading full stable reference model (core+all modules) ...");
    Database dbModelStable = readModelRecursiveHelper(stableDBdir);
    DatabaseData dbDataStable = readDataRecursiveHelper(platform, stableDBdir, dbModelStable);

    getLog().info("Reading model to be tested (core+all modules) ...");
    Database dbModelTest = readModelRecursiveHelper(testDBdir);
    DatabaseData dbDataTest = readDataRecursiveHelper(platform, testDBdir, dbModelTest);

    getLog().info("Comparing data models");
    final DataComparator dataComparator = new DataComparator(platform.getSqlBuilder()
        .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
    dataComparator.compare(dbDataStable, dbDataTest);

    getLog().info("Validating model API");
    ValidateAPIModel validateModel = new ValidateAPIModel(platform, dbModelStable, dbModelTest);
    validateModel.execute();

    getLog().info("Validating data API");
    ValidateAPIData validateData = new ValidateAPIData(dataComparator.getChanges());
    validateData.execute();

    validateModel.printErrors(getLog());
    validateModel.printWarnings(getLog());
    validateData.printErrors(getLog());
    validateData.printWarnings(getLog());

    if (validateModel.hasErrors() || validateData.hasErrors()) {
      throw new OBException("Test did not validate API");
    }

    if (validateModel.hasWarnings() || validateData.hasWarnings()) {
      throw new OBException("Test validate API with warnings");
    }
  }

  private Database readModelRecursiveHelper(File erpBaseDir) {
    String modelFilter = "*/src-db/database/model";
    File modelFolder = new File(erpBaseDir, "src-db/database/model");
    String basedir = erpBaseDir + "/modules/";

    Database fullModelToBeTested = DatabaseUtils.readDatabaseModel(modelFolder, basedir,
        modelFilter);
    return fullModelToBeTested;
  }

  private DatabaseData readDataRecursiveHelper(Platform platform, File erpBaseDir, Database model) {
    final boolean strict = false;

    DatabaseData databaseOrgData = new DatabaseData(model);
    databaseOrgData.setStrictMode(strict);

    String basedir = erpBaseDir + "/modules/";
    String dataFilter = "*/src-db/database/sourcedata";
    File dataFolder = new File(erpBaseDir, "src-db/database/sourcedata");

    log.info("Reading dataset AD sourcedata"); // to find prefixes
    DBSMOBUtil.getInstance().loadDataStructures(platform, databaseOrgData, model, model, basedir,
        dataFilter, dataFolder, strict);

    return databaseOrgData;
  }

}