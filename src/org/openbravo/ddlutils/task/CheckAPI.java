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
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DataToArraySink;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.openbravo.base.exception.OBException;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.OBDataset;
import org.openbravo.ddlutils.util.ValidateAPIData;
import org.openbravo.ddlutils.util.ValidateAPIModel;

/**
 * This class receives two xml models and performs the checkings for their APIs, both in model and
 * application dictionary
 * 
 */
public class CheckAPI extends BaseDatabaseTask {
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
    BasicDataSource ds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getUser(), getPassword());
    Platform platform = PlatformFactory.createNewPlatformInstance(ds);

    getLog().info("Using database platform: " + platform.getName());

    File stableModel = new File(stableDBdir, "/model");
    File stableData = new File(stableDBdir, "/sourcedata");
    File testModel = new File(testDBdir, "/model");
    File testData = new File(testDBdir, "/sourcedata");

    getLog().info("Reading XML model for API checking " + stableModel.getAbsolutePath());
    Database dbModelStable = DatabaseUtils.readDatabaseWithoutConfigScript(stableModel);
    DatabaseData dbDataStable = readDatabaseData(dbModelStable, stableData);

    getLog().info("Reading XML model for API checking " + testModel.getAbsolutePath());
    Database dbModelTest = DatabaseUtils.readDatabaseWithoutConfigScript(testModel);
    DatabaseData dbDataTest = readDatabaseData(dbModelTest, testData);

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
      throw new OBException("Test did not validate API");
    }

    if (validateModel.hasWarnings() || validateData.hasWarnings()) {
      throw new OBException("Test validate API with warnings");
    }
  }

  private DatabaseData readDatabaseData(Database xmlModel, File dataDir) {
    getLog().info("Reading data from XML " + dataDir.getAbsolutePath());
    Vector<File> dataFiles = DBSMOBUtil.loadFilesFromFolder(dataDir.getAbsolutePath());

    final DatabaseDataIO dbdio = new DatabaseDataIO();
    dbdio.setEnsureFKOrder(false);

    DataReader dataReader = dbdio.getConfiguredCompareDataReader(xmlModel);

    DatabaseData databaseXMLData = new DatabaseData(xmlModel);
    for (int j = 0; j < dataFiles.size(); j++) {

      try {
        dataReader.getSink().start();
        final String tablename = dataFiles.get(j).getName()
            .substring(0, dataFiles.get(j).getName().length() - 4);
        final Vector<DynaBean> vectorDynaBeans = ((DataToArraySink) dataReader.getSink())
            .getVector();
        dataReader.parse(dataFiles.get(j));
        databaseXMLData.insertDynaBeansFromVector(tablename, vectorDynaBeans);
        dataReader.getSink().end();
      } catch (final Exception e) {
        getLog().error("Error reading database data", e);
      }
    }
    return databaseXMLData;
  }
}
