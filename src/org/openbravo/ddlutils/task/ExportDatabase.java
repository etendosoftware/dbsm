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
import java.io.FileOutputStream;
import java.io.OutputStream;
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
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.tools.ant.BuildException;
import org.openbravo.base.exception.OBException;
import org.openbravo.dal.service.OBDal;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.OBDataset;
import org.openbravo.ddlutils.util.OBDatasetTable;
import org.openbravo.ddlutils.util.ValidateAPIData;
import org.openbravo.ddlutils.util.ValidateAPIModel;
import org.openbravo.model.ad.module.Module;
import org.openbravo.service.system.SystemService;
import org.openbravo.service.system.SystemValidationResult;

/**
 * 
 * @author adrian
 */
public class ExportDatabase extends BaseDalInitializingTask {

  private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";
  private String codeRevision;

  private File model;
  private File moduledir;
  private File output;
  private String encoding = "UTF-8";
  private boolean force = false;
  private boolean validateModel = true;
  private boolean testAPI = false;
  private String datasetList;

  private boolean rd;

  /** Creates a new instance of ExportDatabase */
  public ExportDatabase() {
  }

  @Override
  public void execute() {
    super.execute();
    getLog().info("Database connection: " + getUrl() + ". User: " + getUser());

    final BasicDataSource ds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getUser(),
        getPassword());

    final Platform platform = PlatformFactory.createNewPlatformInstance(ds);
    // platform.setDelimitedIdentifierModeOn(true);

    boolean hasBeenModified = DBSMOBUtil.getInstance().hasBeenModified(platform, true);
    if (!hasBeenModified && !force) {
      getLog()
          .info(
              "Database doesn't have local changes. We will not export changes. If you want to force the export, do: ant export.database -Dforce=yes");
      return;
    }

    // DBSMOBUtil.verifyRevision(platform, getCodeRevision(), getLog());
    if (!DBSMOBUtil.verifyCheckSum(new File(model.getAbsolutePath() + "/../../../")
        .getAbsolutePath())) {
      if (force) {
        getLog()
            .warn(
                "A file was modified in the database folder, but as the export.database command was forced, it will be run anyway.");
      } else {
        getLog()
            .error(
                "A file was modified in the database folder (this can happen if you update your repository or modify the files, and don't do update.database). Eliminate the differences (by either reverting the changes in the files, or reverting to the old revision of sources), and try to export again.");
        getLog().info("Mercurial revision in database: " + DBSMOBUtil.getDBRevision(platform));
        throw new BuildException("Found modifications in files when exporting");
      }
    }
    try {
      final DBSMOBUtil util = DBSMOBUtil.getInstance();
      util.getModules(platform, excludeobjects);
      if (util.getActiveModuleCount() == 0) {
        getLog()
            .info(
                "No active modules. For a module to be exported, it needs to be set as 'InDevelopment'");
        return;
      }
      Database db;
      db = platform.loadModelFromDatabase(DatabaseUtils.getExcludeFilter(excludeobjects));
      db.checkDataTypes();
      for (int i = 0; i < util.getActiveModuleCount(); i++) {
        getLog().info("Exporting module: " + util.getActiveModule(i).name);
        Database dbI = null;
        try {
          dbI = (Database) db.clone();
        } catch (final Exception e) {
          System.out.println("Error while cloning the database model" + e.getMessage());
          return;
        }
        dbI.applyNamingConventionFilter(util.getActiveModule(i).filter);
        getLog().info(db.toString());
        final DatabaseIO io = new DatabaseIO();
        String strPath;
        File path;

        if (util.getActiveModule(i).name.equalsIgnoreCase("CORE")) {
          strPath = model.getAbsolutePath();
          path = model;
        } else {
          strPath = moduledir + "/" + util.getActiveModule(i).dir + "/src-db/database/model/";
          path = new File(strPath);
        }

        if (testAPI) {
          getLog().info("Reading XML model for API checking" + path);
          Database dbXML = DatabaseUtils.readDatabase(path);
          validateAPIForModel(platform, dbI, dbXML);
        }

        if (validateModel)
          validateDatabaseForModule(util.getActiveModule(i).idMod, dbI);

        getLog().info("Path: " + path);
        io.writeToDir(dbI, path);
      }

      final Vector<String> datasets = new Vector<String>();
      String[] datasetArray = datasetList.split(",");
      for (String dataset : datasetArray)
        datasets.add(dataset);

      DatabaseData databaseOrgData = new DatabaseData(db);
      DBSMOBUtil.getInstance().loadDataStructures(platform, databaseOrgData, db, db,
          moduledir.getAbsolutePath(), "*/src-db/database/sourcedata", output);
      int datasetI = 0;
      for (final String dataSetCode : datasets) {
        if (dataSetCode.equalsIgnoreCase("ADRD") && !rd)
          continue;
        OBDataset dataset = new OBDataset(databaseOrgData, dataSetCode);
        final Vector<OBDatasetTable> tableList = dataset.getTableList();
        for (int i = 0; i < util.getActiveModuleCount(); i++) {
          getLog().info("Exporting module: " + util.getActiveModule(i).name);
          getLog().info(db.toString());

          File path;
          if (util.getActiveModule(i).name.equalsIgnoreCase("CORE")) {
            path = output;
            if (dataSetCode.equalsIgnoreCase("ADRD"))
              path = new File(path, "referencedData");
          } else {
            path = new File(moduledir, util.getActiveModule(i).dir + "/src-db/database/sourcedata/");
          }

          if (testAPI) {
            Vector<File> dataFiles = DBSMOBUtil.loadFilesFromFolder(path.getAbsolutePath());
            getLog().info("data API testing...");

            final DatabaseDataIO dbdio = new DatabaseDataIO();
            dbdio.setEnsureFKOrder(false);
            dbdio.setDatabaseFilter(DatabaseUtils.getDynamicDatabaseFilter(
                "com.openbravo.db.OpenbravoMetadataFilter", db));

            DataReader dataReader = dbdio.getConfiguredCompareDataReader(db);

            final DatabaseData databaseXMLData = new DatabaseData(db);
            for (int j = 0; j < dataFiles.size(); j++) {
              // getLog().info("Loading data for module. Path:
              // "+dataFiles.get(i).getAbsolutePath());
              try {
                dataReader.getSink().start();
                final String tablename = dataFiles.get(j).getName().substring(0,
                    dataFiles.get(j).getName().length() - 4);
                final Vector<DynaBean> vectorDynaBeans = ((DataToArraySink) dataReader.getSink())
                    .getVector();
                dataReader.parse(dataFiles.get(j));
                databaseXMLData.insertDynaBeansFromVector(tablename, vectorDynaBeans);
                dataReader.getSink().end();
              } catch (final Exception e) {
                e.printStackTrace();
              }
            }

            OBDataset ad = new OBDataset(databaseXMLData, "AD");
            final DataComparator dataComparator = new DataComparator(platform.getSqlBuilder()
                .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
            dataComparator.setFilter(DatabaseUtils.getDynamicDatabaseFilter(
                "com.openbravo.db.OpenbravoMetadataFilter", db));
            getLog().info("Comparing models");
            dataComparator.compare(db, db, platform, databaseXMLData, ad,
                util.getActiveModule(i).idMod);

            validateAPIForModel(dataComparator.getChanges());

          }

          final DatabaseDataIO dbdio = new DatabaseDataIO();
          dbdio.setEnsureFKOrder(false);

          if (util.getActiveModule(i).name.equalsIgnoreCase("CORE") || dataSetCode.equals("AD")) {
            getLog().info("Path: " + path);
            path.mkdirs();
            if (datasetI == 0) {
              final File[] filestodelete = path.listFiles();
              for (final File filedelete : filestodelete) {
                if (!filedelete.isDirectory())
                  filedelete.delete();
              }
            }
            for (final OBDatasetTable table : tableList) {
              getLog().info(
                  "Exporting table: " + table.getName() + " to module "
                      + util.getActiveModule(i).name);
              final File tableFile = new File(path, table.getName().toUpperCase() + ".xml");
              final OutputStream out = new FileOutputStream(tableFile);
              final boolean b = dbdio.writeDataForTableToXML(platform, db, table, out,
                  getEncoding(), util.getActiveModule(i).idMod);
              if (!b)
                tableFile.delete();
              out.flush();
            }
          }
        }
        datasetI++;
      }
      getLog().info("Writing checksum info");
      DBSMOBUtil.writeCheckSumInfo(new File(model.getAbsolutePath() + "/../../../")
          .getAbsolutePath());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void validateDatabaseForModule(String moduleId, Database dbI) {
    getLog().info("Validating Module...");
    final Module moduleToValidate = OBDal.getInstance().get(Module.class, moduleId);
    final SystemValidationResult result = SystemService.getInstance().validateDatabase(
        moduleToValidate, dbI);
    SystemService.getInstance().logValidationResult(log, result);
    if (result.getErrors().size() > 0) {
      throw new OBException(
          "Module validation failed, see the above list of errors for more information");
    }
  }

  private void validateAPIForModel(Platform platform, Database dbDB, Database dbXml) {
    getLog().info("Validating model API");
    ValidateAPIModel validate = new ValidateAPIModel(platform, dbXml, dbDB);
    validate.execute();
    validate.printErrors(getLog());
    validate.printWarnings(getLog());
    if (validate.hasErrors()) {
      throw new OBException("Model did not validate API");
    }
  }

  private void validateAPIForModel(Vector<Change> changes) {
    ValidateAPIData val = new ValidateAPIData(changes);
    val.execute();
    val.printErrors();
    val.printWarnings();
    if (val.hasErrors()) {
      throw new OBException("Data did not validate API");
    }

  }

  public String getExcludeobjects() {
    return excludeobjects;
  }

  public void setExcludeobjects(String excludeobjects) {
    this.excludeobjects = excludeobjects;
  }

  public File getModel() {
    return model;
  }

  public void setModel(File model) {
    this.model = model;
  }

  public String getCodeRevision() {
    return codeRevision;
  }

  public void setCodeRevision(String rev) {
    codeRevision = rev;
  }

  public File getModuledir() {
    return moduledir;
  }

  public void setModuledir(File moduledir) {
    this.moduledir = moduledir;
  }

  public File getOutput() {
    return output;
  }

  public void setOutput(File output) {
    this.output = output;
  }

  public String getEncoding() {
    return encoding;
  }

  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  public boolean isForce() {
    return force;
  }

  public void setForce(boolean force) {
    this.force = force;
  }

  public boolean isValidateModel() {
    return validateModel;
  }

  public void setValidateModel(boolean validateModel) {
    this.validateModel = validateModel;
  }

  public boolean isTestAPI() {
    return testAPI;
  }

  public void setTestAPI(boolean testAPI) {
    this.testAPI = testAPI;
  }

  public String getDatasetList() {
    return datasetList;
  }

  public void setDatasetList(String datasetList) {
    this.datasetList = datasetList;
  }

  public boolean isRd() {
    return rd;
  }

  public void setRd(boolean rd) {
    this.rd = rd;
  }
}
