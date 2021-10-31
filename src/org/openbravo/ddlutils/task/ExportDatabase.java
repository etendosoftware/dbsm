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
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.List;
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
import org.apache.ddlutils.model.StructureObject;
import org.apache.ddlutils.platform.ExcludeFilter;
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
 * Task in charge of exporting the database model to XML.
 * 
 * @author adrian
 */
public class ExportDatabase extends BaseDalInitializingTask {

  private static final String OB_MODEL_PATH = "/src-db/database/model/";

  private static final String SYSTEM_USER_ID = "0";

  private File model;
  private File output;
  private String datasetList = "AD,ADRD";
  private String encoding = "UTF-8";

  private File moduledir;
  private File coremoduledir;
  private String openbravoRootPath;
  private boolean force = false;
  private boolean validateModel = true;
  private boolean testAPI = false;
  private boolean checkTranslationConsistency = true;
  private boolean rd;
  private int threads = 0;

  /** Creates a new instance of ExportDatabase */
  public ExportDatabase() {
    setUserId(SYSTEM_USER_ID);
    setAdminMode(true);
  }

  /** main method invoked from export.database.structure ant task */
  public static void main(String[] args) {
    createExportDatabase(args).execute();
  }

  private static ExportDatabase createExportDatabase(String[] args) {
    ExportDatabase exportDatabase = new ExportDatabase();
    exportDatabase.setDriver(args[0]);
    exportDatabase.setUrl(args[1]);
    exportDatabase.setUser(args[2]);
    exportDatabase.setPassword(args[3]);
    exportDatabase.setModuledir(JavaTaskUtils.getFileProperty(args[4]));
    exportDatabase.setPropertiesFile(args[5]);
    exportDatabase.setForce(JavaTaskUtils.getBooleanProperty(args[6]));
    exportDatabase.setValidateModel(JavaTaskUtils.getBooleanProperty(args[7]));
    exportDatabase.setTestAPI(JavaTaskUtils.getBooleanProperty(args[8]));
    exportDatabase.setRd(JavaTaskUtils.getBooleanProperty(args[9]));
    exportDatabase.setCheckTranslationConsistency(JavaTaskUtils.getBooleanProperty(args[10]));
    exportDatabase.setThreads(JavaTaskUtils.getIntegerProperty(args[11]));
    return exportDatabase;
  }

  @Override
  public void execute() {
    ExcludeFilter excludeFilter = DBSMOBUtil.getInstance()
        .getExcludeFilter(new File(openbravoRootPath));

    initLogging();
    super.execute();
    getLog().info("Database connection: " + getUrl() + ". User: " + getUser());

    final BasicDataSource ds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getUser(),
        getPassword());

    final Platform platform = PlatformFactory.createNewPlatformInstance(ds);
    platform.setMaxThreads(threads);

    if (!DBSMOBUtil.verifyCheckSum(new File(openbravoRootPath).getAbsolutePath())) {
      if (force) {
        getLog().warn(
            "A file was modified in the database folder, but as the export.database command was forced, it will be run anyway.");
      } else {
        getLog().error(
            "A file was modified in the database folder (this can happen if you update your repository or modify the files, and don't do update.database). Eliminate the differences (by either reverting the changes in the files, or reverting to the old revision of sources), and try to export again.");
        throw new BuildException("Found modifications in files when exporting");
      }
    }
    try {
      final DBSMOBUtil util = DBSMOBUtil.getInstance();
      util.getModules(platform, excludeFilter);
      if (util.getActiveModuleCount() == 0) {
        getLog().info(
            "No active modules. For a module to be exported, it needs to be set as 'InDevelopment'");
        return;
      }
      Database dbForAD = loadDatabaseForAD(platform, excludeFilter);
      Database dbForModel = loadDatabaseForModel(dbForAD, platform, excludeFilter);
      dbForModel.checkDataTypes();
      DatabaseData databaseOrgData = new DatabaseData(dbForAD);
      // TODO: Change parameter to take into account all the module dirs
      DBSMOBUtil.getInstance()
          .loadDataStructures(databaseOrgData, dbForAD,
              new String[] {moduledir.getAbsolutePath(), coremoduledir.getAbsolutePath()}, "*/src-db/database/sourcedata", output);
      OBDataset ad = new OBDataset(databaseOrgData, "AD");
      DBSMOBUtil.getInstance()
          .removeSortedTemplates(platform, dbForAD, databaseOrgData, moduledir.getAbsolutePath());
      DBSMOBUtil.getInstance()
              .removeSortedTemplates(platform, dbForAD, databaseOrgData, coremoduledir.getAbsolutePath());
      for (int i = 0; i < util.getActiveModuleCount(); i++) {
        Database dbI = null;
        try {
          dbI = (Database) dbForModel.clone();
        } catch (final Exception e) {
          getLog().error("Error while cloning the database model" + e.getMessage());
          return;
        }
        dbI.applyNamingConventionFilter(util.getActiveModule(i).filter);
        getLog().info("Exporting model of module: " + util.getActiveModule(i).name);
        getLog().info("  " + dbI.toString());

        if (checkTranslationConsistency) {
          log.info("  Checking translation consistency");
          long t = System.currentTimeMillis();
          List<StructureObject> inconsistentObjects = platform.checkTranslationConsistency(dbI,
              dbForModel);
          if (!inconsistentObjects.isEmpty()) {
            log.warn(
                "Warning: Some of the functions and triggers which are being exported have been detected to change if they are inserted in a PostgreSQL database again. If you are working on an Oracle-only environment, you should not worry about this. If you are working with PostgreSQL, you should check that the functions and triggers are inserted in a correct way when applying the exported module. The affected objects are: ");
            for (int numObj = 0; numObj < inconsistentObjects.size(); numObj++) {
              log.warn(inconsistentObjects.get(numObj).toString());
            }
          } else {
            log.info("  Translation consistency check finished succesfully in "
                + (System.currentTimeMillis() - t) + " ms");
          }
        }
        final DatabaseIO io = new DatabaseIO();
        File path;

        if (util.getActiveModule(i).name.equalsIgnoreCase("CORE")) {
          path = model;
        } else {
          // TODO: Change this when core in JAR
          File moduleDir = new File(
                  coremoduledir + File.separator + util.getActiveModule(i).dir);
          if (!moduleDir.exists()) {
            moduleDir = new File(
                    moduledir + File.separator + util.getActiveModule(i).dir);
          }
          path = new File(moduleDir, getModelPath());
        }

        if (testAPI) {
          getLog().info("  Reading XML model for API checking" + path);
          Database dbXML = DatabaseUtils.readDatabaseWithoutConfigScript(path);
          validateAPIForModel(platform, dbI, dbXML, ad);
        }

        if (validateModel) {
          validateDatabaseForModule(util.getActiveModule(i).idMod, dbI);
        }

        getLog().debug("  Path: " + path);
        io.writeToDir(dbI, path);
      }

      if (validateModel) {
        // Close DAL connection retrieved for validating the modules
        OBDal.getInstance().commitAndClose();
      }

      final Vector<String> datasets = new Vector<>();
      String[] datasetArray = datasetList.split(",");
      for (String dataset : datasetArray) {
        datasets.add(dataset);
      }

      if (shouldExportAD()) {
        // once model is exported, reload it from files to guarantee data is exported in a fixed
        // manner, other case column position could be different
        // we just need to reload it once before exporting the source data because the model
        // information is the same for all the modules about to be exported

        // TODO: Check this when core in JAR
        Database dbXML = DatabaseUtils.readDatabaseModel(platform, model,
            moduledir.getAbsolutePath(), coremoduledir.getAbsolutePath(), "*/src-db/database/model");

        int datasetI = 0;
        for (final String dataSetCode : datasets) {
          if (dataSetCode.equalsIgnoreCase("ADRD") && !rd) {
            continue;
          }
          OBDataset dataset = new OBDataset(databaseOrgData, dataSetCode);
          final Vector<OBDatasetTable> tableList = dataset.getTableList();
          for (int i = 0; i < util.getActiveModuleCount(); i++) {
            getLog().info("Exporting AD of module: " + util.getActiveModule(i).name);

            File path;
            if (util.getActiveModule(i).name.equalsIgnoreCase("CORE")) {
              path = output;
              if (dataSetCode.equalsIgnoreCase("ADRD")) {
                path = new File(path, "referencedData");
              }
            } else {
              // TODO: Change this when core in JAR
              File moduleDir = new File(
                      coremoduledir + File.separator + util.getActiveModule(i).dir);
              if (!moduleDir.exists()) {
                moduleDir = new File(
                        moduledir + File.separator + util.getActiveModule(i).dir);
              }
              path = new File(moduleDir, "/src-db/database/sourcedata/");
            }

            if (testAPI) {
              Vector<File> dataFiles = DBSMOBUtil.loadFilesFromFolder(path.getAbsolutePath());
              getLog().info("data API testing...");

              final DatabaseDataIO dbdio = new DatabaseDataIO();
              dbdio.setEnsureFKOrder(false);

              DataReader dataReader = dbdio.getConfiguredCompareDataReader(dbForAD);

              final DatabaseData databaseXMLData = new DatabaseData(dbForModel);
              for (int j = 0; j < dataFiles.size(); j++) {
                try {
                  dataReader.getSink().start();
                  final String tablename = dataFiles.get(j)
                      .getName()
                      .substring(0, dataFiles.get(j).getName().length() - 4);
                  final Vector<DynaBean> vectorDynaBeans = ((DataToArraySink) dataReader.getSink())
                      .getVector();
                  dataReader.parse(dataFiles.get(j));
                  databaseXMLData.insertDynaBeansFromVector(tablename, vectorDynaBeans);
                  dataReader.getSink().end();
                } catch (final Exception e) {
                  getLog().error("Error reading database data for API testing: " + e.getMessage());
                }
              }

              final DataComparator dataComparator = new DataComparator(
                  platform.getSqlBuilder().getPlatformInfo(),
                  platform.isDelimitedIdentifierModeOn());
              getLog().info("  Comparing models");
              dataComparator.compare(dbForModel, dbForModel, platform, databaseXMLData, ad,
                  util.getActiveModule(i).idMod);

              validateAPIForModel(dataComparator.getChanges());

            }

            final DatabaseDataIO dbdio = new DatabaseDataIO();
            dbdio.setEnsureFKOrder(false);

            if (util.getActiveModule(i).name.equalsIgnoreCase("CORE") || dataSetCode.equals("AD")) {
              getLog().debug("  Path: " + path);
              DatabaseData dataToExport = new DatabaseData(dbForModel);
              dbdio.readRowsIntoDatabaseData(platform, dbXML, dataToExport, dataset,
                  util.getActiveModule(i).idMod);
              // TODO: Check this when core in JAR
              DBSMOBUtil.getInstance()
                  .removeSortedTemplates(platform, dataToExport, moduledir.getAbsolutePath());
              DBSMOBUtil.getInstance()
                      .removeSortedTemplates(platform, dataToExport, coremoduledir.getAbsolutePath());
              path.mkdirs();
              if (datasetI == 0) {
                final File[] filestodelete = path.listFiles();
                for (final File filedelete : filestodelete) {
                  if (!filedelete.isDirectory()) {
                    filedelete.delete();
                  }
                }
              }
              for (final OBDatasetTable table : tableList) {
                try {
                  final File tableFile = new File(path, table.getName().toUpperCase() + ".xml");
                  final OutputStream out = new FileOutputStream(tableFile);
                  int rows = dbdio.writeDataForTableToXML(platform, dbXML, dataToExport, table, out,
                      getEncoding(), util.getActiveModule(i).idMod);
                  if (rows == 0) {
                    tableFile.delete();
                  } else {
                    getLog().info("  Exported " + table.getName() + " - " + rows + " rows");
                  }
                  out.flush();
                } catch (Exception e) {
                  getLog().error("Error while exporting table" + table.getName() + " to module "
                      + util.getActiveModule(i).name);
                }
              }
            }
          }
          datasetI++;
        }
      }

      if (shouldWriteChecksumInfo()) {
        getLog().info("Writing checksum info");
        // TODO: Check path when core in JAR
        DBSMOBUtil.writeCheckSumInfo(new File(openbravoRootPath).getAbsolutePath());
        DBSMOBUtil.getInstance().updateCRC();
      }
    } catch (Exception e) {
      throw new BuildException(e);
    }
  }

  protected boolean shouldExportAD() {
    return true;
  }

  protected boolean shouldWriteChecksumInfo() {
    return true;
  }

  protected String getModelPath() {
    return OB_MODEL_PATH;
  }

  private Database loadDatabaseForAD(Platform platform, ExcludeFilter excludeFilter) {
    return platform.loadModelFromDatabase(excludeFilter);
  }

  protected Database loadDatabaseForModel(Database adDatabase, final Platform platform,
      ExcludeFilter excludeFilter) {
    return adDatabase;
  }

  private void validateDatabaseForModule(String moduleId, Database dbI) {
    getLog().info("  Validating Module...");
    final Module moduleToValidate = OBDal.getInstance().get(Module.class, moduleId);
    boolean validateAD = shouldExportAD();
    final SystemValidationResult result = SystemService.getInstance()
        .validateDatabase(moduleToValidate, dbI, validateAD);
    SystemService.getInstance().logValidationResult(log, result);
    if (result.getErrors().size() > 0) {
      throw new OBException(
          "Module validation failed, see the above list of errors for more information");
    }
  }

  private void validateAPIForModel(Platform platform, Database dbDB, Database dbXml,
      OBDataset dataset) {
    getLog().info("Validating model API");
    ValidateAPIModel validate = new ValidateAPIModel(platform, dbXml, dbDB, dataset);
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

  public File getModel() {
    return model;
  }

  public void setModel(File model) {
    this.model = model;
  }

  public File getModuledir() {
    return moduledir;
  }

  public void setModuledir(File moduledir) {
    // TODO: When the core in JAR take into account the 'root' modules dir
    this.openbravoRootPath = moduledir.getAbsolutePath() + "/../";
    this.moduledir = moduledir;
    this.coremoduledir = new File(openbravoRootPath + "/modules_core");
    model = new File(openbravoRootPath, getModelPath());
    output = new File(openbravoRootPath, "src-db/database/sourcedata");
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

  public boolean isCheckTranslationConsistency() {
    return checkTranslationConsistency;
  }

  public void setCheckTranslationConsistency(boolean checkTranslationConsistency) {
    this.checkTranslationConsistency = checkTranslationConsistency;
  }

  /** Defines how many threads can be used to execute parallelizable tasks */
  public void setThreads(int threads) {
    this.threads = threads;
  }
}
