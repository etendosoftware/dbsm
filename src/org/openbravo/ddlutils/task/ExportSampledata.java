/*
 ************************************************************************************
 * Copyright (C) 2013-2019 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.ddlutils.task;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.io.DataSetTableExporter;
import org.apache.ddlutils.io.DataSetTableQueryGenerator;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.apache.ddlutils.platform.postgresql.PostgreSqlDatabaseDataIO;
import org.apache.log4j.Logger;
import org.apache.tools.ant.BuildException;
import org.openbravo.ddlutils.util.*;

/**
 * Task in charge of exporting the sample data of a given client.
 *
 * @author huehner
 */
public class ExportSampledata extends BaseDatabaseTask {

  public enum ExportFormat {
    COPY("copy"), XML("xml");
    private String name;

    private ExportFormat(String name) {
      this.name = name;
    }

    public String getFileExtension() {
      return "." + name;
    }

    public static ExportFormat getExportFormatByName(String formatName, String rdbms) {
      ExportFormat exportFormat = null;
      if (formatName != null && formatName.equals(COPY.name) && POSTGRE_RDBMS.equals(rdbms)) {
        exportFormat = COPY;
      } else {
        exportFormat = XML;
      }
      return exportFormat;
    }

    /**
     * Returns the DataSetTableExporter that will be used to export the dataset tables to XML
     */
    public DataSetTableExporter getDataSetTableExporter(DataSetTableQueryGenerator queryGenerator) {
      switch (this) {
        case COPY:
          return new PostgreSqlDatabaseDataIO(queryGenerator);
        default:
          DatabaseDataIO databaseDataIO = new DatabaseDataIO(queryGenerator);
          databaseDataIO.setEnsureFKOrder(false);
          // for sampledata do not write a primary key comment onto each line to save space
          databaseDataIO.setWritePrimaryKeyComment(false);
          return databaseDataIO;
      }
    }
  }

  private String basedir;

  private String client;
  private String clientId;
  private String module;
  private String exportFormatParam;
  private ExportFormat exportFormat;
  private String rdbms;
  private static final String POSTGRE_RDBMS = "POSTGRE";
  private static final String ENCODING = "UTF-8";
  private Map<String, Integer> exportedTablesCount = new HashMap<>();

  private int nThreads = 0;

  public ExportSampledata() {
  }

  /** main method invoked from export.sample.data ant task */
  public static void main(String[] args) {
    createExportSampledata(args).execute();
  }

  private static ExportSampledata createExportSampledata(String[] args) {
    ExportSampledata exportSampledata = new ExportSampledata();
    exportSampledata.setDriver(args[0]);
    exportSampledata.setUrl(args[1]);
    exportSampledata.setUser(args[2]);
    exportSampledata.setPassword(args[3]);
    exportSampledata.setRdbms(args[4]);
    exportSampledata.setBasedir(args[5]);
    exportSampledata.setClient(args[6]);
    exportSampledata.setModule(args[7]);
    exportSampledata.setExportFormat(args[8]);
    exportSampledata.setThreads(JavaTaskUtils.getIntegerProperty(args[9]));
    return exportSampledata;
  }

  public String getClientId() {
    return clientId;
  }

  @Override
  public void doExecute() {
    ExcludeFilter excludeFilter = DBSMOBUtil.getInstance().getExcludeFilter(new File(basedir));

    getLog().info("Database connection: " + getUrl() + ". User: " + getUser());

    final BasicDataSource ds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getUser(),
        getPassword());

    final Platform platform = PlatformFactory.createNewPlatformInstance(ds);
    platform.setMaxThreads(nThreads);
    try {
      final DBSMOBUtil util = DBSMOBUtil.getInstance();
      util.getModules(platform, excludeFilter);

      File moduledir = new File(basedir, "modules");

      Database db = platform.loadTablesFromDatabase(excludeFilter);
      db.checkDataTypes();
      DatabaseData databaseOrgData = new DatabaseData(db);

      ModulesUtil.checkCoreInSources(ModulesUtil.coreInSources());
      String rootProject = ModulesUtil.getProjectRootDir();

      List<String> modulesDir = new ArrayList<>();
      for (String modDir : ModulesUtil.moduleDirs) {
        modulesDir.add(rootProject + File.separator + modDir);
      }
      getLog().info("Loading data structures from: " + Arrays.toString(modulesDir.toArray()));

      DBSMOBUtil.getInstance()
              .loadDataStructures(databaseOrgData, db, modulesDir.toArray(new String[0]), "*/src-db/database/sourcedata", new File(basedir, "src-db/database/sourcedata"));

      getLog().info("Exporting client " + client + " to module: " + module);

      final String dataSetCode = getDataSet();
      getLog().info("Exporting dataset " + dataSetCode);
      OBDataset dataset = new OBDataset(databaseOrgData, dataSetCode);
      final Vector<OBDatasetTable> tableList = dataset.getTableList();

      // find client
      Vector<DynaBean> rowsNewData = findClient(platform, db);
      if (rowsNewData.isEmpty()) {
        log.error("Specified client: " + client + " not found.");
        System.exit(1);
      }
      // unique constraint on client ensures 1 row here
      DynaBean clientToExport = rowsNewData.get(0);

      clientId = (String) clientToExport.get("AD_CLIENT_ID");
      String clientName = getExportFileName(client);

      ModuleRow moduleToExport = getModuleToExport(util);
      String moduleId = moduleToExport != null ? moduleToExport.idMod : "";
      String moduleName = moduleToExport != null ? moduleToExport.name : null;
      File basePath = getBasePath(util, moduleToExport);
      File sampledataFolder = new File(basePath, "referencedata/sampledata");

      log.info("Creating folder " + clientName + " in: " + sampledataFolder);

      if (overwriteDataSetTableWhereClauseWithClientFilter()) {
        for (OBDatasetTable dsTable : tableList) {
          dsTable.setWhereclause("ad_client_id = '" + clientId + "'");
        }
      }

      File path = new File(sampledataFolder, clientName);
      exportFormat = ExportFormat.getExportFormatByName(exportFormatParam, rdbms);
      final DataSetTableExporter dsTableExporter = exportFormat
          .getDataSetTableExporter(getQueryGenerator());

      path.mkdirs();
      final File[] filestodelete = path.listFiles();
      for (final File filedelete : filestodelete) {
        if (!filedelete.isDirectory()) {
          filedelete.delete();
        }
      }

      // sort list of tables by name
      Collections.sort(tableList, new Comparator<OBDatasetTable>() {
        @Override
        public int compare(OBDatasetTable arg0, OBDatasetTable arg1) {
          return arg0.getName().compareTo(arg1.getName());
        }
      });

      Map<String, Object> dsTableExporterExtraParams = new HashMap<String, Object>();
      dsTableExporterExtraParams.put("platform", platform);
      dsTableExporterExtraParams.put("xmlEncoding", ENCODING);

      ExecutorService es = Executors.newFixedThreadPool(platform.getMaxThreads());

      for (final OBDatasetTable table : tableList) {
        try {
          Runnable exportRunner = new ExportDataSetRunner(dsTableExporter, getFile(path, table),
              table, moduleId, db, this, dsTableExporterExtraParams, log, orderByTableId());
          addTableToExportedTablesMap(table.getName());
          es.execute(exportRunner);
        } catch (Exception e) {
          getLog().error(
              "Error while exporting table" + table.getName() + " to module " + moduleName, e);
        }
      }
      es.shutdown();
      boolean ok;
      try {
        // Wait until all the tables have been imported, or until 24 hours have passed
        ok = es.awaitTermination(24, TimeUnit.HOURS);
      } catch (InterruptedException e) {
        throw new RuntimeException("Exception while waiting for the files to be imported", e);
      }
      if (!ok) {
        throw new RuntimeException("Importing the sample data didn't finish within the timeout");
      }

    } catch (Exception e) {
      throw new BuildException(e);
    }
  }

  private void addTableToExportedTablesMap(String name) {
    Integer count = (exportedTablesCount.get(name) == null) ? 0 : exportedTablesCount.get(name);
    exportedTablesCount.put(name, count + 1);
  }

  private File getFile(File path, OBDatasetTable table) {
    String fileExtension = getFileExtension();
    String fileName = null;
    if (exportedTablesCount.containsKey(table.getName())) {
      Integer count = exportedTablesCount.get(table.getName());
      fileName = table.getName().toUpperCase() + "_" + count + fileExtension;
    } else {
      fileName = table.getName().toUpperCase() + fileExtension;
    }
    return new File(path, fileName);
  }

  public void setExportFormat(String exportFormatParam) {
    this.exportFormatParam = exportFormatParam;
  }

  public void setRdbms(String rdbms) {
    this.rdbms = rdbms;
  }

  public void setThreads(int nThreads) {
    this.nThreads = nThreads;
  }

  protected String getFileExtension() {
    return exportFormat.getFileExtension();
  }

  protected DataSetTableQueryGenerator getQueryGenerator() {
    return new DataSetTableQueryGenerator();
  }

  /**
   * @return true if the where clause of the exported dataset tables should be replaced with a
   *         simple client filter
   */
  protected boolean overwriteDataSetTableWhereClauseWithClientFilter() {
    return true;
  }

  private Vector<DynaBean> findClient(final Platform platform, Database db) {
    Connection connection = platform.borrowConnection();
    Table table = db.findTable("AD_CLIENT");
    DatabaseDataIO dbIO = new DatabaseDataIO(getQueryGenerator());
    OBDatasetTable dsTable = new OBDatasetTable();
    dsTable.setName(table.getName());
    dsTable.setWhereclause("1=1");
    dsTable.setSecondarywhereclause("name" + "=" + "'" + client + "'");
    Vector<DynaBean> rowsNewData = dbIO.readRowsFromTableList(connection, platform, db, table,
        dsTable, null);
    platform.returnConnection(connection);
    return rowsNewData;
  }

  // copy&pasted from ExportReferenceDataTask, + change to remove appending '.xml'
  // replace everything except alphabetical characters
  private static String getExportFileName(String clientName) {
    final char[] nameChars = clientName.toCharArray();
    for (int i = 0; i < nameChars.length; i++) {
      final char c = nameChars[i];
      // Only allow valid characters
      final boolean allowedChar = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
      if (!allowedChar) {
        nameChars[i] = '_';
      }
    }
    return new String(nameChars);
  }

  public String getBasedir() {
    return basedir;
  }

  public void setBasedir(String basedir) {
    this.basedir = basedir;
  }

  public String getClient() {
    return client;
  }

  public void setClient(String client) {
    this.client = client;
  }

  public String getModule() {
    return module;
  }

  public void setModule(String module) {
    this.module = module;
  }

  /**
   * Returns the name of the dataset this task will export
   */
  protected String getDataSet() {
    return "Client Definition";
  }

  /**
   * Returns the base path where the data will be exported. The data will be exported to the
   * referencedata/sampledata subfolder of the returned path.
   * 
   * @param util
   * @param moduleToExport
   * @return the base path where the data will be exported
   */
  protected File getBasePath(DBSMOBUtil util, ModuleRow moduleToExport)
      throws IllegalArgumentException {
    getLog().info("Exporting client " + client + " to module: " + module);
    File basePath;
    if (moduleToExport.name.equalsIgnoreCase("CORE")) {
      basePath = new File(basedir);
    } else {
      basePath = new File(new File(basedir, "modules"), moduleToExport.dir);

      if (!basePath.exists()) {
        // Update modules dir to scan
        ModulesUtil.checkCoreInSources(ModulesUtil.coreInSources());
        File rootDir = new File(ModulesUtil.getProjectRootDir());

        for (String modDir : ModulesUtil.moduleDirs) {
          basePath = new File(new File(rootDir, modDir), moduleToExport.dir);
          if (basePath.exists()) {
            break;
          }
        }
      }
    }
    return basePath;
  }

  /**
   * Returns a ModuleRow that represents the module being exported, or null if no particular module
   * is being exported
   * 
   * @param util
   *          DBSMOBUtil object that contains utilities to inquire about the installed modules
   */
  protected ModuleRow getModuleToExport(DBSMOBUtil util) {
    ModuleRow moduleToExport = null;
    for (int i = 0; i < util.getModuleCount(); i++) {
      ModuleRow m = util.getModule(i);
      if (m.dir.equals(module)) {
        moduleToExport = m;
        break;
      }
    }
    if (moduleToExport == null) {
      log.error("Specified module: " + module + " not found.");
      System.exit(1);
    }
    return moduleToExport;
  }

  protected boolean orderByTableId() {
    return true;
  }

  private static class ExportDataSetRunner implements Runnable {

    private DataSetTableExporter dsTableExporter;
    private File file;
    private OBDatasetTable table;
    private String moduleId;
    private Database db;
    private Map<String, Object> dsTableExporterExtraParams;
    private Logger log;
    private boolean orderByTableId;

    public ExportDataSetRunner(DataSetTableExporter dsTableExporter, File file,
        OBDatasetTable table, String moduleId, Database db, ExportSampledata sampleDataExporter,
        Map<String, Object> dsTableExporterExtraParams, Logger log, boolean orderByTableId) {
      super();
      this.dsTableExporter = dsTableExporter;
      this.file = file;
      this.table = table;
      this.moduleId = moduleId;
      this.db = db;
      this.dsTableExporterExtraParams = dsTableExporterExtraParams;
      this.log = log;
      this.orderByTableId = orderByTableId;
    }

    @Override
    public void run() {
      try (OutputStream out = new FileOutputStream(file);
          BufferedOutputStream bufOut = new BufferedOutputStream(out)) {
        // reads table data directly from db
        boolean dataExported = dsTableExporter.exportDataSet(db, table, out, moduleId,
            dsTableExporterExtraParams, orderByTableId);
        if (!dataExported) {
          file.delete();
        }
        bufOut.flush();
        out.flush();
      } catch (Exception e) {
        log.error("Error while exporting table" + table.getName() + " to module " + moduleId, e);
      }
    }
  }

}
