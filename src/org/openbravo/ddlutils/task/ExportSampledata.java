/*
 ************************************************************************************
 * Copyright (C) 2013-2016 Openbravo S.L.U.
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

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
import org.apache.tools.ant.BuildException;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.ModuleRow;
import org.openbravo.ddlutils.util.OBDataset;
import org.openbravo.ddlutils.util.OBDatasetTable;

/**
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
  };

  private String basedir;

  protected final String encoding = "UTF-8";

  private ExcludeFilter excludeFilter;

  private String client;
  private String clientId;
  private String module;
  private String exportFormatParam;
  private ExportFormat exportFormat;
  private String rdbms;
  private static final String POSTGRE_RDBMS = "POSTGRE";

  private Map<String, Integer> exportedTablesCount = null;

  public ExportSampledata() {
  }

  public String getClientId() {
    return clientId;
  }

  @Override
  public void doExecute() {
    excludeFilter = DBSMOBUtil.getInstance().getExcludeFilter(new File(basedir));

    getLog().info("Database connection: " + getUrl() + ". User: " + getUser());

    final BasicDataSource ds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getUser(),
        getPassword());

    final Platform platform = PlatformFactory.createNewPlatformInstance(ds);
    // platform.setDelimitedIdentifierModeOn(true);
    try {
      final DBSMOBUtil util = DBSMOBUtil.getInstance();
      util.getModules(platform, excludeFilter);

      File moduledir = new File(basedir, "modules");

      Database db = platform.loadTablesFromDatabase(excludeFilter);
      db.checkDataTypes();
      DatabaseData databaseOrgData = new DatabaseData(db);
      DBSMOBUtil.getInstance().loadDataStructures(platform, databaseOrgData, db, db,
          moduledir.getAbsolutePath(), "*/src-db/database/sourcedata",
          new File(basedir, "src-db/database/sourcedata"));

      getLog().info("Exporting client " + client + " to module: " + module);

      final String dataSetCode = getDataSet();
      getLog().info("Exporting dataset " + dataSetCode);
      OBDataset dataset = new OBDataset(databaseOrgData, dataSetCode);
      final Vector<OBDatasetTable> tableList = dataset.getTableList();

      // find client
      Vector<DynaBean> rowsNewData = findClient(platform, db);
      if (rowsNewData.size() == 0) {
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
        if (!filedelete.isDirectory())
          filedelete.delete();
      }

      // sort list of tables by name
      Collections.sort(tableList, new Comparator<OBDatasetTable>() {
        @Override
        public int compare(OBDatasetTable arg0, OBDatasetTable arg1) {
          return arg0.getName().compareTo(arg1.getName());
        }
      });
      exportedTablesCount = new HashMap<String, Integer>();

      Map<String, Object> dsTableExporterExtraParams = new HashMap<String, Object>();
      dsTableExporterExtraParams.put("platform", platform);
      dsTableExporterExtraParams.put("xmlEncoding", encoding);

      for (final OBDatasetTable table : tableList) {
        try {
          final File tableFile = getFile(path, table);
          final OutputStream out = new FileOutputStream(tableFile);
          BufferedOutputStream bufOut = new BufferedOutputStream(out);
          // reads table data directly from db
          boolean dataExported = dsTableExporter.exportDataSet(db, table, out, moduleId,
              dsTableExporterExtraParams, orderByTableId());
          if (dataExported) {
            getLog().info("Exported table: " + table.getName());
            addTableToExportedTablesMap(table.getName());
          } else {
            tableFile.delete();
          }
          bufOut.flush();
          out.flush();
          bufOut.close();
          out.close();
        } catch (Exception e) {
          getLog().error(
              "Error while exporting table" + table.getName() + " to module " + moduleName, e);
        }
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
    File f = new File(path, table.getName().toUpperCase() + fileExtension);
    if (f.exists()) {
      Integer count = exportedTablesCount.get(table.getName());
      f = new File(path, table.getName().toUpperCase() + "_" + count + fileExtension);
    }
    return f;
  }

  public void setExportFormat(String exportFormatParam) {
    this.exportFormatParam = exportFormatParam;
  }

  public void setRdbms(String rdbms) {
    this.rdbms = rdbms;
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

}
