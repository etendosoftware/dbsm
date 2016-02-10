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
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.ExcludeFilter;
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

  private String basedir;

  protected final String encoding = "UTF-8";

  private ExcludeFilter excludeFilter;

  private String client;
  private String module;

  private Map<String, Integer> exportedTablesCount = null;

  public ExportSampledata() {
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

      String clientid = (String) clientToExport.get("AD_CLIENT_ID");
      String clientName = getExportFileName(client);

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

      File basePath;
      if (moduleToExport.name.equalsIgnoreCase("CORE")) {
        basePath = new File(basedir);
      } else {
        basePath = new File(new File(basedir, "modules"), moduleToExport.dir);
      }
      File sampledataFolder = new File(basePath, "referencedata/sampledata");

      log.info("Creating folder " + clientName + " in: " + sampledataFolder);

      for (OBDatasetTable dsTable : tableList) {
        setDataSetWhereClause(dsTable, clientid);
      }

      File path = new File(sampledataFolder, clientName);

      final DatabaseDataIO dbdio = getDatabaseDataIO();
      dbdio.setEnsureFKOrder(false);
      // for sampledata do not write a primary key comment onto each line to save space
      dbdio.setWritePrimaryKeyComment(false);

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
      for (final OBDatasetTable table : tableList) {
        try {
          final File tableFile = getFile(path, table);
          final OutputStream out = new FileOutputStream(tableFile);
          BufferedOutputStream bufOut = new BufferedOutputStream(out);
          // reads table data directly from db
          boolean dataExported = dbdio.writeDataForTableToXML(platform, db, table, bufOut,
              encoding, moduleToExport.idMod);
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
          getLog()
              .error(
                  "Error while exporting table" + table.getName() + " to module "
                      + moduleToExport.name, e);
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
    File f = new File(path, table.getName().toUpperCase() + ".xml");
    if (f.exists()) {
      Integer count = exportedTablesCount.get(table.getName());
      f = new File(path, table.getName().toUpperCase() + "_" + count + ".xml");
    }
    return f;
  }

  /**
   * Returns the instance of DatabaseDataIO that will be used to export the dataset tables to XML
   */
  protected DatabaseDataIO getDatabaseDataIO() {
    return new DatabaseDataIO();
  }

  /**
   * Manages the dataset where clause
   * 
   * @param dsTable
   *          the dataset table
   * @param clientid
   *          the id of the client being exported
   */
  protected void setDataSetWhereClause(OBDatasetTable dsTable, String clientid) {
    // To export the sample data, the where clause defined in the dataset is overwritten with a
    // client filter
    String whereClause = "ad_client_id = '" + clientid + "'";
    dsTable.setWhereclause(whereClause);
  }

  private Vector<DynaBean> findClient(final Platform platform, Database db) {
    Connection connection = platform.borrowConnection();
    Table table = db.findTable("AD_CLIENT");
    DatabaseDataIO dbIO = new DatabaseDataIO();
    OBDatasetTable dsTable = new OBDatasetTable();
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

}
