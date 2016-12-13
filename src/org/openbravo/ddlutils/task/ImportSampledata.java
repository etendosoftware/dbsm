/*
 ************************************************************************************
 * Copyright (C) 2013-2015 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.ddlutils.task;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DataToArraySink;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.postgresql.PostgreSqlDatabaseDataIO;
import org.apache.tools.ant.BuildException;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.OBDatasetTable;
import org.openbravo.modulescript.ModuleScriptHandler;
import org.xml.sax.SAXException;

/**
 * 
 * @author huehner
 */
public class ImportSampledata extends BaseDatabaseTask {

  private String basedir;

  private static final String POSTGRE_RDBMS = "POSTGRE";

  private boolean executeModuleScripts = true;
  private String rdbms;

  public ImportSampledata() {
    doOBRebuildAppender = false;
  }

  @Override
  public void doExecute() {
    getLog().info("Database connection: " + getUrl() + ". User: " + getUser());
    final BasicDataSource ds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getUser(),
        getPassword());

    final Platform platform = PlatformFactory.createNewPlatformInstance(ds);
    // default value defined for a column should be used on missing data
    platform.setOverrideDefaultValueOnMissingData(false);

    try {

      Vector<File> dirs = new Vector<File>();
      dirs.add(new File(basedir, "/src-db/database/model/"));
      File modules = new File(basedir, "/modules");

      for (int j = 0; j < modules.listFiles().length; j++) {
        final File dirF = new File(modules.listFiles()[j], "/src-db/database/model/");
        if (dirF.exists()) {
          dirs.add(dirF);
        }
      }
      File[] fileArray2 = new File[dirs.size()];
      for (int i = 0; i < dirs.size(); i++) {
        fileArray2[i] = dirs.get(i);
      }
      Database db = DatabaseUtils.readDatabase(fileArray2);

      log.info("Disabling constraints...");
      Connection con = null;
      try {
        con = platform.borrowConnection();
        log.info("   Disabling foreign keys");
        platform.disableAllFK(con, db, false);
        log.info("   Disabling triggers");
        platform.disableAllTriggers(con, db, false);
        log.info("   Disabling check constraints");
        platform.disableCheckConstraints(db);
      } finally {
        if (con != null) {
          platform.returnConnection(con);
        }
      }

      Vector<File> refdirs = new Vector<File>();
      refdirs.add(new File(basedir, "referencedata/sampledata"));

      for (int j = 0; j < modules.listFiles().length; j++) {
        final File dirF = new File(modules.listFiles()[j], "referencedata/sampledata");
        if (dirF.exists()) {
          refdirs.add(dirF);
        }
      }
      // for each folder having sampledata
      for (File baseFolder : refdirs) {
        String[] folders = baseFolder.list();
        Arrays.sort(folders);
        // for each subfolder having data for one client to be imported
        for (String file : folders) {
          File entry = new File(baseFolder, file);
          if (!entry.isDirectory()) {
            log.warn("Skipping entry in sampledata folder: " + entry.getName());
            continue;
          }
          log.info("Importing sampledata from folder: " + entry.getPath());

          /**
           * If there is an AD_CLIENT.xml in the folder to import, check if that client is not
           * already in the db. If it is -> skip it. If no AD_CLIENT.xml is present in the folder,
           * skip the check. That allows for easy support of incremental sampledata loading into
           * existing clients.
           * 
           */
          if (clientFileIsDefined(entry)) {
            String clientId = getClientIdFromEntry(entry, db);
            DynaBean dbClient = findClient(platform, db, clientId);
            if (dbClient != null) {
              log.info("Client is already present in the database, skipping import");
              continue;
            }
          }

          final Vector<File> files = new Vector<File>();
          files.addAll(Arrays.asList(DatabaseUtils.readFileArray(entry)));
          files.addAll(Arrays.asList(DatabaseUtils.readCopyFileArray(entry)));
          Collections.sort(files);
          getLog().debug("Number of files read: " + files.size());

          getLog().info("Inserting data into the database...");

          for (int i = 0; i < files.size(); i++) {
            File f = files.get(i);
            getLog().debug("Importing data from file: " + files.get(i).getName());
            if (f.getName().endsWith(".xml")) {
              importXmlFile(f, platform, db);
            } else if (f.getName().endsWith(".copy")) {
              if (POSTGRE_RDBMS.equals(rdbms)) {
                importPgCopyFile(f, platform);
              } else {
                getLog().warn("File " + f.getName() + " cannot be imported in Oracle");
              }
            }
          }
        }
      }

      platform.updateDBStatistics();

      if (executeModuleScripts) {
        log.info("Running modulescripts...");
        try {
          ModuleScriptHandler hd = new ModuleScriptHandler();
          hd.setModulesVersionMap(new HashMap<String, String>());
          hd.setBasedir(new File(basedir));
          hd.execute();
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        log.info("Skipping modulescripts...");
      }

      log.info("Enabling constraints...");
      try {
        con = platform.borrowConnection();
        log.info("   Enabling check constraints");
        platform.enableCheckConstraints(db);
        log.info("   Enabling triggers");
        platform.enableAllTriggers(con, db, false);
        log.info("   Enabling foreign keys");
        platform.enableAllFK(con, db, false);
      } finally {
        if (con != null) {
          platform.returnConnection(con);
        }
      }

    } catch (final Exception e) {
      e.printStackTrace();
      throw new BuildException(e);
    }
  }

  private String getClientIdFromEntry(File referenceDataFolder, Database db) {
    String clientId = null;
    File clientFileXML = new File(referenceDataFolder, "AD_CLIENT.xml");
    File clientFileCopy = new File(referenceDataFolder, "AD_CLIENT.copy");
    if (clientFileXML.exists()) {
      try {
        Vector<DynaBean> clients = readOneDataXml(db, clientFileXML);
        DynaBean client = clients.get(0);
        clientId = (String) client.get("AD_CLIENT_ID");
      } catch (Exception e) {
        getLog().error("Error while obtaining the client ID from a XML file");
      }
    } else {
      clientId = getClientIdFromCopyFile(clientFileCopy);
    }
    return clientId;
  }

  // the client file can be stored as .xml and .copy
  private boolean clientFileIsDefined(File entry) {
    File clientFileXML = new File(entry, "AD_CLIENT.xml");
    File clientFileCopy = new File(entry, "AD_CLIENT.copy");
    return (clientFileXML.exists() || clientFileCopy.exists());
  }

  private String getClientIdFromCopyFile(File clientFileCopy) {
    String clientId = null;
    try (InputStream fis = new FileInputStream(clientFileCopy);
        InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
        BufferedReader br = new BufferedReader(isr);) {

      // skips the first line, it contains the column names
      br.readLine();
      // the values for the client is in the second line
      String values = br.readLine();
      clientId = values.substring(0, values.indexOf(","));
    } catch (Exception e) {
      getLog()
          .error(
              "Error while tring to obtain the client id from file "
                  + clientFileCopy.getAbsolutePath(), e);
    }
    return clientId;
  }

  /**
   * Parses a single sourcedata style database and returns it as a list of DynaBeans
   */
  private static Vector<DynaBean> readOneDataXml(Database db, File file) throws IOException,
      SAXException {
    final DatabaseDataIO dbdio = new DatabaseDataIO();
    DataReader dataReader = dbdio.getConfiguredCompareDataReader(db);

    dataReader.getSink().start();
    dataReader.parse(file);
    dataReader.getSink().end();

    return ((DataToArraySink) dataReader.getSink()).getVector();
  }

  /**
   * Loads the Dynabean for the specified AD_CLIENT from the database
   * 
   * @param clientId
   *          ad_client_id
   */
  private DynaBean findClient(final Platform platform, Database db, String clientId) {
    Connection connection = platform.borrowConnection();
    Table table = db.findTable("AD_CLIENT");
    DatabaseDataIO dbIO = new DatabaseDataIO();
    OBDatasetTable dsTable = new OBDatasetTable();
    dsTable.setWhereclause("1=1");
    dsTable.setSecondarywhereclause("ad_client_id" + "=" + "'" + clientId + "'");
    Vector<DynaBean> rowsNewData = dbIO.readRowsFromTableList(connection, platform, db, table,
        dsTable, null);
    platform.returnConnection(connection);
    if (rowsNewData != null && rowsNewData.size() > 0) {
      return rowsNewData.get(0);
    }
    return null;
  }

  public String getBasedir() {
    return basedir;
  }

  public void setBasedir(String basedir) {
    this.basedir = basedir;
  }

  public void setRdbms(String rdbms) {
    this.rdbms = rdbms;
  }

  public void setExecuteModuleScripts(boolean executeModuleScripts) {
    this.executeModuleScripts = executeModuleScripts;
  }

  private void importXmlFile(File file, Platform platform, Database db) {
    final DatabaseDataIO dbdio = new DatabaseDataIO();
    dbdio.setEnsureFKOrder(false);
    DataReader dataReader = null;
    dbdio.setUseBatchMode(true);
    dataReader = dbdio.getConfiguredDataReader(platform, db);
    dataReader.getSink().start();
    dbdio.writeDataToDatabase(dataReader, file);
    dataReader.getSink().end();
  }

  private void importPgCopyFile(File file, Platform platform) {
    final PostgreSqlDatabaseDataIO dbdio = new PostgreSqlDatabaseDataIO();
    dbdio.importCopyFile(file, platform);
  }

}
