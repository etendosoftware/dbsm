/*
 ************************************************************************************
 * Copyright (C) 2013 Openbravo S.L.U.
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
import java.util.Arrays;
import java.util.Vector;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.model.Database;
import org.apache.tools.ant.BuildException;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.modulescript.ModuleScriptHandler;

/**
 *
 * @author huehner
 */
public class ImportSampledata extends BaseDatabaseTask {


  private String basedir;

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

          final Vector<File> files = new Vector<File>();
          final File[] fileArray = DatabaseUtils.readFileArray(entry);
          Arrays.sort(fileArray);
          for (int i = 0; i < fileArray.length; i++) {
            files.add(fileArray[i]);
          }
          getLog().debug("Number of files read: " + files.size());

          getLog().info("Inserting data into the database...");
          // Now we insert the data into the database
          final DatabaseDataIO dbdio = new DatabaseDataIO();
          dbdio.setEnsureFKOrder(false);
          DataReader dataReader = null;
          dbdio.setUseBatchMode(true);

          dataReader = dbdio.getConfiguredDataReader(platform, db);
          dataReader.getSink().start();

          for (int i = 0; i < files.size(); i++) {
            getLog().debug("Importing data from file: " + files.get(i).getName());
            dbdio.writeDataToDatabase(dataReader, files.get(i));
          }

          dataReader.getSink().end();
        }
      }

      log.info("Running modulescripts...");
      try {
        ModuleScriptHandler hd = new ModuleScriptHandler();
        hd.setBasedir(new File(basedir));
        hd.execute();
      } catch (Exception e) {
        e.printStackTrace();
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

  public String getBasedir() {
    return basedir;
  }

  public void setBasedir(String basedir) {
    this.basedir = basedir;
  }

}
