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
import java.io.FileWriter;
import java.io.Writer;
import java.sql.Connection;
import java.util.List;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.tools.ant.BuildException;
import org.openbravo.dal.service.OBDal;

/**
 * 
 * @author adrian
 */
public class AlterXML2SQL extends AlterDatabaseDataAll {

  private File output;

  /** Creates a new instance of ExecuteXML2SQL */
  public AlterXML2SQL() {
  }

  @Override
  public void doExecute() {

    try {
      getLog().info("Database connection: " + getUrl() + ". User: " + getUser());

      final BasicDataSource ds = new BasicDataSource();
      ds.setDriverClassName(getDriver());
      ds.setUrl(getUrl());
      ds.setUsername(getUser());
      ds.setPassword(getPassword());
      if (getDriver().contains("Oracle"))
        ds.setValidationQuery("SELECT 1 FROM DUAL");
      else
        ds.setValidationQuery("SELECT 1");
      ds.setTestOnBorrow(true);

      final Platform platform = PlatformFactory.createNewPlatformInstance(ds);
      // platform.setDelimitedIdentifierModeOn(true);

      Writer w = new FileWriter(output);
      platform.getSqlBuilder().setScript(true);

      Database db = null;
      db = readDatabaseModel();

      Database originaldb;
      if (getOriginalmodel() == null) {
        originaldb = platform.loadModelFromDatabase(DatabaseUtils.getExcludeFilter(excludeobjects));
        if (originaldb == null) {
          originaldb = new Database();
          getLog().info("Original model considered empty.");
        } else {
          getLog().info("Original model loaded from database.");
        }
      } else {
        // Load the model from the file
        originaldb = DatabaseUtils.readDatabase(getModel());
        getLog().info("Original model loaded from file.");
      }

      final DatabaseData databaseOrgData = new DatabaseData(db);
      loadDataStructures(platform, databaseOrgData, originaldb, db);

      getLog().info("Comparing databases to find differences");

      final DataComparator dataComparatorDS = new DataComparator(platform.getSqlBuilder()
          .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
      dataComparatorDS.compareUsingDALToUpdate(db, platform, databaseOrgData, "DS", null);
      if (dataComparatorDS.getChanges().size() > 0) {
        String message = "The Dataset DS definition was changed. The update.database.script will need to be run a second time (after the script has been run in the database) to get the second part of the script update. Both scripts will need to be executed in the final database to obtain the desired result.";
        getLog().info(message);
        w.append("-- " + message + "\n\n");
      }

      final DataComparator dataComparator = new DataComparator(platform.getSqlBuilder()
          .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
      dataComparator.compareUsingDALToUpdate(db, platform, databaseOrgData, "ADCS", null);

      getLog().info("Data changes we will perform: ");
      for (final Change change : dataComparator.getChanges())
        getLog().info(change);

      OBDal.getInstance().commitAndClose();

      // execute the pre-script
      // try to execute the default prescript
      final File fpre = new File(getModel(), "prescript-" + platform.getName() + ".sql");
      if (fpre.exists()) {
        getLog().info("Appending default prescript");
        w.append(DatabaseUtils.readFile(fpre));
      }
      final Database oldModel = (Database) originaldb.clone();
      platform.getSqlBuilder().setWriter(w);
      getLog().info("Updating database model...");
      platform.getSqlBuilder().alterDatabase(originaldb, db, null);
      getLog().info("Model update complete.");

      getLog().info("Disabling foreign keys");
      final Connection connection = platform.borrowConnection();
      platform.disableAllFK(originaldb, !isFailonerror(), w);
      getLog().info("Disabling triggers");
      platform.disableAllTriggers(db, !isFailonerror(), w);
      getLog().info("Updating database data...");
      platform.alterData(db, dataComparator.getChanges(), w);
      getLog().info("Removing invalid rows.");
      platform.getSqlBuilder().setWriter(w);
      platform.getSqlBuilder().deleteInvalidConstraintRows(db);
      getLog().info("Recreating Primary Keys");
      List changes = platform.getSqlBuilder().alterDatabaseRecreatePKs(oldModel, db, null);
      getLog().info("Executing update final script (NOT NULLs and dropping temporal tables");
      platform.getSqlBuilder().alterDatabasePostScript(oldModel, db, null, changes, null);

      getLog().info("Enabling Foreign Keys and Triggers");
      platform.enableAllFK(originaldb, !isFailonerror(), w);
      platform.enableAllTriggers(db, !isFailonerror(), w);

      // execute the post-script
      // try to execute the default prescript
      final File fpost = new File(getModel(), "postscript-" + platform.getName() + ".sql");
      if (fpost.exists()) {
        getLog().info("Executing default postscript");
        w.append(DatabaseUtils.readFile(fpost));
      }
      w.flush();
      w.close();

    } catch (final Exception e) {
      // log(e.getLocalizedMessage());
      e.printStackTrace();
      throw new BuildException(e);
    }
  }

  public File getOutput() {
    return output;
  }

  public void setOutput(File output) {
    this.output = output;
  }

}
