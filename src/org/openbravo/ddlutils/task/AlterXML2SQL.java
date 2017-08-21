/*
 ************************************************************************************
 * Copyright (C) 2001-2017 Openbravo S.L.U.
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
import org.openbravo.ddlutils.task.DatabaseUtils.ConfigScriptConfig;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.OBDataset;

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
  public void execute() {

    try {
      excludeFilter = DBSMOBUtil.getInstance().getExcludeFilter(
          new File(model.getAbsolutePath() + "/../../../"));
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

      Writer w = new FileWriter(output);
      platform.getSqlBuilder().setScript(true);

      Database db = null;
      DatabaseInfo dbInfo = readDatabaseModel(new DatabaseModelConfig(platform, null, db, basedir,
          datafilter, input, strict, true));
      db = dbInfo.getDatabase();
      DatabaseData dbData = dbInfo.getDatabaseData();
      Database originaldb;
      if (getOriginalmodel() == null) {
        originaldb = platform.loadModelFromDatabase(excludeFilter);
        if (originaldb == null) {
          originaldb = new Database();
          getLog().info("Original model considered empty.");
        } else {
          getLog().info("Original model loaded from database.");
        }
      } else {
        // Load the model from the file
        boolean strictMode = true;
        boolean applyConfigScriptData = false;
        boolean loadModuleInfoFromXML = false;

        ConfigScriptConfig config = new ConfigScriptConfig(platform, basedir, strictMode,
            applyConfigScriptData, loadModuleInfoFromXML);
        originaldb = DatabaseUtils.readDatabase(getModel(), config);
        getLog().info("Original model loaded from file.");
      }

      DBSMOBUtil.getInstance().loadDataStructures(platform, dbData, originaldb, db, basedir,
          datafilter, input);

      getLog().info("Comparing databases to find differences");

      OBDataset ad = new OBDataset(dbData, "AD");

      final DataComparator dataComparator = new DataComparator(platform.getSqlBuilder()
          .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
      dataComparator.compareToUpdate(db, platform, dbData, ad, null);

      getLog().info("Data changes we will perform: ");
      for (final Change change : dataComparator.getChanges())
        getLog().info(change);

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
      w.write(platform.disableNOTNULLColumnsSql(db, ad));
      getLog().info("Updating database data...");
      platform.alterData(db, dataComparator.getChanges(), w);
      getLog().info("Removing invalid rows.");
      platform.getSqlBuilder().setWriter(w);
      platform.getSqlBuilder().deleteInvalidConstraintRows(db, ad, true);
      getLog().info("Recreating Primary Keys");
      List changes = platform.getSqlBuilder().alterDatabaseRecreatePKs(oldModel, db, null);
      getLog().info("Recreating not null constraints");
      w.write(platform.enableNOTNULLColumnsSql(db, ad));
      platform.enableNOTNULLColumns(db, ad);
      getLog().info("Executing update final script (NOT NULLs and dropping temporal tables");
      platform.getSqlBuilder().alterDatabasePostScript(oldModel, db, null, changes, null, ad);

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
