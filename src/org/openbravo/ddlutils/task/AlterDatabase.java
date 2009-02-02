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

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.model.Database;
import org.apache.tools.ant.BuildException;

/**
 * 
 * @author adrian
 */
public class AlterDatabase extends BaseDatabaseTask {

  private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

  private File prescript = null;
  private File postscript = null;

  private File originalmodel;
  private File model;
  private boolean failonerror = false;

  private String object = null;

  /** Creates a new instance of CreateDatabase */
  public AlterDatabase() {
  }

  @Override
  public void doExecute() {

    final BasicDataSource ds = new BasicDataSource();
    ds.setDriverClassName(getDriver());
    ds.setUrl(getUrl());
    ds.setUsername(getUser());
    ds.setPassword(getPassword());

    final Platform platform = PlatformFactory.createNewPlatformInstance(ds);
    // platform.setDelimitedIdentifierModeOn(true);

    try {

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

      // execute the pre-script
      if (getPrescript() == null) {
        // try to execute the default prescript
        final File fpre = new File(getModel(), "prescript-" + platform.getName() + ".sql");
        if (fpre.exists()) {
          getLog().info("Executing default prescript");
          platform.evaluateBatch(DatabaseUtils.readFile(fpre), !isFailonerror());
        }
      } else {
        platform.evaluateBatch(DatabaseUtils.readFile(getPrescript()), !isFailonerror());
      }

      Database db = DatabaseUtils.readDatabase(getModel());

      // Alter the database
      getLog().info("Executing update script");
      // crop database if needed
      if (object != null) {
        db = DatabaseUtils.cropDatabase(originaldb, db, object);
        getLog().info("for database object " + object);
      } else {
        getLog().info("for the complete database");
      }

      platform.alterTables(originaldb, db, !isFailonerror());
      platform.alterTablesPostScript(originaldb, db, !isFailonerror());

      // execute the post-script
      if (getPostscript() == null) {
        // try to execute the default prescript
        final File fpost = new File(getModel(), "postscript-" + platform.getName() + ".sql");
        if (fpost.exists()) {
          getLog().info("Executing default postscript");
          platform.evaluateBatch(DatabaseUtils.readFile(fpost), !isFailonerror());
        }
      } else {
        platform.evaluateBatch(DatabaseUtils.readFile(getPostscript()), !isFailonerror());
      }

      // try {
      // getLog().info("Executing system model script");
      // DatabaseUtils.manageDatabase(ds);
      // } catch (SQLException ex) {
      // // Exception if already exists the table.
      // }
      //            
      // try {
      // // Save model in the database if posible
      // DatabaseUtils.saveCurrentDatabase(ds, db);
      // } catch (SQLException ex) {
      // getLog().info("Database model not saved in the database.");
      // }

      // save the model
    } catch (final Exception e) {
      // log(e.getLocalizedMessage());
      throw new BuildException(e);
    }
  }

  public String getExcludeobjects() {
    return excludeobjects;
  }

  public void setExcludeobjects(String excludeobjects) {
    this.excludeobjects = excludeobjects;
  }

  public File getOriginalmodel() {
    return originalmodel;
  }

  public void setOriginalmodel(File input) {
    this.originalmodel = input;
  }

  public File getModel() {
    return model;
  }

  public void setModel(File model) {
    this.model = model;
  }

  public boolean isFailonerror() {
    return failonerror;
  }

  public void setFailonerror(boolean failonerror) {
    this.failonerror = failonerror;
  }

  public File getPrescript() {
    return prescript;
  }

  public void setPrescript(File prescript) {
    this.prescript = prescript;
  }

  public File getPostscript() {
    return postscript;
  }

  public void setPostscript(File postscript) {
    this.postscript = postscript;
  }

  public void setObject(String object) {
    if (object == null || object.trim().startsWith("$") || object.trim().equals("")) {
      this.object = null;
    } else {
      this.object = object;
    }
  }

  public String getObject() {
    return object;
  }

}
