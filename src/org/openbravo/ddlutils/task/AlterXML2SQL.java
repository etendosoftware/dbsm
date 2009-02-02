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
import java.io.IOException;
import java.io.Writer;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.model.Database;
import org.apache.tools.ant.BuildException;

/**
 * 
 * @author adrian
 */
public class AlterXML2SQL extends BaseDatabaseTask {

  private String platform = null;
  private File originalmodel = null;

  private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

  private File model;
  private File output;

  private String object = null;

  /** Creates a new instance of ExecuteXML2SQL */
  public AlterXML2SQL() {
  }

  @Override
  public void doExecute() {

    Platform pl;
    Database originaldb;

    if (platform == null || originalmodel == null) {
      final BasicDataSource ds = new BasicDataSource();
      ds.setDriverClassName(getDriver());
      ds.setUrl(getUrl());
      ds.setUsername(getUser());
      ds.setPassword(getPassword());

      pl = PlatformFactory.createNewPlatformInstance(ds);
      // platform.setDelimitedIdentifierModeOn(true);
      getLog().info("Using database platform.");

      try {

        if (getOriginalmodel() == null) {
          originaldb = pl.loadModelFromDatabase(DatabaseUtils.getExcludeFilter(excludeobjects));
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
      } catch (final Exception e) {
        // log(e.getLocalizedMessage());
        throw new BuildException(e);
      }
    } else {
      pl = PlatformFactory.createNewPlatformInstance(platform);
      getLog().info("Using platform : " + platform);

      originaldb = DatabaseUtils.readDatabase(originalmodel);
      getLog().info("Original model loaded from file.");
    }

    try {

      Database db = DatabaseUtils.readDatabase(model);

      // Write update script
      getLog().info("Writing update script");
      // crop database if needed
      if (object != null) {
        db = DatabaseUtils.cropDatabase(originaldb, db, object);
        getLog().info("for database object " + object);
      } else {
        getLog().info("for the complete database");
      }

      final Writer w = new FileWriter(output);
      pl.getSqlBuilder().setWriter(w);
      pl.getSqlBuilder().alterDatabase(originaldb, db, null);
      pl.getSqlBuilder().alterDatabasePostScript(originaldb, db, null);
      w.close();

      getLog().info("Database script created in : " + output.getPath());

    } catch (final IOException e) {
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

  public String getPlatform() {
    return platform;
  }

  public void setPlatform(String platform) {
    this.platform = platform;
  }

  public File getOriginalmodel() {
    return originalmodel;
  }

  public void setOriginalmodel(File originalmodel) {
    this.originalmodel = originalmodel;
  }

  public File getOutput() {
    return output;
  }

  public void setOutput(File output) {
    this.output = output;
  }

  public File getModel() {
    return model;
  }

  public void setModel(File model) {
    this.model = model;
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
