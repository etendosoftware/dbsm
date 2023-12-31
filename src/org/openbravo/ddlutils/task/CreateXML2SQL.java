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

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.model.Database;
import org.apache.tools.ant.BuildException;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.ddlutils.task.DatabaseUtils.ConfigScriptConfig;

/**
 * 
 * @author adrian
 */
public class CreateXML2SQL extends BaseDatabaseTask {

  private File model;
  private File output;
  private boolean dropfirst;

  private String object = null;

  /** Creates a new instance of ExecuteXML2SQL */
  public CreateXML2SQL() {
  }

  @Override
  public void doExecute() {
  }

  @Override
  public void execute() {

    final BasicDataSource ds = new BasicDataSource();
    ds.setDriverClassName(getDriver());
    ds.setUrl(getUrl());
    ds.setUsername(getUser());
    ds.setPassword(getPassword());

    Platform pl = PlatformFactory.createNewPlatformInstance(ds);
    getLog().info("Using database platform " + pl.getName());

    try {
      String basedir = OBPropertiesProvider.getInstance()
          .getOpenbravoProperties()
          .getProperty("source.path");
      boolean strictMode = true;
      boolean applyConfigScriptData = false;
      ConfigScriptConfig config = new ConfigScriptConfig(pl, basedir, strictMode,
          applyConfigScriptData);
      Database db = DatabaseUtils.readDatabase(model, config);

      // Write creation script
      getLog().info("Writing creation script");
      // crop database if needed
      if (object != null) {
        final Database empty = new Database();
        empty.setName("empty");
        db = DatabaseUtils.cropDatabase(empty, db, object);
        getLog().info("for database object " + object);
      } else {
        getLog().info("for the complete database");
      }

      final Writer w = new FileWriter(output);
      w.write(pl.getCreateTablesSqlScript(db, isDropfirst(), false));
      w.close();

      getLog().info("Database script created in : " + output.getPath());

    } catch (final Exception e) {
      throw new BuildException(e);
    }
  }

  public File getModel() {
    return model;
  }

  public void setModel(File model) {
    this.model = model;
  }

  public File getOutput() {
    return output;
  }

  public void setOutput(File output) {
    this.output = output;
  }

  public boolean isDropfirst() {
    return dropfirst;
  }

  public void setDropfirst(boolean dropfirst) {
    this.dropfirst = dropfirst;
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
