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
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.io.DatabaseFilter;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.io.UniqueDatabaseFilter;
import org.apache.ddlutils.model.Database;
import org.apache.tools.ant.BuildException;
import org.openbravo.ddlutils.util.DBSMOBUtil;

/**
 * 
 * @author adrian
 */
public class ExportDataXML extends BaseDatabaseTask {

  private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

  private File prescript = null;
  private File postscript = null;

  private File model = null;
  private String filter = "org.apache.ddlutils.io.AllDatabaseFilter";

  private File output;
  private String encoding = "UTF-8";

  private String codeRevision;

  /** Creates a new instance of WriteDataXML */
  public ExportDataXML() {
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
    DBSMOBUtil.verifyRevision(platform, getCodeRevision(), getLog());

    try {
      // execute the pre-script
      if (getPrescript() != null) {
        platform.evaluateBatch(DatabaseUtils.readFile(getPrescript()), true);
      }

      Database originaldb;
      if (getModel() == null) {
        originaldb = platform.loadModelFromDatabase(DatabaseUtils.getExcludeFilter(excludeobjects));
        if (originaldb == null) {
          originaldb = new Database();
          getLog().info("Model considered empty.");
        } else {
          getLog().info("Model loaded from database.");
        }
      } else {
        // Load the model from the file
        originaldb = DatabaseUtils.readDatabase(getModel());
        getLog().info("Model loaded from file.");
      }

      final DatabaseDataIO dbdio = new DatabaseDataIO();
      dbdio.setEnsureFKOrder(false);
      final DatabaseFilter dbfilter = DatabaseUtils.getDynamicDatabaseFilter(getFilter(),
          originaldb);

      if (getOutput().isDirectory()) {
        // First we delete all .xml files in the directory

        final File[] filestodelete = DatabaseIO.readFileArray(getOutput());
        for (final File filedelete : filestodelete) {
          filedelete.delete();
        }

        // Create a set of files one for each table

        final String[] tablenames = dbfilter.getTableNames();
        for (int i = 0; i < tablenames.length; i++) {

          final OutputStream out = new FileOutputStream(new File(getOutput(), tablenames[i]
              + ".xml"));
          dbdio.setDatabaseFilter(new UniqueDatabaseFilter(dbfilter, tablenames[i]));
          dbdio.writeDataToXML(platform, originaldb, out, getEncoding());
          out.close();
        }

      } else {
        // Create a single file

        final OutputStream out = new FileOutputStream(getOutput());
        dbdio.setDatabaseFilter(dbfilter);
        dbdio.writeDataToXML(platform, originaldb, out, getEncoding());
        out.close();
      }

      // execute the post-script
      if (getPostscript() != null) {
        platform.evaluateBatch(DatabaseUtils.readFile(getPostscript()), true);
      }
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

  public File getModel() {
    return model;
  }

  public void setModel(File model) {
    this.model = model;
  }

  public void setFilter(String filter) {
    this.filter = filter;
  }

  public String getFilter() {
    return filter;
  }

  public File getOutput() {
    return output;
  }

  public void setOutput(File output) {
    this.output = output;
  }

  public String getEncoding() {
    return encoding;
  }

  public void setEncoding(String encoding) {
    this.encoding = encoding;
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

  public String getCodeRevision() {
    return codeRevision;
  }

  public void setCodeRevision(String rev) {
    codeRevision = rev;
  }

}
