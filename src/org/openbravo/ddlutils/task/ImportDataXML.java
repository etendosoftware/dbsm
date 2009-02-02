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
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.ModuleRow;

/**
 * 
 * @author adrian
 */
public class ImportDataXML extends BaseDatabaseTask {

  private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

  private File prescript = null;
  private File postscript = null;

  private File model = null;
  private String filter = "org.apache.ddlutils.io.NoneDatabaseFilter";

  private String basedir;
  private String input;
  private String encoding = "UTF-8";

  /** Creates a new instance of ReadDataXML */
  public ImportDataXML() {
    super();
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

    final String filters = getFilter();
    final StringTokenizer strTokFil = new StringTokenizer(filters, ",");
    final String folders = getInput();
    final StringTokenizer strTokFol = new StringTokenizer(folders, ",");

    final Vector<File> files = new Vector<File>();

    while (strTokFol.hasMoreElements()) {
      if (basedir == null) {
        getLog().info("Basedir not specified, will insert just Core data files.");
        final String folder = strTokFol.nextToken();
        final File[] fileArray = DatabaseUtils.readFileArray(new File(folder));
        for (int i = 0; i < fileArray.length; i++)
          files.add(fileArray[i]);
      } else {
        final String token = strTokFol.nextToken();
        final DirectoryScanner dirScanner = new DirectoryScanner();
        dirScanner.setBasedir(new File(basedir));
        final String[] dirFilterA = { token };
        dirScanner.setIncludes(dirFilterA);
        dirScanner.scan();
        final String[] incDirs = dirScanner.getIncludedDirectories();
        for (int j = 0; j < incDirs.length; j++) {
          final File dirFolder = new File(basedir, incDirs[j] + "/");
          final File[] fileArray = DatabaseUtils.readFileArray(dirFolder);
          for (int i = 0; i < fileArray.length; i++) {
            files.add(fileArray[i]);
          }
        }
      }
    }

    try {
      // execute the pre-script
      if (getPrescript() == null) {
        // try to execute the default prescript
        final File fpre = new File(getInput(), "prescript-" + platform.getName() + ".sql");
        if (fpre.exists()) {
          getLog().info("Executing default prescript");
          platform.evaluateBatch(DatabaseUtils.readFile(fpre), true);
        }
      } else {
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
      DataReader dataReader = null;
      while (strTokFil.hasMoreElements()) {
        final String filter = strTokFil.nextToken();
        if (filter != null && !filter.equals("")) {
          dbdio.setDatabaseFilter(DatabaseUtils.getDynamicDatabaseFilter(filter, originaldb));
          dataReader = dbdio.getConfiguredDataReader(platform, originaldb);
          dataReader.getSink().start(); // we do this to delete data
          // from tables in each of the
          // filters
        }
      }
      for (int i = 0; i < files.size(); i++) {
        getLog().debug("Importing data from file: " + files.get(i).getName());
        dbdio.writeDataToDatabase(dataReader, files.get(i));
      }

      dataReader.getSink().end();

      // execute the post-script
      if (getPostscript() == null) {
        // try to execute the default prescript
        final File fpost = new File(getInput(), "postscript-" + platform.getName() + ".sql");
        if (fpost.exists()) {
          getLog().info("Executing default postscript");
          platform.evaluateBatch(DatabaseUtils.readFile(fpost), true);
        }
      } else {
        platform.evaluateBatch(DatabaseUtils.readFile(getPostscript()), true);
      }

      final DBSMOBUtil util = DBSMOBUtil.getInstance();
      util.getModules(platform, "org.apache.ddlutils.platform.ExcludeFilter");
      util.generateIndustryTemplateTree();
      for (int i = 0; i < util.getIndustryTemplateCount(); i++) {
        final ModuleRow temp = util.getIndustryTemplateId(i);
        final File f = new File(basedir, "modules/" + temp.dir
            + "/src-db/database/configScript.xml");
        getLog().info(
            "Loading config script for module " + temp.name + ". Path: " + f.getAbsolutePath());
        if (f.exists()) {
          final DatabaseIO dbIO = new DatabaseIO();
          final Vector<Change> changesConfigScript = dbIO.readChanges(f);
          platform.applyConfigScript(originaldb, changesConfigScript);
        } else {
          getLog().error(
              "Error. We couldn't find configuration script for template " + temp.name + ". Path: "
                  + f.getAbsolutePath());
        }
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

  public String getInput() {
    return input;
  }

  public void setInput(String input) {
    this.input = input;
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

  public String getBasedir() {
    return basedir;
  }

  public void setBasedir(String basedir) {
    this.basedir = basedir;
  }

}
