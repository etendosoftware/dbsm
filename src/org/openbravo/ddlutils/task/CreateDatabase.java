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
import java.util.Vector;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.model.Database;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.openbravo.ddlutils.util.DBSMOBUtil;

/**
 * 
 * @author Adrian
 */
public class CreateDatabase extends BaseDatabaseTask {

  private File prescript = null;
  private File postscript = null;

  private File model;
  private boolean dropfirst = false;
  private boolean failonerror = false;

  private String object = null;

  private String basedir;
  private String dirFilter;

  /** Creates a new instance of CreateDatabase */
  public CreateDatabase() {
  }

  @Override
  public void doExecute() {
    getLog().info("Database connection: " + getUrl() + ". User: " + getUser());
    final BasicDataSource ds = new BasicDataSource();
    ds.setDriverClassName(getDriver());
    ds.setUrl(getUrl());
    ds.setUsername(getUser());
    ds.setPassword(getPassword());

    final Platform platform = PlatformFactory.createNewPlatformInstance(ds);
    // platform.setDelimitedIdentifierModeOn(true);

    try {

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

      Database db = null;
      if (basedir == null) {
        getLog().info(
            "Basedir for additional files not specified. Creating database with just Core.");
        db = DatabaseUtils.readDatabase(getModel());
      } else {
        // We read model files using the filter, obtaining a file array.
        // The models will be merged
        // to create a final target model.
        final Vector<File> dirs = new Vector<File>();
        dirs.add(model);
        final DirectoryScanner dirScanner = new DirectoryScanner();
        dirScanner.setBasedir(new File(basedir));
        final String[] dirFilterA = { dirFilter };
        dirScanner.setIncludes(dirFilterA);
        dirScanner.scan();
        final String[] incDirs = dirScanner.getIncludedDirectories();
        for (int j = 0; j < incDirs.length; j++) {
          final File dirF = new File(basedir, incDirs[j]);
          dirs.add(dirF);
        }
        final File[] fileArray = new File[dirs.size()];
        for (int i = 0; i < dirs.size(); i++) {
          fileArray[i] = dirs.get(i);
        }
        db = DatabaseUtils.readDatabase(fileArray);
      }

      // Create database
      getLog().info("Executing creation script");
      // crop database if needed
      if (object != null) {
        final Database empty = new Database();
        empty.setName("empty");
        db = DatabaseUtils.cropDatabase(empty, db, object);
        getLog().info("for database object " + object);
      } else {
        getLog().info("for the complete database");
      }

      platform.createTables(db, isDropfirst(), !isFailonerror());

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
      getLog().info("Writing checksum info");
      DBSMOBUtil.writeCheckSumInfo(new File(model.getAbsolutePath() + "/../../../")
          .getAbsolutePath());

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

  public String getBasedir() {
    return basedir;
  }

  public void setBasedir(String basedir) {
    this.basedir = basedir;
  }

  public String getDirFilter() {
    return dirFilter;
  }

  public void setDirFilter(String dirFilter) {
    this.dirFilter = dirFilter;
  }

  public boolean isDropfirst() {
    return dropfirst;
  }

  public void setDropfirst(boolean dropfirst) {
    this.dropfirst = dropfirst;
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
