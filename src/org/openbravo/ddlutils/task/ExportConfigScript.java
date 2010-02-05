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

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.ColumnDataChange;
import org.apache.ddlutils.alteration.ColumnSizeChange;
import org.apache.ddlutils.alteration.DataChange;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.alteration.ModelChange;
import org.apache.ddlutils.alteration.RemoveCheckChange;
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DataToArraySink;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.tools.ant.BuildException;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.OBDataset;

/**
 * 
 * @author adrian
 */
public class ExportConfigScript extends BaseDalInitializingTask {

  private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

  private File prescript = null;
  private File postscript = null;

  private File model = null;
  private String coreData = null;
  private File moduledir;
  private String filter = "org.apache.ddlutils.io.AllDatabaseFilter";

  private File output;
  private String encoding = "UTF-8";

  private String codeRevision;
  private String industryTemplate;

  /** Creates a new instance of WriteDataXML */
  public ExportConfigScript() {
  }

  @Override
  public void execute() {
    initLogging();
    try {
      if (industryTemplate == null) {
        throw new BuildException("No industry template was specified.");
      }

      final BasicDataSource ds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getUser(),
          getPassword());

      final Platform platform = PlatformFactory.createNewPlatformInstance(ds);
      final DBSMOBUtil util = DBSMOBUtil.getInstance();
      util.getModules(platform, excludeobjects);
      util.checkTemplateExportIsPossible(log);
      final String indTemp = util.getNameOfActiveIndustryTemplate();
      industryTemplate = indTemp;

      // util.getModulesForIndustryTemplate(industryTemplate, new
      // Vector<String>());
      getLog().info("Loading model from XML files");
      final Vector<File> dirs = new Vector<File>();
      dirs.add(model);

      for (int j = 0; j < util.getModuleCount(); j++) {
        if (!util.getModule(j).name.equalsIgnoreCase("CORE")) {
          final File dirF = new File(moduledir, util.getModule(j).dir + "/src-db/database/model/");
          System.out.println(dirF.getAbsolutePath());
          if (dirF.exists()) {
            dirs.add(dirF);
          }
        }
      }
      final File[] fileArray = new File[dirs.size()];
      for (int i = 0; i < dirs.size(); i++) {
        getLog().info("Loading model for module. Path: " + dirs.get(i).getAbsolutePath());
        fileArray[i] = dirs.get(i);
      }
      final Database xmlModel = DatabaseUtils.readDatabase(fileArray);

      getLog().info("Loading original data from XML files");

      final DatabaseDataIO dbdio = new DatabaseDataIO();
      dbdio.setEnsureFKOrder(false);
      dbdio.setDatabaseFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), xmlModel));

      final DataReader dataReader = dbdio.getConfiguredCompareDataReader(xmlModel);

      final Vector<File> dataFiles = DBSMOBUtil.loadFilesFromFolder(getCoreData());

      Vector<File> configScripts = new Vector<File>();
      for (int j = 0; j < util.getModuleCount(); j++) {
        if (!util.getModule(j).name.equalsIgnoreCase("CORE")) {
          final File dirF = new File(moduledir, util.getModule(j).dir
              + "/src-db/database/sourcedata/");
          System.out.println(dirF.getAbsolutePath());
          if (dirF.exists()) {
            dataFiles.addAll(DBSMOBUtil.loadFilesFromFolder(dirF.getAbsolutePath()));
          }
          File configScript = new File(moduledir, util.getModule(j).dir
              + "/src-db/database/configScript.xml");
          if (!util.getModule(j).dir.equals(industryTemplate) && configScript.exists())
            configScripts.add(configScript);
        }
      }

      final DatabaseData databaseOrgData = new DatabaseData(xmlModel);
      for (int i = 0; i < dataFiles.size(); i++) {
        // getLog().info("Loading data for module. Path:
        // "+dataFiles.get(i).getAbsolutePath());
        try {
          dataReader.getSink().start();
          final String tablename = dataFiles.get(i).getName().substring(0,
              dataFiles.get(i).getName().length() - 4);
          final Vector<DynaBean> vectorDynaBeans = ((DataToArraySink) dataReader.getSink())
              .getVector();
          dataReader.parse(dataFiles.get(i));
          databaseOrgData.insertDynaBeansFromVector(tablename, vectorDynaBeans);
          dataReader.getSink().end();
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }

      getLog().info("Loading and applying configuration scripts");
      DatabaseIO dbIOs = new DatabaseIO();
      for (File f : configScripts) {
        getLog().info("Loading configuration script: " + f.getAbsolutePath());
        Vector<Change> changes = dbIOs.readChanges(f);
        for (Change change : changes) {
          if (change instanceof ModelChange)
            ((ModelChange) change).apply(xmlModel, platform.isDelimitedIdentifierModeOn());
          else if (change instanceof DataChange)
            ((DataChange) change).apply(databaseOrgData, platform.isDelimitedIdentifierModeOn());
          getLog().debug(change);
        }
      }

      getLog().info("Loading complete model from current database");
      final Database currentdb = platform.loadModelFromDatabase(DatabaseUtils
          .getExcludeFilter(excludeobjects));

      getLog().info("Creating submodels for modules");

      Database databaseModel = null;
      for (int i = 0; i < util.getModuleCount(); i++) {
        getLog().info("Creating submodel for module: " + util.getModule(i).name);
        Database dbI = null;
        try {
          dbI = (Database) currentdb.clone();
        } catch (final Exception e) {
          System.out.println("Error while cloning the database model" + e.getMessage());
          e.printStackTrace();
          return;
        }
        try {
          dbI.applyNamingConventionFilter(util.getModule(i).filter);
          if (databaseModel == null)
            databaseModel = dbI;
          else
            databaseModel.mergeWith(dbI);
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }

      getLog().info("Comparing models...");
      final Vector<String> modIds = new Vector<String>();
      for (int i = 0; i < util.getModuleCount(); i++) {
        final String mod = util.getModule(i).idMod;
        getLog().info("Module added to comparison: " + mod);
        modIds.add(mod);
      }

      OBDataset ad = new OBDataset(databaseOrgData, "AD");
      final DataComparator dataComparator = new DataComparator(platform.getSqlBuilder()
          .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
      dataComparator.setFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), currentdb));
      dataComparator.compare(xmlModel, databaseModel, platform, databaseOrgData, ad, null);
      final Vector<Change> dataChanges = new Vector<Change>();
      dataChanges.addAll(dataComparator.getChanges());
      final Vector<Change> finalChanges = new Vector<Change>();
      final Vector<Change> comparatorChanges = new Vector<Change>();
      comparatorChanges.addAll(dataComparator.getModelChangesList());
      for (final Object change : dataComparator.getModelChangesList())
        if (change instanceof ColumnSizeChange || change instanceof RemoveCheckChange) {
          finalChanges.add((Change) change);
          comparatorChanges.remove(change);
        }

      for (final Change change : dataComparator.getChanges())
        if (change instanceof ColumnDataChange) {
          finalChanges.add((change));
          dataChanges.remove(change);
        }

      comparatorChanges.addAll(dataChanges);

      final DatabaseIO dbIO = new DatabaseIO();

      final File configFile = new File(moduledir, industryTemplate
          + "/src-db/database/configScript.xml");
      final File folder = new File(configFile.getParent());

      folder.mkdirs();
      dbIO.write(configFile, finalChanges);

      if (comparatorChanges.size() > 0) {
        getLog().info("Changes that couldn't be exported to the config script:");
        getLog().info("*******************************************************");
      }
      for (final Change c : comparatorChanges) {
        getLog().info(c);
      }
      DBSMOBUtil.getInstance().updateCRC(platform);
    } catch (Exception e) {
      e.printStackTrace();
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

  public File getModuledir() {
    return moduledir;
  }

  public void setModuledir(File moduledir) {
    this.moduledir = moduledir;
  }

  public String getCoreData() {
    return coreData;
  }

  public void setCoreData(String coreData) {
    this.coreData = coreData;
  }

  public String getIndustryTemplate() {
    return industryTemplate;
  }

  public void setIndustryTemplate(String industryTemplate) {
    this.industryTemplate = industryTemplate;
  }

}
