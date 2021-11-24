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
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.ColumnDataChange;
import org.apache.ddlutils.alteration.DataChange;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.alteration.ModelChange;
import org.apache.ddlutils.alteration.RemoveTriggerChange;
import org.apache.ddlutils.alteration.VersionInfo;
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DataToArraySink;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.apache.tools.ant.BuildException;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.ModulesUtil;
import org.openbravo.ddlutils.util.OBDataset;

/**
 * 
 * @author adrian
 */
public class ExportConfigScript extends BaseDatabaseTask {

  private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

  private File prescript = null;
  private File postscript = null;

  private File model = null;
  private String coreData = null;
  private File moduledir;
  private File modulecoredir;

  private File output;
  private String encoding = "UTF-8";

  private String codeRevision;
  private String industryTemplate;
  private ExcludeFilter excludeFilter;

  public ExportConfigScript() {
  }

  @Override
  protected void doExecute() {
    excludeFilter = DBSMOBUtil.getInstance()
        .getExcludeFilter(new File(model.getAbsolutePath() + "/../../../"));
    try {
      if (industryTemplate == null) {
        throw new BuildException("No industry template was specified.");
      }

      final BasicDataSource ds = DBSMOBUtil.getDataSource(getDriver(), getUrl(), getUser(),
          getPassword());

      final Platform platform = PlatformFactory.createNewPlatformInstance(ds);
      final DBSMOBUtil util = DBSMOBUtil.getInstance();
      util.getModules(platform, excludeFilter);
      util.checkTemplateExportIsPossible(log);
      final String indTemp = util.getNameOfActiveIndustryTemplate();
      industryTemplate = indTemp;

      getLog().info("Loading model from XML files");
      final Vector<File> dirs = new Vector<File>();
      dirs.add(model);
      ModulesUtil.checkCoreInSources(ModulesUtil.coreInSources());

      String rootDir = ModulesUtil.getProjectRootDir();
      File rootDirFile = new File(rootDir);
      getLog().info("Export config script root dir: " + rootDirFile.getAbsolutePath());

      for (int j = 0; j < util.getModuleCount(); j++) {
        if (!util.getModule(j).name.equalsIgnoreCase("CORE")) {
          for (String modDir : ModulesUtil.moduleDirs) {
            File modDirFile = new File(rootDirFile, modDir);
            getLog().debug("Looking for module model '" + util.getModule(j).dir + "' in: " + modDirFile.getAbsolutePath());
            File dirF = new File(modDirFile,util.getModule(j).dir + "/src-db/database/model/");
            if (dirF.exists()) {
              getLog().debug("Module dir found: " + util.getModule(j).dir);
              dirs.add(dirF);
              break;
            }
          }
        }
      }
      final File[] fileArray = new File[dirs.size()];
      for (int i = 0; i < dirs.size(); i++) {
        getLog().debug("Loading model for module. Path: " + dirs.get(i).getAbsolutePath());
        fileArray[i] = dirs.get(i);
      }
      // ConfigScripts should not be applied in order to export changes into a configScript
      final Database xmlModel = DatabaseUtils.readDatabaseWithoutConfigScript(fileArray);

      getLog().info("Loading original data from XML files");

      final DatabaseDataIO dbdio = new DatabaseDataIO();
      dbdio.setEnsureFKOrder(false);

      final DataReader dataReader = dbdio.getConfiguredCompareDataReader(xmlModel);

      final Vector<File> dataFiles = DBSMOBUtil.loadFilesFromFolder(getCoreData());

      for (int j = 0; j < util.getModuleCount(); j++) {
        if (!util.getModule(j).name.equalsIgnoreCase("CORE")) {

          for (String modDir : ModulesUtil.moduleDirs) {
            File modDirFile = new File(rootDirFile, modDir);
            getLog().debug("Looking for module source data '" + util.getModule(j).dir + "' in:" + modDirFile.getAbsolutePath());
            File dirF = new File(modDirFile,util.getModule(j).dir + "/src-db/database/sourcedata/");
            if (dirF.exists()) {
              getLog().debug("Module dir found: " + util.getModule(j).dir);
              dataFiles.addAll(DBSMOBUtil.loadFilesFromFolder(dirF.getAbsolutePath()));
              break;
            }
          }
        }
      }

      final DatabaseData databaseOrgData = new DatabaseData(xmlModel);
      for (int i = 0; i < dataFiles.size(); i++) {
        // getLog().info("Loading data for module. Path:
        // "+dataFiles.get(i).getAbsolutePath());
        try {
          dataReader.getSink().start();
          final String tablename = dataFiles.get(i)
              .getName()
              .substring(0, dataFiles.get(i).getName().length() - 4);
          final Vector<DynaBean> vectorDynaBeans = ((DataToArraySink) dataReader.getSink())
              .getVector();
          dataReader.parse(dataFiles.get(i));
          databaseOrgData.insertDynaBeansFromVector(tablename, vectorDynaBeans);
          dataReader.getSink().end();
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }

      getLog().info("Loading complete model from current database");
      final Database currentdb = platform.loadModelFromDatabase(excludeFilter);

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
          if (databaseModel == null) {
            databaseModel = dbI;
          } else {
            databaseModel.mergeWith(dbI);
          }
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }

      List<String> configScripts = DBSMOBUtil.getInstance().getSortedTemplates(databaseOrgData);
      getLog().info("Loading and applying configuration scripts");
      DatabaseIO dbIOs = new DatabaseIO();
      for (String configScript : configScripts) {
        File f = null;
        for (String modDir : ModulesUtil.moduleDirs) {
          File modDirFile = new File(rootDirFile, modDir);
          getLog().debug("Looking for module config script '"+configScript+"' in:" + modDirFile.getAbsolutePath());
          f = new File(modDirFile,configScript + "/src-db/database/configScript.xml");
          if (f.exists()) {
            break;
          }
        }

        if (configScript.equals(industryTemplate) || f == null || !f.exists()
            || !DBSMOBUtil.isApplied(platform, configScript)) {
          continue;
        }

        getLog().info("Loading configuration script: " + f.getAbsolutePath());
        Vector<Change> changes = dbIOs.readChanges(f);
        for (Change change : changes) {
          if (change instanceof ModelChange) {
            ((ModelChange) change).apply(xmlModel, platform.isDelimitedIdentifierModeOn());
          } else if (change instanceof DataChange) {
            ((DataChange) change).apply(databaseOrgData, platform.isDelimitedIdentifierModeOn());
          }
          getLog().debug(change);
        }
      }

      getLog().info("Comparing models...");
      final Vector<String> modIds = new Vector<String>();
      for (int i = 0; i < util.getModuleCount(); i++) {
        final String mod = util.getModule(i).idMod;
        modIds.add(mod);
      }

      OBDataset ad = new OBDataset(platform, currentdb, "AD");
      final DataComparator dataComparator = new DataComparator(
          platform.getSqlBuilder().getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
      dataComparator.compare(xmlModel, databaseModel, platform, databaseOrgData, ad, null);
      Vector<Change> finalChanges = new Vector<Change>();
      Vector<Change> notExportedChanges = new Vector<Change>();
      String obVersion = DBSMOBUtil.getInstance().getOBVersion(platform);
      if (obVersion != null) {
        VersionInfo version = new VersionInfo();
        version.setVersion(obVersion);
        finalChanges.add(version);

      }
      dataComparator.generateConfigScript(finalChanges, notExportedChanges);

      final DatabaseIO dbIO = new DatabaseIO();

      File baseDir = null;

      for (String modDir : ModulesUtil.moduleDirs) {
        File modDirFile = new File(rootDirFile, modDir);
        getLog().debug("Looking for module template '"+industryTemplate+"' in:" + modDirFile.getAbsolutePath());
        baseDir = new File(modDirFile, industryTemplate);
        if (baseDir.exists()) {
          break;
        }
      }

      if (baseDir == null || !baseDir.exists()) {
        throw new IllegalArgumentException("The module template in development '"+industryTemplate+"' does not exists.\n" +
                "Searched in the following locations: " + Arrays.toString(ModulesUtil.moduleDirs));
      }

      final File configFile = new File(baseDir,
          "/src-db/database/configScript.xml");
      final File folder = new File(configFile.getParent());

      folder.mkdirs();

      File formalChangesf = new File("src-db/database/formalChangesScript.xml");
      if (formalChangesf.exists()) {
        getLog().info("Loading script of formal changes");
        Vector<Change> formalChanges = dbIOs.readChanges(formalChangesf);
        for (Change change : formalChanges) {
          ColumnDataChange change2 = (ColumnDataChange) change;
          ColumnDataChange changeToRemove = null;
          for (Change changeS : finalChanges) {
            if (changeS instanceof ColumnDataChange
                && ((ColumnDataChange) changeS).equals(change2)) {
              changeToRemove = (ColumnDataChange) changeS;
            }
          }
          if (changeToRemove != null) {
            finalChanges.remove(changeToRemove);
          }
        }
      }
      dbIO.write(configFile, finalChanges);
      for (Change c : finalChanges) {
        if (c instanceof RemoveTriggerChange) {
          log.info("The trigger " + ((RemoveTriggerChange) c).getTriggerName()
              + " has not been found in the database, and therefore a RemoveTriggerChange has been exported to the configuration script.");
        } else if (c instanceof ModelChange) {
          log.info(c);
        } else {
          log.debug(c);
        }
      }

      if (notExportedChanges.size() > 0) {
        getLog().info("Changes that couldn't be exported to the config script:");
        getLog().info("*******************************************************");
      }
      for (final Change c : notExportedChanges) {
        getLog().info(c);
      }
      DBSMOBUtil.getInstance().updateCRC();
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

  /**
   * Functionality for deleting data during create.database was removed. Function is kept to not
   * require lock-step update of dbsm.jar & build-create.xml
   */
  @Deprecated
  public void setFilter(String filter) {
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
    this.modulecoredir = new File(moduledir, "../modules_core");
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
