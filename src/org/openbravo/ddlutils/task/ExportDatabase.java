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
import java.util.List;
import java.util.Vector;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.tools.ant.BuildException;
import org.openbravo.base.exception.OBException;
import org.openbravo.dal.service.OBDal;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.ModuleRow;
import org.openbravo.model.ad.module.Module;
import org.openbravo.model.ad.utility.DataSet;
import org.openbravo.model.ad.utility.DataSetTable;
import org.openbravo.service.dataset.DataSetService;
import org.openbravo.service.system.SystemService;
import org.openbravo.service.system.SystemValidationResult;

/**
 * 
 * @author adrian
 */
public class ExportDatabase extends BaseDalInitializingTask {

  private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";
  private String codeRevision;

  private File model;
  private File moduledir;
  private String module;
  private File output;
  private String encoding = "UTF-8";

  /** Creates a new instance of ExportDatabase */
  public ExportDatabase() {
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

      final Platform platform = PlatformFactory.createNewPlatformInstance(ds);
      // platform.setDelimitedIdentifierModeOn(true);

      DBSMOBUtil.verifyRevision(platform, getCodeRevision(), getLog());

      final DBSMOBUtil util = DBSMOBUtil.getInstance();
      util.getModules(platform, excludeobjects);
      if (util.getActiveModuleCount() == 0) {
        getLog()
            .info(
                "No active modules. For a module to be exported, it needs to be set as 'InDevelopment'");
        return;
      }
      Database db;
      if (module == null || module.equals("%")) {
        db = platform.loadModelFromDatabase(DatabaseUtils.getExcludeFilter(excludeobjects));

        for (int i = 0; i < util.getActiveModuleCount(); i++) {
          getLog().info("Exporting module: " + util.getActiveModule(i).name);
          Database dbI = null;
          try {
            dbI = (Database) db.clone();
          } catch (final Exception e) {
            System.out.println("Error while cloning the database model" + e.getMessage());
            return;
          }
          dbI.applyNamingConventionFilter(util.getActiveModule(i).filter);
          getLog().info(db.toString());
          final DatabaseIO io = new DatabaseIO();

          validateModule(util.getActiveModule(i).idMod, dbI);

          if (util.getActiveModule(i).name.equalsIgnoreCase("CORE")) {
            getLog().info("Path: " + model.getAbsolutePath());
            io.writeToDir(dbI, model);
          } else {
            final File path = new File(moduledir, util.getActiveModule(i).dir
                + "/src-db/database/model/");
            getLog().info("Path: " + path);
            io.writeToDir(dbI, path);
          }
        }
      } else {
        getLog().info("Loading models for AD and RD Datasets");
        db = platform.loadModelFromDatabase(DatabaseUtils.getExcludeFilter(excludeobjects), "AD");
        final Database dbrd = platform.loadModelFromDatabase(DatabaseUtils
            .getExcludeFilter(excludeobjects), "ADRD");
        db.mergeWith(dbrd);

        if (module != null && !module.equals("%"))
          util.getIncDependenciesForModuleList(module);

        for (int i = 0; i < util.getActiveModuleCount(); i++) {
          final ModuleRow row = util.getActiveModule(i);
          if (util.isIncludedInExportList(row)) {
            if (row == null)
              throw new BuildException("Module not found in AD_MODULE table.");
            if (row.prefixes.size() == 0) {
              getLog().info("Module doesn't have dbprefix. We will not export structure for it.");
              return;
            }
            getLog().info("Exporting module: " + row.name);
            if (row.isInDevelopment != null && row.isInDevelopment.equalsIgnoreCase("Y")) {
              getLog().info("Loading submodel from database...");
              db = platform
                  .loadModelFromDatabase(row.filter, row.prefixes.get(0), false, row.idMod);

              validateModule(row.idMod, db);

              getLog().info("Submodel loaded");
              final DatabaseIO io = new DatabaseIO();
              final File path = new File(moduledir, row.dir + "/src-db/database/model/");
              getLog().info("Path: " + path);
              io.writeToDir(db, path);
            } else {
              getLog().info(
                  "Module is not in development. Check that it is, before trying to export it.");
            }
          }
        }
      }

      final Vector<String> datasets = new Vector<String>();
      datasets.add("AD");
      datasets.add("ADRD");

      final DataSetService datasetService = DataSetService.getInstance();
      int datasetI = 0;

      for (final String dataSetCode : datasets) {
        final DataSet dataSet = datasetService.getDataSetByValue(dataSetCode);
        System.out.println(dataSet);
        final List<DataSetTable> tableList = datasetService.getDataSetTables(dataSet);
        for (int i = 0; i < util.getActiveModuleCount(); i++) {
          if (module == null || module.equals("%")
              || util.isIncludedInExportList(util.getActiveModule(i))) {
            getLog().info("Exporting module: " + util.getActiveModule(i).name);
            getLog().info(db.toString());
            final DatabaseDataIO dbdio = new DatabaseDataIO();
            dbdio.setEnsureFKOrder(false);
            if (util.getActiveModule(i).name.equalsIgnoreCase("CORE")) {
              getLog().info("Path: " + output.getAbsolutePath());
              // First we delete all .xml files in the directory

              if (datasetI == 0) {
                final File[] filestodelete = DatabaseIO.readFileArray(getOutput());
                for (final File filedelete : filestodelete) {
                  filedelete.delete();
                }
              }

              for (final DataSetTable table : tableList) {
                getLog().info("Exporting table: " + table.getTable().getTableName() + " to Core");
                final OutputStream out = new FileOutputStream(new File(getOutput(), table
                    .getTable().getTableName().toUpperCase()
                    + ".xml"));
                dbdio.writeDataForTableToXML(db, datasetService, dataSetCode, table, out,
                    getEncoding(), util.getActiveModule(i).idMod);
                out.flush();
              }
            } else {
              if (dataSetCode.equals("AD")) {
                final File path = new File(moduledir, util.getActiveModule(i).dir
                    + "/src-db/database/sourcedata/");
                getLog().info("Path: " + path);
                path.mkdirs();
                if (datasetI == 0) {
                  final File[] filestodelete = DatabaseIO.readFileArray(path);
                  for (final File filedelete : filestodelete) {
                    filedelete.delete();
                  }
                }
                for (final DataSetTable table : tableList) {
                  getLog().info(
                      "Exporting table: " + table.getTable().getTableName() + " to module "
                          + util.getActiveModule(i).name);
                  final File tableFile = new File(path, table.getTable().getTableName()
                      .toUpperCase()
                      + ".xml");
                  final OutputStream out = new FileOutputStream(tableFile);
                  final boolean b = dbdio.writeDataForTableToXML(db, datasetService, dataSetCode,
                      table, out, getEncoding(), util.getActiveModule(i).idMod);
                  if (!b)
                    tableFile.delete();
                  out.flush();
                }
              }
            }
          }
        }
        datasetI++;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void validateModule(String moduleId, Database dbI) {
    getLog().info("Validating Module...");
    final Module moduleToValidate = OBDal.getInstance().get(Module.class, moduleId);
    final SystemValidationResult result = SystemService.getInstance().validateModule(
        moduleToValidate, dbI);
    SystemService.getInstance().logValidationResult(log, result);
    if (result.getErrors().size() > 0) {
      throw new OBException(
          "Module validation failed, see the above list of errors for more information");
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

  public String getModule() {
    return module;
  }

  public void setModule(String module) {
    this.module = module;
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
}
