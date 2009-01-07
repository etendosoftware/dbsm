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
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.tools.ant.BuildException;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.ModuleRow;

/**
 * 
 * @author adrian
 */
public class ExportDatabase extends BaseDatabaseTask {

    private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";
    private String codeRevision;

    private File model;
    private File moduledir;
    private String module;

    /** Creates a new instance of ExportDatabase */
    public ExportDatabase() {
    }

    @Override
    public void doExecute() {
        getLog().info(
                "Database connection: " + getUrl() + ". User: " + getUser());

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

        if (module == null || module.equals("%")) {
            final Database db = platform.loadModelFromDatabase(DatabaseUtils
                    .getExcludeFilter(excludeobjects));

            for (int i = 0; i < util.getActiveModuleCount(); i++) {
                getLog().info(
                        "Exporting module: " + util.getActiveModule(i).name);
                Database dbI = null;
                try {
                    dbI = (Database) db.clone();
                } catch (final Exception e) {
                    System.out.println("Error while cloning the database model"
                            + e.getMessage());
                    return;
                }
                dbI.applyNamingConventionFilter(util.getActiveModule(i).filter);
                getLog().info(db.toString());
                final DatabaseIO io = new DatabaseIO();
                if (util.getActiveModule(i).name.equalsIgnoreCase("CORE")) {
                    getLog().info("Path: " + model.getAbsolutePath());
                    io.writeToDir(dbI, model);
                } else {
                    final File path = new File(moduledir, util
                            .getActiveModule(i).dir
                            + "/src-db/database/model/");
                    getLog().info("Path: " + path);
                    io.writeToDir(dbI, path);
                }
            }
        } else {
            if (module != null && !module.equals("%"))
                util.getIncDependenciesForModuleList(module);

            for (int i = 0; i < util.getActiveModuleCount(); i++) {
                final ModuleRow row = util.getActiveModule(i);
                if (util.isIncludedInExportList(row)) {
                    getLog().info("Exporting module: " + row.name);
                    if (row == null)
                        throw new BuildException(
                                "Module not found in AD_MODULE table.");
                    if (row.prefixes.size() == 0) {
                        getLog()
                                .info(
                                        "Module doesn't have dbprefix. We will not export structure for it.");
                        return;
                    }
                    if (row.isInDevelopment != null
                            && row.isInDevelopment.equalsIgnoreCase("Y")) {
                        getLog().info("Loading submodel from database...");
                        final Database db = platform.loadModelFromDatabase(
                                row.filter, row.prefixes.get(0), false,
                                row.idMod);
                        getLog().info("Submodel loaded");
                        final DatabaseIO io = new DatabaseIO();
                        final File path = new File(moduledir, row.dir
                                + "/src-db/database/model/");
                        getLog().info("Path: " + path);
                        io.writeToDir(db, path);
                    } else {
                        getLog()
                                .info(
                                        "Module is not in development. Check that it is, before trying to export it.");
                    }
                }
            }
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
}
