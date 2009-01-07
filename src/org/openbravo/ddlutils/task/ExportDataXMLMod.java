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
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.model.ad.utility.DataSet;
import org.openbravo.model.ad.utility.DataSetTable;
import org.openbravo.service.db.DataSetService;

/**
 * 
 * @author adrian
 */
public class ExportDataXMLMod extends BaseDalInitializingTask {

    private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

    private File prescript = null;
    private File postscript = null;

    private File model = null;
    private File moduledir;
    private String filter = "org.apache.ddlutils.io.AllDatabaseFilter";
    private String module;

    private File output;
    private String encoding = "UTF-8";

    private String codeRevision;

    /** Creates a new instance of WriteDataXML */
    public ExportDataXMLMod() {
    }

    @Override
    public void doExecute() {

        final DataSetService datasetService = DataSetService.getInstance();

        final BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(getDriver());
        ds.setUrl(getUrl());
        ds.setUsername(getUser());
        ds.setPassword(getPassword());

        final Platform platform = PlatformFactory.createNewPlatformInstance(ds);
        // platform.setDelimitedIdentifierModeOn(true);
        // DBSMOBUtil.verifyRevision(platform, codeRevision, getLog());

        final DBSMOBUtil util = DBSMOBUtil.getInstance();
        util.getModules(platform, excludeobjects);
        if (util.getActiveModuleCount() == 0) {
            getLog()
                    .info(
                            "No active modules. For a module to be exported, it needs to be set as 'InDevelopment'");
            return;
        }
        util.generateIndustryTemplateTree();
        if (module != null && !module.equals("%"))
            util.getIncDependenciesForModuleList(module);

        try {
            // execute the pre-script
            if (getPrescript() != null) {
                platform.evaluateBatch(DatabaseUtils.readFile(getPrescript()),
                        true);
            }
            getLog().info("Loading models for AD and RD Datasets");
            Database originaldb;
            originaldb = platform.loadModelFromDatabase(DatabaseUtils
                    .getExcludeFilter(excludeobjects), "AD");
            final Database dbrd = platform.loadModelFromDatabase(DatabaseUtils
                    .getExcludeFilter(excludeobjects), "ADRD");
            originaldb.mergeWith(dbrd);
            getLog().info("Model loaded");

            // Create a set of files one for each table
            final Vector<String> datasets = new Vector<String>();
            datasets.add("AD");
            datasets.add("ADRD");

            int datasetI = 0;

            for (final String dataSetCode : datasets) {
                final DataSet dataSet = datasetService
                        .getDataSetByValue(dataSetCode);
                System.out.println(dataSet);
                final List<DataSetTable> tableList = datasetService
                        .getDataSetTables(dataSet);
                for (int i = 0; i < util.getActiveModuleCount(); i++) {
                    if (module == null
                            || module.equals("%")
                            || util.isIncludedInExportList(util
                                    .getActiveModule(i))) {
                        getLog().info(
                                "Exporting module: "
                                        + util.getActiveModule(i).name);
                        getLog().info(originaldb.toString());
                        final DatabaseDataIO dbdio = new DatabaseDataIO();
                        dbdio.setEnsureFKOrder(false);
                        if (util.getActiveModule(i).name
                                .equalsIgnoreCase("CORE")) {
                            getLog().info("Path: " + output.getAbsolutePath());
                            // First we delete all .xml files in the directory

                            if (datasetI == 0) {
                                final File[] filestodelete = DatabaseIO
                                        .readFileArray(getOutput());
                                for (final File filedelete : filestodelete) {
                                    filedelete.delete();
                                }
                            }

                            for (final DataSetTable table : tableList) {
                                getLog().info(
                                        "Exporting table: "
                                                + table.getTable()
                                                        .getTableName()
                                                + " to Core");
                                final OutputStream out = new FileOutputStream(
                                        new File(getOutput(), table.getTable()
                                                .getTableName().toUpperCase()
                                                + ".xml"));
                                dbdio.writeDataForTableToXML(originaldb,
                                        datasetService, dataSetCode, table,
                                        out, getEncoding(), util
                                                .getActiveModule(i).idMod);
                                out.flush();
                            }
                        } else {
                            if (dataSetCode.equals("AD")) {
                                final File path = new File(moduledir, util
                                        .getActiveModule(i).dir
                                        + "/src-db/database/sourcedata/");
                                getLog().info("Path: " + path);
                                path.mkdirs();
                                if (datasetI == 0) {
                                    final File[] filestodelete = DatabaseIO
                                            .readFileArray(path);
                                    for (final File filedelete : filestodelete) {
                                        filedelete.delete();
                                    }
                                }
                                for (final DataSetTable table : tableList) {
                                    getLog()
                                            .info(
                                                    "Exporting table: "
                                                            + table
                                                                    .getTable()
                                                                    .getTableName()
                                                            + " to module "
                                                            + util
                                                                    .getActiveModule(i).name);
                                    final File tableFile = new File(path, table
                                            .getTable().getTableName()
                                            .toUpperCase()
                                            + ".xml");
                                    final OutputStream out = new FileOutputStream(
                                            tableFile);
                                    final boolean b = dbdio
                                            .writeDataForTableToXML(
                                                    originaldb,
                                                    datasetService,
                                                    dataSetCode,
                                                    table,
                                                    out,
                                                    getEncoding(),
                                                    util.getActiveModule(i).idMod);
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

            // execute the post-script
            if (getPostscript() != null) {
                platform.evaluateBatch(DatabaseUtils.readFile(getPostscript()),
                        true);
            }
        } catch (final Exception e) {
            e.printStackTrace();
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
