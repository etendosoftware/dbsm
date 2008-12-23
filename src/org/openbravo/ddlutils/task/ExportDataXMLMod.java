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
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.task.VerbosityLevel;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.tools.ant.BuildException;
import org.openbravo.model.ad.utility.DataSetTable;
import org.openbravo.model.ad.utility.DataSet;
import org.openbravo.dal.core.DalInitializingTask;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.service.db.DataSetService;

/**
 * 
 * @author adrian
 */
public class ExportDataXMLMod extends DalInitializingTask {

    private String driver;
    private String url;
    private String user;
    private String password;
    private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

    private File prescript = null;
    private File postscript = null;

    private File model = null;
    private File moduledir;
    private String filter = "org.apache.ddlutils.io.AllDatabaseFilter";
    private String module;

    private File output;
    private String encoding = "UTF-8";

    protected Log _log;
    private VerbosityLevel _verbosity = null;
    private String codeRevision;

    /** Creates a new instance of WriteDataXML */
    public ExportDataXMLMod() {
    }

    /**
     * Initializes the logging.
     */
    private void initLogging() {
        // For Ant, we're forcing DdlUtils to do logging via log4j to the
        // console
        Properties props = new Properties();
        String level = (_verbosity == null ? Level.INFO.toString() : _verbosity
                .getValue()).toUpperCase();

        props.setProperty("log4j.rootCategory", level + ",A");
        props.setProperty("log4j.appender.A",
                "org.apache.log4j.ConsoleAppender");
        props.setProperty("log4j.appender.A.layout",
                "org.apache.log4j.PatternLayout");
        props.setProperty("log4j.appender.A.layout.ConversionPattern", "%m%n");
        // we don't want debug logging from Digester/Betwixt
        props.setProperty("log4j.logger.org.apache.commons", "WARN");

        LogManager.resetConfiguration();
        PropertyConfigurator.configure(props);

        _log = LogFactory.getLog(getClass());
    }

    @Override
    public void doExecute() {

        initLogging();

        DataSetService datasetService = DataSetService.getInstance();

        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(getDriver());
        ds.setUrl(getUrl());
        ds.setUsername(getUser());
        ds.setPassword(getPassword());

        Platform platform = PlatformFactory.createNewPlatformInstance(ds);
        // platform.setDelimitedIdentifierModeOn(true);
        // DBSMOBUtil.verifyRevision(platform, codeRevision, _log);

        DBSMOBUtil util = DBSMOBUtil.getInstance();
        util.getModules(platform, excludeobjects);
        if (util.getActiveModuleCount() == 0) {
            _log
                    .info("No active modules. For a module to be exported, it needs to be set as 'InDevelopment'");
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
            _log.info("Loading models for AD and RD Datasets");
            Database originaldb;
            originaldb = platform.loadModelFromDatabase(DatabaseUtils
                    .getExcludeFilter(excludeobjects), "AD");
            Database dbrd = platform.loadModelFromDatabase(DatabaseUtils
                    .getExcludeFilter(excludeobjects), "ADRD");
            originaldb.mergeWith(dbrd);
            _log.info("Model loaded");

            // Create a set of files one for each table
            Vector<String> datasets = new Vector<String>();
            datasets.add("AD");
            datasets.add("ADRD");

            int datasetI = 0;

            for (String dataSetCode : datasets) {
                DataSet dataSet = datasetService.getDataSetByValue(dataSetCode);
                System.out.println(dataSet);
                List<DataSetTable> tableList = datasetService
                        .getDataSetTables(dataSet);
                for (int i = 0; i < util.getActiveModuleCount(); i++) {
                    if (module == null
                            || module.equals("%")
                            || util.isIncludedInExportList(util
                                    .getActiveModule(i))) {
                        _log.info("Exporting module: "
                                + util.getActiveModule(i).name);
                        _log.info(originaldb.toString());
                        DatabaseDataIO dbdio = new DatabaseDataIO();
                        dbdio.setEnsureFKOrder(false);
                        if (util.getActiveModule(i).name
                                .equalsIgnoreCase("CORE")) {
                            _log.info("Path: " + output.getAbsolutePath());
                            // First we delete all .xml files in the directory

                            if (datasetI == 0) {
                                File[] filestodelete = DatabaseIO
                                        .readFileArray(getOutput());
                                for (File filedelete : filestodelete) {
                                    filedelete.delete();
                                }
                            }

                            for (DataSetTable table : tableList) {
                                _log.info("Exporting table: "
                                        + table.getTable().getTableName()
                                        + " to Core");
                                OutputStream out = new FileOutputStream(
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
                                File path = new File(moduledir, util
                                        .getActiveModule(i).dir
                                        + "/src-db/database/sourcedata/");
                                _log.info("Path: " + path);
                                path.mkdirs();
                                if (datasetI == 0) {
                                    File[] filestodelete = DatabaseIO
                                            .readFileArray(path);
                                    for (File filedelete : filestodelete) {
                                        filedelete.delete();
                                    }
                                }
                                for (DataSetTable table : tableList) {
                                    _log.info("Exporting table: "
                                            + table.getTable().getTableName()
                                            + " to module "
                                            + util.getActiveModule(i).name);
                                    File tableFile = new File(path, table
                                            .getTable().getTableName()
                                            .toUpperCase()
                                            + ".xml");
                                    OutputStream out = new FileOutputStream(
                                            tableFile);
                                    boolean b = dbdio.writeDataForTableToXML(
                                            originaldb, datasetService,
                                            dataSetCode, table, out,
                                            getEncoding(), util
                                                    .getActiveModule(i).idMod);
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
        } catch (Exception e) {
            e.printStackTrace();
            // log(e.getLocalizedMessage());
            throw new BuildException(e);
        }
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
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

    /**
     * Specifies the verbosity of the task's debug output.
     * 
     * @param level
     *            The verbosity level
     * @ant.not-required Default is <code>INFO</code>.
     */
    public void setVerbosity(VerbosityLevel level) {
        _verbosity = level;
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
