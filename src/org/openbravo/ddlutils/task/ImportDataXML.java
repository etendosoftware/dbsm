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
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.task.VerbosityLevel;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Task;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.ModuleRow;

/**
 * 
 * @author adrian
 */
public class ImportDataXML extends Task {

    private String driver;
    private String url;
    private String user;
    private String password;
    private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

    private File prescript = null;
    private File postscript = null;

    private File model = null;
    private String filter = "org.apache.ddlutils.io.NoneDatabaseFilter";

    private String basedir;
    private String input;
    private String encoding = "UTF-8";

    protected Log _log;
    private VerbosityLevel _verbosity = null;

    /** Creates a new instance of ReadDataXML */
    public ImportDataXML() {
        super();
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
    public void execute() {

        initLogging();

        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(getDriver());
        ds.setUrl(getUrl());
        ds.setUsername(getUser());
        ds.setPassword(getPassword());

        Platform platform = PlatformFactory.createNewPlatformInstance(ds);
        // platform.setDelimitedIdentifierModeOn(true);

        String filters = getFilter();
        StringTokenizer strTokFil = new StringTokenizer(filters, ",");
        String folders = getInput();
        StringTokenizer strTokFol = new StringTokenizer(folders, ",");

        Vector<File> files = new Vector<File>();

        while (strTokFol.hasMoreElements()) {
            if (basedir == null) {
                _log
                        .info("Basedir not specified, will insert just Core data files.");
                String folder = strTokFol.nextToken();
                File[] fileArray = DatabaseUtils
                        .readFileArray(new File(folder));
                for (int i = 0; i < fileArray.length; i++)
                    files.add(fileArray[i]);
            } else {
                String token = strTokFol.nextToken();
                DirectoryScanner dirScanner = new DirectoryScanner();
                dirScanner.setBasedir(new File(basedir));
                String[] dirFilterA = { token };
                dirScanner.setIncludes(dirFilterA);
                dirScanner.scan();
                String[] incDirs = dirScanner.getIncludedDirectories();
                for (int j = 0; j < incDirs.length; j++) {
                    File dirFolder = new File(basedir, incDirs[j] + "/");
                    File[] fileArray = DatabaseUtils.readFileArray(dirFolder);
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
                File fpre = new File(getInput(), "prescript-"
                        + platform.getName() + ".sql");
                if (fpre.exists()) {
                    _log.info("Executing default prescript");
                    platform.evaluateBatch(DatabaseUtils.readFile(fpre), true);
                }
            } else {
                platform.evaluateBatch(DatabaseUtils.readFile(getPrescript()),
                        true);
            }

            Database originaldb;
            if (getModel() == null) {
                originaldb = platform.loadModelFromDatabase(DatabaseUtils
                        .getExcludeFilter(excludeobjects));
                if (originaldb == null) {
                    originaldb = new Database();
                    _log.info("Model considered empty.");
                } else {
                    _log.info("Model loaded from database.");
                }
            } else {
                // Load the model from the file
                originaldb = DatabaseUtils.readDatabase(getModel());
                _log.info("Model loaded from file.");
            }
            DatabaseDataIO dbdio = new DatabaseDataIO();
            dbdio.setEnsureFKOrder(false);
            DataReader dataReader = null;
            while (strTokFil.hasMoreElements()) {
                String filter = strTokFil.nextToken();
                if (filter != null && !filter.equals("")) {
                    dbdio.setDatabaseFilter(DatabaseUtils
                            .getDynamicDatabaseFilter(filter, originaldb));
                    dataReader = dbdio.getConfiguredDataReader(platform,
                            originaldb);
                    dataReader.getSink().start(); // we do this to delete data
                                                  // from tables in each of the
                                                  // filters
                }
            }
            for (int i = 0; i < files.size(); i++) {
                _log.debug("Importing data from file: "
                        + files.get(i).getName());
                dbdio.writeDataToDatabase(dataReader, files.get(i));
            }

            dataReader.getSink().end();

            // execute the post-script
            if (getPostscript() == null) {
                // try to execute the default prescript
                File fpost = new File(getInput(), "postscript-"
                        + platform.getName() + ".sql");
                if (fpost.exists()) {
                    _log.info("Executing default postscript");
                    platform.evaluateBatch(DatabaseUtils.readFile(fpost), true);
                }
            } else {
                platform.evaluateBatch(DatabaseUtils.readFile(getPostscript()),
                        true);
            }

            DBSMOBUtil util = DBSMOBUtil.getInstance();
            util.getModules(platform,
                    "org.apache.ddlutils.platform.ExcludeFilter");
            util.generateIndustryTemplateTree();
            for (int i = 0; i < util.getIndustryTemplateCount(); i++) {
                ModuleRow temp = util.getIndustryTemplateId(i);
                File f = new File(basedir, "modules/" + temp.dir
                        + "/src-db/database/configScript.xml");
                _log.info("Loading config script for module " + temp.name
                        + ". Path: " + f.getAbsolutePath());
                if (f.exists()) {
                    DatabaseIO dbIO = new DatabaseIO();
                    Vector<Change> changesConfigScript = dbIO.readChanges(f);
                    platform.applyConfigScript(originaldb, changesConfigScript);
                } else {
                    _log
                            .error("Error. We couldn't find configuration script for template "
                                    + temp.name
                                    + ". Path: "
                                    + f.getAbsolutePath());
                }
            }

        } catch (Exception e) {
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

    public String getBasedir() {
        return basedir;
    }

    public void setBasedir(String basedir) {
        this.basedir = basedir;
    }

}
