package org.openbravo.ddlutils.task;

import java.io.File;
import java.io.StringWriter;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DataToArraySink;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.task.VerbosityLevel;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.tools.ant.Task;

public class CompareData extends Task {

    private String driver;
    private String url;
    private String user;
    private String password;
    private File model;
    private String data;
    private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

    private File prescript = null;
    private File postscript = null;

    private String filter = "org.apache.ddlutils.io.NoneDatabaseFilter";

    private File originalmodel;
    private boolean failonerror = false;

    private String object = null;

    protected Log _log;
    private VerbosityLevel _verbosity = null;

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

        _log.info("Loading model from XML files");
        Database originaldb = DatabaseUtils.readDatabase(getModel());

        DatabaseDataIO dbdio = new DatabaseDataIO();
        dbdio.setEnsureFKOrder(false);
        dbdio.setDatabaseFilter(DatabaseUtils.getDynamicDatabaseFilter(
                getFilter(), originaldb));

        DataReader dataReader = null;
        dataReader = dbdio.getConfiguredCompareDataReader(originaldb);

        String folders = getData();

        StringTokenizer strTokFol = new StringTokenizer(folders, ",");

        Vector<File> files = new Vector<File>();

        while (strTokFol.hasMoreElements()) {
            String folder = strTokFol.nextToken();
            File[] fileArray = DatabaseUtils.readFileArray(new File(folder));
            for (int i = 0; i < fileArray.length; i++) {
                files.add(fileArray[i]);
            }
        }
        _log.info("Loading data from XML files");
        DatabaseData databaseData = new DatabaseData(originaldb);
        for (int i = 0; i < files.size(); i++) {
            try {
                dataReader.getSink().start();
                String tablename = files.get(i).getName().substring(0,
                        files.get(i).getName().length() - 4);
                Vector<DynaBean> vectorDynaBeans = ((DataToArraySink) dataReader
                        .getSink()).getVector();
                dataReader.parse(files.get(i));
                databaseData.insertDynaBeansFromVector(tablename,
                        vectorDynaBeans);
                dataReader.getSink().end();
            } catch (Exception e) {
                System.out.println(e.getLocalizedMessage());
            }
        }

        _log.info("Loading model from current database");
        Database currentdb = platform.loadModelFromDatabase(DatabaseUtils
                .getExcludeFilter(excludeobjects));

        DataComparator dataComparator = new DataComparator(platform
                .getSqlBuilder().getPlatformInfo(), platform
                .isDelimitedIdentifierModeOn());
        dataComparator.setFilter(DatabaseUtils.getDynamicDatabaseFilter(
                getFilter(), originaldb));
        dataComparator.compare(originaldb, currentdb, platform, databaseData);

        Vector<Change> changes = dataComparator.getChanges();
        for (int i = 0; i < changes.size(); i++)
            System.out.println(changes.get(i));
        System.out.println("=======================");
        try {
            StringWriter stringWriter = new StringWriter();
            platform.getSqlBuilder().setWriter(stringWriter);
            platform.getSqlBuilder().processDataChanges(currentdb,
                    databaseData, changes);
            System.out.println(stringWriter.toString());
        } catch (Exception e) {
            System.out.println("oops: " + e.getMessage());
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

    public File getOriginalmodel() {
        return originalmodel;
    }

    public void setOriginalmodel(File input) {
        this.originalmodel = input;
    }

    public File getModel() {
        return model;
    }

    public void setModel(File model) {
        this.model = model;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public boolean isFailonerror() {
        return failonerror;
    }

    public void setFailonerror(boolean failonerror) {
        this.failonerror = failonerror;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String getFilter() {
        return filter;
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
        if (object == null || object.trim().startsWith("$")
                || object.trim().equals("")) {
            this.object = null;
        } else {
            this.object = object;
        }
    }

    public String getObject() {
        return object;
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

}