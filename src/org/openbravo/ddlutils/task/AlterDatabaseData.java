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
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.model.Database;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.task.VerbosityLevel;

/**
 *
 * @author adrian
 */
public class AlterDatabaseData extends Task {
    
    private String driver;
    private String url;
    private String user;
    private String password;
    private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";
    
    private File prescript = null;
    private File postscript = null;
    
    private String filter = "org.apache.ddlutils.io.NoneDatabaseFilter";
    
    private File input;   
    private String encoding = "UTF-8";
    private File originalmodel;   
    private File model;
    private boolean failonerror = false;
    
    private String object = null;

    protected Log _log;
    private VerbosityLevel _verbosity = null;
    
    /** Creates a new instance of ReadDataXML */
    public AlterDatabaseData() {
        super();
    }
    
    /**
     * Initializes the logging.
     */
    private void initLogging() {
        // For Ant, we're forcing DdlUtils to do logging via log4j to the console
        Properties props = new Properties();
        String     level = (_verbosity == null ? Level.INFO.toString() : _verbosity.getValue()).toUpperCase();

        props.setProperty("log4j.rootCategory", level + ",A");
        props.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");
        props.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
        props.setProperty("log4j.appender.A.layout.ConversionPattern", "%m%n");
        // we don't want debug logging from Digester/Betwixt
        props.setProperty("log4j.logger.org.apache.commons", "WARN");

        LogManager.resetConfiguration();
        PropertyConfigurator.configure(props);

        _log = LogFactory.getLog(getClass());
    }
    
    public void execute() {
       
        initLogging();
    
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(getDriver());
        ds.setUrl(getUrl());
        ds.setUsername(getUser());
        ds.setPassword(getPassword());    
        
        Platform platform = PlatformFactory.createNewPlatformInstance(ds);
        // platform.setDelimitedIdentifierModeOn(true);
        
        try {      
            // execute the pre-script
            if (getPrescript() == null) {
                // try to execute the default prescript
                File fpre = new File(getModel(), "prescript-" + platform.getName() + ".sql");
                if (fpre.exists()) {
                    _log.info("Executing default prescript");
                    platform.evaluateBatch(DatabaseUtils.readFile(fpre), true);
                }
            } else {
                platform.evaluateBatch(DatabaseUtils.readFile(getPrescript()), true);
            }
                        

            Database originaldb;
            if (getOriginalmodel() == null) {
                originaldb = platform.loadModelFromDatabase(DatabaseUtils.getExcludeFilter(excludeobjects)); 
                if (originaldb == null) {
                    originaldb =  new Database();
                    _log.info("Original model considered empty.");
                } else {
                    _log.info("Original model loaded from database.");
                }             
            } else {
                // Load the model from the file
                originaldb = DatabaseUtils.readDatabase(getModel());
                _log.info("Original model loaded from file.");
            }   

            Database db = DatabaseUtils.readDatabase(getModel());   

            Database oldModel=(Database)originaldb.clone();
            platform.alterTables(originaldb, db, !isFailonerror()); 
            
            DatabaseDataIO dbdio = new DatabaseDataIO();
            dbdio.setEnsureFKOrder(false);
            dbdio.setDatabaseFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), originaldb));
            dbdio.writeDataToDatabase(platform, db, DatabaseUtils.readFileArray(getInput())); 
            
            platform.alterTablesPostScript(oldModel, db, !isFailonerror());
            
            // execute the post-script
            if (getPostscript() == null) {
                // try to execute the default prescript
                File fpost = new File(getInput(), "postscript-" + platform.getName() + ".sql");
                if (fpost.exists()) {
                    _log.info("Executing default postscript");
                    platform.evaluateBatch(DatabaseUtils.readFile(fpost), true);
                }                
            } else {
                platform.evaluateBatch(DatabaseUtils.readFile(getPostscript()), true);
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
    
    public File getInput() {
        return input;
    }

    public void setInput(File input) {
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
    
    /**
     * Specifies the verbosity of the task's debug output.
     * 
     * @param level The verbosity level
     * @ant.not-required Default is <code>INFO</code>.
     */
    public void setVerbosity(VerbosityLevel level)
    {
        _verbosity = level;
    }
    
}
