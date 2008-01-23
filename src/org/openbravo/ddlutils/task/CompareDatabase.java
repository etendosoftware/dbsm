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
import org.apache.ddlutils.model.Database;
import org.apache.log4j.PropertyConfigurator;
import org.apache.tools.ant.Task;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.Sequence;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.Trigger;
import org.apache.ddlutils.model.View;
import org.apache.ddlutils.task.VerbosityLevel;
import org.apache.tools.ant.BuildException;

/**
 *
 * @author adrian
 */
public class CompareDatabase extends Task {
    
    private String driver;
    private String url;
    private String user;
    private String password;
    private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";
    
    private File model;   

    protected Log _log;
    private VerbosityLevel _verbosity = null;    
    
    /** Creates a new instance of ExportDatabase */
    public CompareDatabase() {
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
        
            // Load database
            Database db1 = platform.loadModelFromDatabase(DatabaseUtils.getExcludeFilter(excludeobjects));        
//            if (db1 == null) { 
//                db1 = DatabaseUtils.loadCurrentDatabase(ds);
//            }
            _log.info("Platform database");
            _log.info(db1.toString());

            //Load database
            Database db2 = DatabaseUtils.readDatabase(getModel());        
            _log.info("Model database");
            _log.info(db2.toString());

            // Compare tables
            for (int i = 0; i < db1.getTableCount(); i++) {
                Table t1 = db1.getTable(i);
                Table t2 = db2.findTable(t1.getName());

                if (t2 == null)  {
                    _log.info("DIFF: TABLE NOT EXISTS "  + t1.getName());
                } else {
                    if (!t1.equals(t2)) {
                        _log.info("DIFF: TABLES DIFFERENTS "  + t1.getName());
                    }
                }            
            }

            // Compare views 
            for (int i = 0; i < db1.getViewCount(); i++) {
                View w1 = db1.getView(i);
                View w2 = db2.findView(w1.getName());

                if (w2 == null)  {
                    _log.info("DIFF: VIEW NOT EXISTS "  + w1.getName());
                } else {
                    if (!w1.equals(w2)) {
                        _log.info("DIFF: VIEWS DIFFERENTS "  + w1.getName());
                    }
                } 
            }

            // Compare sequences 
            for (int i = 0; i < db1.getSequenceCount(); i++) {
                Sequence s1 = db1.getSequence(i);
                Sequence s2 = db2.findSequence(s1.getName());

                if (s2 == null)  {
                    _log.info("DIFF: SEQUENCE NOT EXISTS "  + s1.getName());
                } else {
                    if (!s1.equals(s2)) {
                        _log.info("DIFF: SEQUENCES DIFFERENTS "  + s1.getName());
                    }
                } 
            }

            // Compare functions 
            for (int i = 0; i < db1.getFunctionCount(); i++) {
                Function f1 = db1.getFunction(i);
                Function f2 = db2.findFunction(f1.getName());

                if (f2 == null)  {
                    _log.info("DIFF: FUNCTION NOT EXISTS "  + f1.getName());
                } else {
                    if (!f1.equals(f2)) {
                        _log.info("DIFF: FUNCTIONS DIFFERENTS "  + f1.getName());
                    }
                } 
            }        

            // Compare TRIGGERS 
            for (int i = 0; i < db1.getTriggerCount(); i++) {
                Trigger t1 = db1.getTrigger(i);
                Trigger t2 = db2.findTrigger(t1.getName());

                if (t2 == null)  {
                    _log.info("DIFF: TRIGGER NOT EXISTS "  + t1.getName());
                } else {
                    if (!t1.equals(t2)) {
                        _log.info("DIFF: TRIGGERS DIFFERENTS "  + t1.getName());
                    }
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
