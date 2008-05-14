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
import java.util.List;
import java.util.Properties;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.model.Database;
import org.apache.log4j.PropertyConfigurator;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.BuildException;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.task.VerbosityLevel;

/**
 *
 * @author adrian
 */
public class ExportDatabase extends Task {
    
    private String driver;
    private String url;
    private String user;
    private String password;
    private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";
    private String codeRevision;
    
    private File model;   

    protected Log _log;
    private VerbosityLevel _verbosity = null;    
    
    /** Creates a new instance of ExportDatabase */
    public ExportDatabase() {
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
        

        
        Database db = platform.loadModelFromDatabase(DatabaseUtils.getExcludeFilter(excludeobjects)); 
        

        verifyRevision(platform, db);
        _log.info(db.toString());
        
        DatabaseIO io = new DatabaseIO();
        io.writeToDir(db, model);        
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

    public String getCodeRevision() {
        return codeRevision;
    }

    public void setCodeRevision(String rev) {
        codeRevision=rev;
    }
    
    public void verifyRevision(Platform platform, Database database)
    {
    	String sql="SELECT * FROM AD_SYSTEM_INFO";
    	List list=platform.fetch(database, sql);
    	if(list.isEmpty())
    	{
    		throw new BuildException("Code revision not found in database");
    	}
    	else
    	{
    		DynaBean dynaBean=(DynaBean)list.get(0);
    		int databaseRevision=Integer.parseInt(dynaBean.get("CODE_REVISION").toString());
    		_log.info("Database code revision: #"+databaseRevision+"#");
    		

    		_log.info("Source code revision: #"+codeRevision+"#");
    		if(codeRevision.equals("0"))
    		{
    			_log.info("Subversion code revision not found.");
    		}
    		else if(Integer.parseInt(codeRevision)!=databaseRevision)
    		{
    			throw new BuildException("Database revision different from source code revision. A update.database is needed before exporting.");
    		}
    		
    		
    	}
    	
    }
}
