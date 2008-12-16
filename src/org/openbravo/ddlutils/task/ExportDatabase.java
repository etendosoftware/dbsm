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
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.task.VerbosityLevel;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.ExceptionRow;
import org.openbravo.ddlutils.util.ModuleRow;

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
	private File moduledir;
	private String module;

	protected Log _log;
	private VerbosityLevel _verbosity = null;

	/** Creates a new instance of ExportDatabase */
	public ExportDatabase() {
	}

	/**
	 * Initializes the logging.
	 */
	private void initLogging() {
		// For Ant, we're forcing DdlUtils to do logging via log4j to the
		// console
		Properties props = new Properties();
		String level = (_verbosity == null ? Level.INFO.toString() : _verbosity.getValue()).toUpperCase();

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

	@Override
	public void execute() {

		initLogging();
    _log.info("Database connection: "+getUrl()+". User: "+getUser());

		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName(getDriver());
		ds.setUrl(getUrl());
		ds.setUsername(getUser());
		ds.setPassword(getPassword());

		Platform platform = PlatformFactory.createNewPlatformInstance(ds);
		// platform.setDelimitedIdentifierModeOn(true);

		DBSMOBUtil.verifyRevision(platform, getCodeRevision(), _log);

		DBSMOBUtil util = DBSMOBUtil.getInstance();
		util.getModules(platform, excludeobjects);
		if(util.getActiveModuleCount()==0)
		{
		  _log.info("No active modules. For a module to be exported, it needs to be set as 'InDevelopment'");
		  return;
		}

		if(module==null || module.equals("%"))
		{
			Database db = platform.loadModelFromDatabase(DatabaseUtils.getExcludeFilter(excludeobjects));
	
			for (int i = 0; i < util.getActiveModuleCount(); i++) {
				_log.info("Exporting module: " + util.getActiveModule(i).name);
        System.out.println("exceptions");
        Vector<ExceptionRow> v=util.getActiveModule(i).exceptions;
        for(ExceptionRow row:v)
          System.out.println(row.name1+";;"+row.name2+";;"+row.type);
        System.out.println("otherexceptions");
        v=util.getActiveModule(i).othersexceptions;
        for(ExceptionRow row:v)
          System.out.println(row.name1+";;"+row.name2+";;"+row.type);
				Database dbI = null;
				try {
					dbI = (Database) db.clone();
				}
				catch (Exception e) {
					System.out.println("Error while cloning the database model" + e.getMessage());
					return;
				}
				dbI.applyNamingConventionFilter(util.getActiveModule(i).filter);
				_log.info(db.toString());
				DatabaseIO io = new DatabaseIO();
				if (util.getActiveModule(i).name.equalsIgnoreCase("CORE")) {
					_log.info("Path: " + model.getAbsolutePath());
					io.writeToDir(dbI, model);
				}
				else {
					File path = new File(moduledir, util.getActiveModule(i).dir + "/src-db/database/model/");
					_log.info("Path: " + path);
					io.writeToDir(dbI, path);
				}
			}
		}
		else
		{
		    if(module!=null && !module.equals("%"))
			    util.getIncDependenciesForModuleList(module);
		    
        	    for(int i=0;i<util.getActiveModuleCount();i++)
        	    {
		        ModuleRow row=util.getActiveModule(i);
		        if(util.isIncludedInExportList(row))
		        {
        	        	_log.info("Exporting module: "+row.name);
        		        if(row==null)
        		        	throw new BuildException("Module "+row.name+" not found in AD_MODULE table.");
        		        if(row.prefixes.size()==0)
        		        {
        		        	_log.info("Module doesn't have dbprefix. We will not export structure for it.");
        		        	return;
        		        }
        		        if(row.isInDevelopment!=null && row.isInDevelopment.equalsIgnoreCase("Y"))
        		        {
        		        	_log.info("Loading submodel from database...");
        		        	Database db = platform.loadModelFromDatabase(row.filter, row.prefixes.get(0), false, row.idMod);
        		        	_log.info("Submodel loaded");
        		        	DatabaseIO io = new DatabaseIO();
        		        	File path = new File(moduledir, row.dir + "/src-db/database/model/");
        		        	_log.info("Path: " + path);
        		        	io.writeToDir(db, path);
        		        }
        		        else
        		        {
        		            _log.info("Module is not in development. Check that it is, before trying to export it.");
        		        }
        	        }
        	    }
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
