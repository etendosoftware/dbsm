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
import java.sql.Connection;
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
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.openbravo.dal.core.DalInitializingTask;
import org.openbravo.dal.service.OBDal;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.ModuleRow;

/**
 *
 * @author adrian
 */
public class AlterDatabaseDataMod extends DalInitializingTask {
    
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
    private String basedir;
    private String dirFilter;
    private String datadir;   
    private String datafilter;
    private String module;
    
    /** Creates a new instance of ReadDataXML */
    public AlterDatabaseDataMod() {
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
    
    @Override
	public void doExecute() {
       
        initLogging();
        _log.info("Database connection: "+getUrl()+". User: "+getUser());
        
        if(module==null || module.equals(""))
        {
        	_log.error("This task requires a module name to be passed as parameter. Example: ant update.database.mod -Dmodule=modulename");
        	throw new BuildException("No module name provided.");
        }
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(getDriver());
        ds.setUrl(getUrl());
        ds.setUsername(getUser());
        ds.setPassword(getPassword());    
        
        Platform platform = PlatformFactory.createNewPlatformInstance(ds);
        // platform.setDelimitedIdentifierModeOn(true);
        

        _log.info("Creating submodel for application dictionary");
        Database dbXML =null;
        if(basedir==null)
        {
          _log.info("Basedir for additional files not specified. Updating database with just Core.");
          dbXML = DatabaseUtils.readDatabase(getModel());
        }
        else
        {
          Vector<File> dirs=new Vector<File>();
          dirs.add(model);
          DirectoryScanner dirScanner=new DirectoryScanner();
          dirScanner.setBasedir(new File(basedir));
          String[] dirFilterA = {dirFilter};
          dirScanner.setIncludes(dirFilterA);
          dirScanner.scan();
          String[] incDirs=dirScanner.getIncludedDirectories();
          for(int j=0;j<incDirs.length;j++)
          {
            File dirF=new File(basedir, incDirs[j]);
            dirs.add(dirF);
          }
          File[] fileArray=new File[dirs.size()];
          for(int i=0;i<dirs.size();i++)
          {
            fileArray[i]=dirs.get(i);
          }
          dbXML = DatabaseUtils.readDatabase(fileArray);
        }
        
        Database completedb=null;
        Database dbAD=null;
        Database oldModel=null;
        try{
          completedb=(Database)dbXML.clone();
          dbAD = (Database)dbXML.clone();
          oldModel=(Database)dbXML.clone();
          dbAD.filterByDataset("ADCS");
        }catch(Exception e)
        {
          e.printStackTrace();
        }
        
        Vector<Vector<Change>> dataChanges = new Vector<Vector<Change>>();
        Vector<Database> moduleModels = new Vector<Database>();
        Vector<Database> moduleOldModels = new Vector<Database>();
        Vector<ModuleRow> moduleRows=new Vector<ModuleRow>();
        
        DBSMOBUtil util=DBSMOBUtil.getInstance();
        util.getModules(platform, excludeobjects);
        if(module.toUpperCase().contains("CORE") || module.equals("%"))
        {
        	_log.info("You've either specified a list that contains Core, or module specified is %. Complete update.database will be performed.");
        	AlterDatabaseDataAll ada=new AlterDatabaseDataAll();
        	ada.setDriver(driver);
        	ada.setUrl(url);
        	ada.setUser(user);
        	ada.setPassword(password);
        	ada.setExcludeobjects(excludeobjects);
        	ada.setModel(model);
        	ada.setFilter(filter);
        	ada.setInput(input);
        	ada.setObject(object);
        	ada.setFailonerror(failonerror);
        	ada.setVerbosity(_verbosity);
        	ada.setBasedir(basedir);
        	ada.setDirFilter(dirFilter);
        	ada.setDatadir(datadir);
        	ada.setDatafilter(datafilter);
        	ada.setUserId(userId);
        	ada.setPropertiesFile(propertiesFile);
        	ada.doExecute();
        	return;
        }
        StringTokenizer st=new StringTokenizer(module, ",");
        while(st.hasMoreElements())
        {
        	String modName=st.nextToken().trim();
        	_log.info("Updating module: "+modName);
	        ModuleRow row=util.getRowFromName(modName);
	        moduleRows.add(row);
	        if(row==null)
	        	throw new BuildException("Module "+modName+" not found in AD_MODULE table.");
	        Database originaldb=null;
	        try {
	        	if(row.prefixes.size()==0)
	        	{
	        		_log.info("Module doesn't have dbprefix. We will not update database model.");
	        	}
	        	else
	        	{
		        	_log.info("Loading submodel from database...");
		        	originaldb = platform.loadModelFromDatabase(row.filter, row.prefixes.get(0), true);
		        	originaldb.moveModifiedToTables();
		        	_log.info("Submodel loaded");
	        	}

	          Database db=(Database)dbXML.clone();
	          db.applyNamingConventionToUpdate(row.filter);
            Database olddb=(Database)oldModel.clone();
            olddb.applyNamingConventionToUpdate(row.filter);
	            
	          DatabaseDataIO dbdio = new DatabaseDataIO();
	          dbdio.setEnsureFKOrder(false);
	          dbdio.setDatabaseFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), dbAD));
	        	
	
	          _log.info("Loading data from XML files");
    				Vector<File> files=new Vector<File>();
    				File fsourcedata=new File(basedir, "/"+row.dir+"/src-db/database/sourcedata/");
    				File[] datafiles=DatabaseUtils.readFileArray(fsourcedata);
    				for(int i=0;i<datafiles.length;i++)
    					files.add(datafiles[i]);
	
	
	      		DataReader dataReader = dbdio.getConfiguredCompareDataReader(dbAD);
	      		DatabaseData databaseOrgData = new DatabaseData(dbAD);
	      		for (int i = 0; i < files.size(); i++) {
	      			try {
	      				dataReader.getSink().start();
	      				String tablename = files.get(i).getName().substring(0, files.get(i).getName().length() - 4);
	      				Vector<DynaBean> vectorDynaBeans = ((DataToArraySink) dataReader.getSink()).getVector();
	      				dataReader.parse(files.get(i));
	      				databaseOrgData.insertDynaBeansFromVector(tablename, vectorDynaBeans);
	      				dataReader.getSink().end();
	      			}
	      			catch (Exception e) {
	      				e.printStackTrace();
	      			}
	      		}
	
  	    		DataComparator dataComparator = new DataComparator(platform.getSqlBuilder().getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
  	    		dataComparator.compareUsingDALToUpdate(dbAD, platform, databaseOrgData, "AD", row.idMod);
  	    		_log.info("Data changes we will perform: ");
  	    		for(Change change:dataComparator.getChanges())
  	    			_log.info(change);
  	    		_log.info("Comparing databases to find data differences");
  	    		dataChanges.add(dataComparator.getChanges());
  	    		moduleModels.add(db);
  	    		moduleOldModels.add(olddb);
  	    		OBDal.getInstance().commitAndClose();
  	            
            _log.info("Updating database model...");
  
            if(row.prefixes.size()>0)
            	platform.alterTables(originaldb, db, !isFailonerror()); 
            /*StringWriter sw=new StringWriter();
            platform.getSqlBuilder().setWriter(sw);
            platform.getSqlBuilder().alterDatabase(originaldb, db, null);
            System.out.println(sw.toString());*/
            _log.info("Model update complete.");
  	            
	        } catch (Exception e) {
	            // log(e.getLocalizedMessage());
	        	e.printStackTrace();
	            throw new BuildException(e);
	        }   
        }
        
        _log.info("Updating database data...");

        _log.info("Disabling foreign keys");
        Connection connection=platform.borrowConnection();
        platform.disableAllFK(connection, dbAD, !isFailonerror());
        _log.info("Disabling triggers");
        platform.disableAllTriggers(connection, dbAD, !isFailonerror());
        for(int i=0;i<dataChanges.size();i++)
        {
          _log.info("Updating database data for module "+moduleRows.get(i).name);
              platform.alterData(connection, dbAD, dataChanges.get(i));
          _log.info("Removing invalid rows.");
          platform.deleteInvalidConstraintRows(completedb, !isFailonerror());
          _log.info("Executing update final script (NOT NULLs and dropping temporal tables");
              platform.alterTablesPostScript(moduleOldModels.get(i), moduleModels.get(i), !isFailonerror());
        }
        _log.info("Enabling Foreign Keys and Triggers");
        platform.enableAllFK(connection, dbAD, !isFailonerror());
        platform.enableAllTriggers(connection, dbAD, !isFailonerror());
            
        
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
    
    public String getBasedir() {
        return basedir;
    }
    
    public void setBasedir(String basedir)
    {
      this.basedir=basedir;
    }
    
    public String getDirFilter() {
      return dirFilter;
    }
    
    public void setDirFilter(String dirFilter) {
      this.dirFilter=dirFilter;
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

    public String getDatadir() {
      return datadir;
    }

    public void setDatadir(String datadir) {
      this.datadir = datadir;
    }

    public String getDatafilter() {
      return datafilter;
    }

    public void setDatafilter(String datafilter) {
      this.datafilter = datafilter;
    }

	public String getModule() {
		return module;
	}

	public void setModule(String module) {
		this.module = module;
	}
    
}
