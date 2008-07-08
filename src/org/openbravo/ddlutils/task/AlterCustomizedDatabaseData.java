
package org.openbravo.ddlutils.task;

import java.io.File;
import java.io.StringWriter;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.beanutils.DynaProperty;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.AddFunctionChange;
import org.apache.ddlutils.alteration.AddTriggerChange;
import org.apache.ddlutils.alteration.AddViewChange;
import org.apache.ddlutils.alteration.DataChange;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.alteration.ModelChange;
import org.apache.ddlutils.alteration.RemoveFunctionChange;
import org.apache.ddlutils.alteration.RemoveTriggerChange;
import org.apache.ddlutils.alteration.RemoveViewChange;
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DataToArraySink;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.io.DatabaseFilter;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.task.VerbosityLevel;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.tools.ant.Task;


public class AlterCustomizedDatabaseData extends Task {

    private String driver;
    private String url;
    private String user;
    private String password;
    private File model;
    private File orgModel;
    private String data;
    private String orgData;
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
        
        _log.info("Loading original model from XML files");
        Database originaldb = DatabaseUtils.readDatabase(getOrgModel());
        
        DatabaseDataIO dbdio = new DatabaseDataIO();
        dbdio.setEnsureFKOrder(false);
        dbdio.setDatabaseFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), originaldb));

        DataReader dataReader = dbdio.getConfiguredCompareDataReader(originaldb);

        Vector<File> files=loadFilesFromFolder(getOrgData());
        
        _log.info("Loading original data from XML files");
        DatabaseData databaseOrgData=new DatabaseData(originaldb);
        for(int i=0;i<files.size();i++)
        {
        	try{
            	dataReader.getSink().start();
        		String tablename=files.get(i).getName().substring(0, files.get(i).getName().length()-4);
        		Vector<DynaBean> vectorDynaBeans=((DataToArraySink)dataReader.getSink()).getVector();
        		dataReader.parse(files.get(i));
        		databaseOrgData.insertDynaBeansFromVector(tablename,vectorDynaBeans);
        		dataReader.getSink().end();
        	}catch(Exception e){
            	System.out.println(e.getLocalizedMessage());
            }
        }

        _log.info("Loading model from current database");
        Database currentdb = platform.loadModelFromDatabase(DatabaseUtils.getExcludeFilter(excludeobjects)); 

    	Database currentcloneddb=null;
    	
    	try
    	{
    		currentcloneddb=(Database)currentdb.clone();
    	}
    	catch(Exception e)
    	{
    		_log.info("Model not cloned: "+e.getMessage());
    		currentcloneddb=currentdb;
    	}
    	
        _log.info("Finding customizations made to the database");
        DataComparator dataComparator=new DataComparator(platform.getSqlBuilder().getPlatformInfo(),platform.isDelimitedIdentifierModeOn());
        dataComparator.setFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), currentdb));
        dataComparator.compare(originaldb, currentdb, platform, databaseOrgData);
        
        Vector<DataChange> dataCustomization=dataComparator.getChanges();
        for(int i=0;i<dataCustomization.size();i++)
        	System.out.println(dataCustomization.get(i));
        System.out.println("=======================");
        
        _log.info("Loading new version model from XML files");

        Database newDb = DatabaseUtils.readDatabase(getModel());

        DatabaseDataIO dbdioNew = new DatabaseDataIO();
        dbdioNew.setEnsureFKOrder(false);
        dbdioNew.setDatabaseFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), newDb));

        DataReader dataReaderNew = dbdio.getConfiguredCompareDataReader(newDb);

        Vector<File> filesNew=loadFilesFromFolder(getData());

        _log.info("Loading new version data from XML files");
        DatabaseData databaseNewData=new DatabaseData(newDb);
        for(int i=0;i<filesNew.size();i++)
        {
        	try{ 
            	dataReaderNew.getSink().start();
        		String tablename=filesNew.get(i).getName().substring(0, filesNew.get(i).getName().length()-4);
        		Vector<DynaBean> vectorDynaBeans=((DataToArraySink)dataReaderNew.getSink()).getVector();
        		dataReaderNew.parse(filesNew.get(i));
        		databaseNewData.insertDynaBeansFromVector(tablename,vectorDynaBeans);
        		dataReaderNew.getSink().end();
        		
        	}catch(Exception e){
            	System.out.println(e.getLocalizedMessage());
            }
        }
        
        _log.info("Filtering customizations");

        List modelChanges=dataComparator.getModelChangesList();
        for(int i=0;i<modelChanges.size();i++)
        {
        	if(modelChanges.get(i) instanceof RemoveTriggerChange)
        	{
        		boolean cont=false;
        		RemoveTriggerChange removeTrigger=(RemoveTriggerChange)modelChanges.get(i);
        		for(int j=0;!cont && j<modelChanges.size();j++)
        		{
        			
        			if(modelChanges.get(j) instanceof AddTriggerChange && 
        					removeTrigger.getTrigger().getName().equalsIgnoreCase(((AddTriggerChange)modelChanges.get(j)).getNewTrigger().getName()))
        			{
        				AddTriggerChange addTrigger=(AddTriggerChange)modelChanges.get(j);
        				modelChanges.remove(addTrigger);
        				modelChanges.remove(removeTrigger);
        				i--;
        				cont=true;
        			}
        		}
        	}
        	else if(modelChanges.get(i) instanceof RemoveFunctionChange)
        	{
        		boolean cont=false;
        		RemoveFunctionChange removeFunction=(RemoveFunctionChange)modelChanges.get(i);
        		for(int j=0;!cont && j<modelChanges.size();j++)
        		{
        			
        			if(modelChanges.get(j) instanceof AddFunctionChange && 
        					removeFunction.getFunction().getName().equalsIgnoreCase(((AddFunctionChange)modelChanges.get(j)).getNewFunction().getName()))
        			{
        				AddFunctionChange addFunction=(AddFunctionChange)modelChanges.get(j);
        				modelChanges.remove(addFunction);
        				modelChanges.remove(removeFunction);
        				i--;
        				cont=true;
        			}
        		}
        	}
        	else if(modelChanges.get(i) instanceof AddViewChange)
        	{
				AddViewChange addView=(AddViewChange)modelChanges.get(i);
				if(newDb.findView(addView.getNewView().getName())!=null)
        		{
        				modelChanges.remove(addView);
        		}
        	}
        }
        
        _log.info("Applying model customizations to the new version");
        
        Iterator itModelChanges=dataComparator.getModelChanges();
        while(itModelChanges.hasNext())
        {
        	((ModelChange)itModelChanges.next()).apply(newDb, platform.isDelimitedIdentifierModeOn());
        }

        _log.info("Applying data customizations to the new version");
        Iterator itDataChanges=dataCustomization.iterator();
        while(itDataChanges.hasNext())
        {
        	((DataChange)itDataChanges.next()).apply(databaseNewData, platform.isDelimitedIdentifierModeOn());
        }

        /*_log.info("Comparing customized new version to existing database");

        DataComparator finalDataComparator=new DataComparator(platform.getSqlBuilder().getPlatformInfo(),platform.isDelimitedIdentifierModeOn());
        dataComparator.setFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), originaldb));
        dataComparator.compare(currentdb, newDb, platform, databaseNewData);*/
        _log.info("Updating existing database");
        

        try{
        	
        	  
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
            
        	_log.info("Updating database model");
        	platform.alterTables(currentdb, newDb, !isFailonerror()); 

        	_log.info("Updating database data");
        	dataReader = dbdio.getConfiguredCompareDataReader(newDb);
        }catch(Exception e)
        {
        	e.printStackTrace();
        	System.out.println(e.getMessage());
        }
    	Connection connection=platform.borrowConnection();
    	//dataReader.getSink().start();
    	
    	_log.debug("Disabling foreign keys");
    	platform.disableAllFK(connection, newDb, !isFailonerror());
    	_log.debug("Disabling triggers");
    	platform.disableAllTriggers(connection, newDb, !isFailonerror());
    	DatabaseFilter filter=DatabaseUtils.getDynamicDatabaseFilter(getFilter(), originaldb);
    	if (filter != null) {
            String [] tablenames = filter.getTableNames();
            String [] tableFilters=new String[tablenames.length];
            int ind=0;
            for (String table : tablenames) {
            	tableFilters[ind]=filter.getTableFilter(table);
            	ind++;
            }
            platform.deleteDataFromTable(connection, newDb, tablenames, tableFilters, !isFailonerror());
            
        }      
    	//StringWriter stringWriter=new StringWriter();
    	//platform.getSqlBuilder().setWriter(stringWriter);
    	for(int i=0;i<newDb.getTableCount();i++)
    	{
    		Table table=newDb.getTable(i);
    		_log.debug("Inserting data from table "+table.getName());
    		Vector<DynaBean> rowsTable=databaseNewData.getRowsFromTable(table.getName());
    		if(rowsTable!=null)
    		{
	    		for(int j=0;j<rowsTable.size();j++)
	    		{
	    			DynaBean row=rowsTable.get(j);
	    			try{
		    			platform.upsert(connection, newDb, row);
	    			}catch(Exception e)
	    			{
	    				_log.info("Error. Row "+row+" couldn't be inserted. "+e.getMessage());
	    			}
	    		}
    		}
    	}
    	_log.debug("Executing update final script (NOT NULLs and dropping temporal tables");
        platform.alterTablesPostScript(currentcloneddb, newDb, !isFailonerror());
        _log.debug("Enabling Foreign Keys and Triggers");
		platform.deleteInvalidConstraintRows(newDb, !isFailonerror());
    	platform.enableAllFK(connection, newDb, !isFailonerror());
    	platform.enableAllTriggers(connection, newDb, !isFailonerror());
        try
        {
        	connection.close();
            if (getPostscript() == null) {
                // try to execute the default prescript
                File fpost = new File(getModel(), "postscript-" + platform.getName() + ".sql");
                if (fpost.exists()) {
                    _log.info("Executing default postscript");
                    platform.evaluateBatch(DatabaseUtils.readFile(fpost), true);
                }                
            } else {
                platform.evaluateBatch(DatabaseUtils.readFile(getPostscript()), true);
            }
        }catch(Exception e)
        {
        	e.printStackTrace();
        	System.out.println("oops: "+e.getMessage());
        }
        
        
        
    }

    private Vector<File> loadFilesFromFolder(String folders)
    {
    	StringTokenizer strTokFol=new StringTokenizer(folders,",");
        Vector<File> files=new Vector<File>();
        
        while(strTokFol.hasMoreElements())
        {
        	String folder=strTokFol.nextToken();
        	File[] fileArray=DatabaseUtils.readFileArray(new File(folder));
        	for(int i=0;i<fileArray.length;i++)
        	{
        		files.add(fileArray[i]);
        	}
        }
        return files;
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

    public File getOrgModel() {
        return orgModel;
    }

    public void setOrgModel(File model) {
        this.orgModel = model;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getOrgData() {
        return orgData;
    }

    public void setOrgData(String data) {
        this.orgData = data;
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