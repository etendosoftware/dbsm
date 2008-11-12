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
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.ColumnDataChange;
import org.apache.ddlutils.alteration.ColumnSizeChange;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DataToArraySink;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.task.VerbosityLevel;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.tools.ant.BuildException;
import org.openbravo.dal.core.DalInitializingTask;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.service.db.DataSetService;

/**
 * 
 * @author adrian
 */
public class ExportConfigScript extends DalInitializingTask {

	private String driver;
	private String url;
	private String user;
	private String password;
	private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

	private File prescript = null;
	private File postscript = null;

	private File model = null;
	private String coreData = null;
	private File moduledir;
	private String filter = "org.apache.ddlutils.io.AllDatabaseFilter";

	private File output;
	private String encoding = "UTF-8";

	protected Log _log;
	private VerbosityLevel _verbosity = null;
	private String codeRevision;
	private String industryTemplate;

	/** Creates a new instance of WriteDataXML */
	public ExportConfigScript() {
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
	public void doExecute() {

		if (industryTemplate == null) {
			throw new BuildException("No industry template was specified.");
		}

		initLogging();

		DataSetService datasetService = DataSetService.getInstance();

		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName(getDriver());
		ds.setUrl(getUrl());
		ds.setUsername(getUser());
		ds.setPassword(getPassword());

		Platform platform = PlatformFactory.createNewPlatformInstance(ds);
		DBSMOBUtil util = DBSMOBUtil.getInstance();
		util.getModules(platform, excludeobjects);
		
		String indTemp=util.getNameOfActiveIndustryTemplate();
		if(indTemp==null)
		{
			_log.info("ERROR: There is no industry template set as development.");
			System.exit(1);
		}
		industryTemplate=indTemp;
		
		//util.getModulesForIndustryTemplate(industryTemplate, new Vector<String>());
		_log.info("Loading model from XML files");
		Vector<File> dirs = new Vector<File>();
		dirs.add(model);

		for (int j = 0; j < util.getModuleCount(); j++) {
			if (!util.getModule(j).name.equalsIgnoreCase("CORE")) {
				File dirF = new File(moduledir, util.getModule(j).dir + "/src-db/database/model/");
				System.out.println(dirF.getAbsolutePath());
				if (dirF.exists()) {
					dirs.add(dirF);
				}
			}
		}
		File[] fileArray = new File[dirs.size()];
		for (int i = 0; i < dirs.size(); i++) {
			_log.info("Loading model for module. Path: " + dirs.get(i).getAbsolutePath());
			fileArray[i] = dirs.get(i);
		}
		Database xmlModel = DatabaseUtils.readDatabase(fileArray);

		_log.info("Loading original data from XML files");

		DatabaseDataIO dbdio = new DatabaseDataIO();
		dbdio.setEnsureFKOrder(false);
		dbdio.setDatabaseFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), xmlModel));

		DataReader dataReader = dbdio.getConfiguredCompareDataReader(xmlModel);

		Vector<File> dataFiles = DBSMOBUtil.loadFilesFromFolder(getCoreData());

		for (int j = 0; j < util.getModuleCount(); j++) {
			if (!util.getModule(j).name.equalsIgnoreCase("CORE")) {
				File dirF = new File(moduledir, util.getModule(j).dir + "/src-db/database/sourcedata/");
				System.out.println(dirF.getAbsolutePath());
				if (dirF.exists()) {
					dataFiles.addAll(DBSMOBUtil.loadFilesFromFolder(dirF.getAbsolutePath()));
				}
			}
		}

		DatabaseData databaseOrgData = new DatabaseData(xmlModel);
		for (int i = 0; i < dataFiles.size(); i++) {
			// _log.info("Loading data for module. Path:
			// "+dataFiles.get(i).getAbsolutePath());
			try {
				dataReader.getSink().start();
				String tablename = dataFiles.get(i).getName().substring(0, dataFiles.get(i).getName().length() - 4);
				Vector<DynaBean> vectorDynaBeans = ((DataToArraySink) dataReader.getSink()).getVector();
				dataReader.parse(dataFiles.get(i));
				databaseOrgData.insertDynaBeansFromVector(tablename, vectorDynaBeans);
				dataReader.getSink().end();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}

		_log.info("Loading complete model from current database");
		Database currentdb = platform.loadModelFromDatabase(DatabaseUtils.getExcludeFilter(excludeobjects));

		_log.info("Creating submodels for modules");

		Database databaseModel = null;
		for (int i = 0; i < util.getModuleCount(); i++) {
			_log.info("Creating submodel for module: " + util.getModule(i).name);
			Database dbI = null;
			try {
				dbI = (Database) currentdb.clone();
			}
			catch (Exception e) {
				System.out.println("Error while cloning the database model" + e.getMessage());
				e.printStackTrace();
				return;
			}
			dbI.applyNamingConventionFilter(util.getActiveModule(i).filter);
			if (databaseModel == null)
				databaseModel = dbI;
			else
				databaseModel.mergeWith(dbI);
		}

		_log.info("Comparing models...");
		Vector<String> modIds = new Vector<String>();
		for (int i = 0; i < util.getModuleCount(); i++) {
			String mod = util.getModule(i).idMod;
			_log.info("Module added to comparison: " + mod);
			modIds.add(mod);
		}
		DataComparator dataComparator = new DataComparator(platform.getSqlBuilder().getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
		dataComparator.setFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), currentdb));
		dataComparator.compareUsingDAL(xmlModel, databaseModel, platform, databaseOrgData, "ADCS", null);
		Vector<Change> dataChanges = new Vector<Change>();
		dataChanges.addAll(dataComparator.getChanges());
		Vector<Change> finalChanges = new Vector<Change>();
		Vector<Change> comparatorChanges = new Vector<Change>();
		comparatorChanges.addAll(dataComparator.getModelChangesList());
		for (Object change : dataComparator.getModelChangesList())
			if (change instanceof ColumnSizeChange)
			{
				finalChanges.add((ColumnSizeChange) change);
				comparatorChanges.remove(change);
			}
		
		for(Change change: dataComparator.getChanges())
			if(change instanceof ColumnDataChange)
			{
				finalChanges.add((change));
				dataChanges.remove(change);
			}

		comparatorChanges.addAll(dataChanges);
		
		DatabaseIO dbIO = new DatabaseIO();

		File configFile = new File(moduledir, industryTemplate + "/src-db/database/configScript.xml");
		File folder = new File(configFile.getParent());

		folder.mkdirs();
		dbIO.write(configFile, finalChanges);
		
		_log.info("Changes that couldn't be exported to the config script:");
		_log.info("*******************************************************");
		for(Change c:comparatorChanges)
		{
			_log.info(c);
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

	public String getCoreData() {
		return coreData;
	}

	public void setCoreData(String coreData) {
		this.coreData = coreData;
	}

	public String getIndustryTemplate() {
		return industryTemplate;
	}

	public void setIndustryTemplate(String industryTemplate) {
		this.industryTemplate = industryTemplate;
	}

}
