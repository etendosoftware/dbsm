package org.openbravo.ddlutils.util;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.apache.tools.ant.BuildException;
import org.openbravo.ddlutils.task.DatabaseUtils;

public class DBSMOBUtil {

	private Vector<ModuleRow> allModules = new Vector<ModuleRow>();
	private Vector<ModuleRow> activeModules = new Vector<ModuleRow>();
	private HashMap<String, Vector<String>> dependencies = new HashMap<String, Vector<String>>();
	private HashMap<String, Vector<String>> incdependencies = new HashMap<String, Vector<String>>();
	private Vector<String> dirDependantModules = new Vector<String>();
	private HashMap<String, Vector<String>> prefixDependencies = new HashMap<String, Vector<String>>();
	private Vector<String> idTemplates = new Vector<String>();
	private Vector<String> idModulesToExport = new Vector<String>();

	private static DBSMOBUtil instance = null;

	public static DBSMOBUtil getInstance() {
		if (instance == null)
			instance = new DBSMOBUtil();
		return instance;
	}

	public ModuleRow getModule(int i) {
		return allModules.get(i);
	}

	public ModuleRow getActiveModule(int i) {
		return activeModules.get(i);
	}

	public ModuleRow getTemplateModule(int i) {
		String dirMod = dirDependantModules.get(i);
		for (ModuleRow row : allModules)
			if (row.dir.equalsIgnoreCase(dirMod))
				return row;
		return null;
	}
	
	public ModuleRow getIndustryTemplateId(int ind)
	{
		return getRow(idTemplates.get(ind));
	}
	
	public int getIndustryTemplateCount()
	{
		return idTemplates.size();
	}

	public int getModuleCount() {
		return allModules.size();
	}

	public int getActiveModuleCount() {
		return activeModules.size();
	}

	public int getTemplateModuleCount() {
		return dirDependantModules.size();
	}

	public void getModules(Platform platform, String excludeobjects) {
		getDependencies(platform);
		getInclusiveDependencies(platform);
		String sql = "SELECT * FROM AD_MODULE";

		Connection connection = platform.borrowConnection();
		ResultSet resultSet = null;
		try {
			PreparedStatement statement = connection.prepareStatement(sql);
			statement.execute();
			resultSet = statement.getResultSet();
			if (resultSet.next()) {
			} else {
				throw new BuildException(
						"Module information not found in database: table AD_MODULE empty.");
			}
		} catch (Exception e) {
			throw new BuildException(
					"Module information not found in database (error while trying to read table AD_MODULE).");
		}

		try {
			do {
				ModuleRow modRow = new ModuleRow();
				// modRow.value=readStringFromRS(resultSet, "DB_PREFIX");
				modRow.prefixes = new Vector<String>();
        modRow.exceptions = new Vector<ExceptionRow>();
        modRow.othersexceptions = new Vector<ExceptionRow>();
				modRow.name = readStringFromRS(resultSet, "NAME");
				modRow.isInDevelopment = readStringFromRS(resultSet,
						"ISINDEVELOPMENT");
				modRow.dir = readStringFromRS(resultSet, "JAVAPACKAGE");
				modRow.idMod = readStringFromRS(resultSet, "AD_MODULE_ID");
				modRow.type = readStringFromRS(resultSet, "TYPE");
				if (modRow.isInDevelopment!=null && modRow.isInDevelopment.equalsIgnoreCase("Y")) {
					activeModules.add(modRow);
				}
				allModules.add(modRow);
				String sqlPrefix = "SELECT * FROM AD_MODULE_DBPREFIX WHERE AD_MODULE_ID='"
						+ modRow.idMod + "'";
				PreparedStatement statementPrefix = connection
						.prepareStatement(sqlPrefix);
				statementPrefix.execute();
				ResultSet rsPrefix = statementPrefix.getResultSet();
				while (rsPrefix.next()) {
					String prefix = readStringFromRS(rsPrefix, "NAME");
					modRow.prefixes.add(prefix);
				}


        String sqlExceptions = "SELECT * FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"+ modRow.idMod + "'";
        PreparedStatement statementExceptions = connection.prepareStatement(sqlExceptions);
        statementExceptions.execute();
        ResultSet resExceptions = statementExceptions.getResultSet();
        while (resExceptions.next()) {
          ExceptionRow ex=new ExceptionRow();
          ex.name1 = readStringFromRS(resExceptions, "NAME1");
          ex.name2 = readStringFromRS(resExceptions, "NAME2");
          ex.type = readStringFromRS(resExceptions, "TYPE");
          modRow.exceptions.add(ex);
        }

        String sqlExceptions2 = "SELECT * FROM AD_EXCEPTIONS WHERE AD_MODULE_ID<>'"+ modRow.idMod + "'";
        PreparedStatement statementExceptions2 = connection.prepareStatement(sqlExceptions2);
        statementExceptions2.execute();
        ResultSet resExceptions2 = statementExceptions2.getResultSet();
        while (resExceptions2.next()) {
          ExceptionRow ex=new ExceptionRow();
          ex.name1 = readStringFromRS(resExceptions2, "NAME1");
          ex.name2 = readStringFromRS(resExceptions2, "NAME2");
          ex.type = readStringFromRS(resExceptions2, "TYPE");
          modRow.othersexceptions.add(ex);
        }

			} while (resultSet.next());

			for (int i = 0; i < allModules.size(); i++) {
				ExcludeFilter filter = DatabaseUtils
						.getExcludeFilter(excludeobjects);
				for (String s : allModules.get(i).prefixes)
					filter.addPrefix(s);
				Vector<String> otherMods = new Vector<String>();
				Vector<String> otherActiveMods = new Vector<String>();
				for (int j = 0; j < allModules.size(); j++)
					if (!allModules.get(j).dir.equalsIgnoreCase(allModules
							.get(i).dir)) {
						if (allModules.get(j).isInDevelopment!=null && allModules.get(j).isInDevelopment.equals("Y"))
							otherActiveMods.addAll(allModules.get(j).prefixes);
						otherMods.addAll(allModules.get(j).prefixes);
					}
				filter.addOthersPrefixes(otherMods);
				filter.addOtherActivePrefixes(otherActiveMods);
				filter.addDependencies(prefixDependencies);
				filter.setName(allModules.get(i).dir);
        for(ExceptionRow row:allModules.get(i).exceptions)
          filter.addException(row);
        for(ExceptionRow row:allModules.get(i).othersexceptions)
          filter.addOthersException(row);
				allModules.get(i).filter = filter;
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new BuildException(
					"Error while trying to fetch module information from database: "
							+ e.getMessage());
		}
	}

	private String readStringFromRS(ResultSet rs, String columnName)
			throws SQLException {
		String value = "";
		try {
			value = rs.getString(columnName);
		} catch (Exception e) {
			value = rs.getString(columnName.toLowerCase());
		}
		return value;
	}

	public static void verifyRevision(Platform platform, String codeRevision,
			Log _log) {
		String sql = "SELECT * FROM AD_SYSTEM_INFO";

		Connection connection = platform.borrowConnection();
		ResultSet resultSet = null;
		try {
			PreparedStatement statement = connection.prepareStatement(sql);
			statement.execute();
			resultSet = statement.getResultSet();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			throw new BuildException("Code revision not found in database");
		}
		try {
			if (resultSet.next()) {
			} else {
				throw new BuildException("Code revision not found in database");
			}
		} catch (Exception e) {
			throw new BuildException("Code revision not found in database");
		}
		int databaseRevision = 0;
		try {
			databaseRevision = resultSet.getInt("CODE_REVISION");
		} catch (Exception e) {
			try {
				databaseRevision = resultSet.getInt("code_revision");
			} catch (Exception er) {
				System.out
						.println("Error while trying to fetch code revision from database.");
			}
		}
		_log.info("Database code revision: #" + databaseRevision + "#");

		_log.info("Source code revision: #" + codeRevision + "#");
		if (codeRevision.equals("0")) {
			_log.info("Subversion code revision not found.");
		} else if (Integer.parseInt(codeRevision) != databaseRevision) {
			throw new BuildException(
					"Database revision different from source code revision. An svn switch to your previous revision should be performed before exporting: svn switch URL@revision .");
		}
	}

	public static Vector<File> loadFilesFromFolder(String folders) {
		StringTokenizer strTokFol = new StringTokenizer(folders, ",");
		Vector<File> files = new Vector<File>();

		while (strTokFol.hasMoreElements()) {
			String folder = strTokFol.nextToken();
			File[] fileArray = DatabaseUtils.readFileArray(new File(folder));
			for (int i = 0; i < fileArray.length; i++) {
				files.add(fileArray[i]);
			}
		}
		return files;
	}
	
	public void getIncDependenciesForModuleList(String list)
	{
	    StringTokenizer st=new StringTokenizer(list, ",");
	    while(st.hasMoreElements())
	    {
		getIncludedModulesInModule(st.nextToken(), idModulesToExport);
	    }
	}
	
	public boolean isIncludedInExportList(ModuleRow row)
	{
	    return idModulesToExport.contains(row.idMod);
	}

	public void getModulesForIndustryTemplate(String templateDir,
			Vector<String> idList) {
		String tempId = getId(templateDir);
		dirDependantModules.add(templateDir);
		if(!idList.contains(tempId))
		    idList.add(tempId);
		if (incdependencies.get(tempId) != null) {
			for (String id : incdependencies.get(tempId)) {
				if (!idList.contains(id)) {
					getModulesForIndustryTemplate(getDir(id), idList);
				}
			}
		}
	}

	public void getIncludedModulesInModule(String moduleDir,Vector<String> idList) {
		ModuleRow row=getRowFromDir(moduleDir);
		if(!idList.contains(row.idMod))
		    idList.add(row.idMod);
		if (incdependencies.get(row.idMod) != null) {
			for (String id : incdependencies.get(row.idMod)) {
				if (!idList.contains(id)) {
				    getIncludedModulesInModule(getRow(id).name, idList);
				}
			}
		}
	}
	
	public void generateIndustryTemplateTree()
	{
		for(ModuleRow row:allModules)
		{
			if(row.type.equalsIgnoreCase("T") && incdependencies.get(row.idMod)==null)
			{
				//We've found an Industry Template which is not included in any other one.
				//We will find the Industry Templates which are included in it
				recursiveTemplateLoader(row.idMod);
				idTemplates.add(row.idMod);
			}
		}
	}
	
	private void recursiveTemplateLoader(String idDepTemplate)
	{
		for(ModuleRow row:allModules)
		{
			if(row.type.equalsIgnoreCase("T"))
			{
				Vector<String> dep = incdependencies.get(row.idMod);
				if(dep!=null && dep.contains(idDepTemplate))
				{
					recursiveTemplateLoader(row.idMod);
					idTemplates.add(row.idMod);
				}
			}
		}
	}
	
	public String getNameOfActiveIndustryTemplate()
	{
		for(ModuleRow row:allModules)
			if(row.type.equals("T") && row.isInDevelopment.equals("Y"))
				return row.dir;
		return null;
	}

	private void getDependencyHashMap() {
		for (ModuleRow row : allModules) {
			getDependencyForPrefix(row.dir, new Vector<String>(), row);
		}
	}

	private void getDependencyForPrefix(String dir, Vector<String> idList,
			ModuleRow row) {
		if (prefixDependencies.get(dir) == null)
			prefixDependencies.put(dir, new Vector<String>());
		prefixDependencies.get(dir).addAll(row.prefixes);
		idList.add(row.idMod);
		if (dependencies.get(row.idMod) != null) {
			for (String id : dependencies.get(row.idMod)) {
				if (!idList.contains(id))
					getDependencyForPrefix(dir, idList, getRow(id));
			}
		}

	}

	private void getDependencies(Platform platform) {
		String query = "SELECT * from AD_MODULE_DEPENDENCY";
		Connection connection = platform.borrowConnection();
		ResultSet resultSet = null;
		try {
			PreparedStatement statement = connection.prepareStatement(query);
			statement.execute();
			resultSet = statement.getResultSet();
			while (resultSet.next()) {
				String ad_module_id = readStringFromRS(resultSet,
						"AD_MODULE_ID");
				String ad_dependent_module_id = readStringFromRS(resultSet,
						"AD_DEPENDENT_MODULE_ID");
				if (!dependencies.containsKey(ad_dependent_module_id))
					dependencies.put(ad_dependent_module_id, new Vector<String>());
				dependencies.get(ad_dependent_module_id).add(ad_module_id);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new BuildException(
					"Problem while loading module dependencies.");
		}
	}

	private void getInclusiveDependencies(Platform platform) {
		String query = "SELECT * from AD_MODULE_DEPENDENCY WHERE ISINCLUDED='Y'";
		Connection connection = platform.borrowConnection();
		ResultSet resultSet = null;
		try {
			PreparedStatement statement = connection.prepareStatement(query);
			statement.execute();
			resultSet = statement.getResultSet();
			while (resultSet.next()) {
				String ad_module_id = readStringFromRS(resultSet,
						"AD_MODULE_ID");
				String ad_dependent_module_id = readStringFromRS(resultSet,
						"AD_DEPENDENT_MODULE_ID");
				if (!incdependencies.containsKey(ad_dependent_module_id))
					incdependencies.put(ad_dependent_module_id, new Vector<String>());
				incdependencies.get(ad_dependent_module_id).add(ad_module_id);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new BuildException(
					"Problem while loading module dependencies.");
		}
	}

	public String getDir(String modId) {
		for (ModuleRow row : allModules)
			if (row.idMod.equalsIgnoreCase(modId))
				return row.dir;
		return null;
	}

	public String getId(String dir) {
		for (ModuleRow row : allModules) {
			if (row.dir.equalsIgnoreCase(dir))
				return row.idMod;
		}
		return null;
	}

	public ModuleRow getRow(String modId) {
		for (ModuleRow row : allModules) {
			if (row.idMod.equalsIgnoreCase(modId))
				return row;
		}
		return null;
	}
  
  public ModuleRow getRowFromName(String name) {
    for(ModuleRow row: allModules){
      if(row.name.equalsIgnoreCase(name))
        return row;
    }
    return null;
  }
  
  public ModuleRow getRowFromDir(String dir) {
    for(ModuleRow row: allModules){
      if(row.dir.equalsIgnoreCase(dir))
        return row;
    }
    return null;
  }
}