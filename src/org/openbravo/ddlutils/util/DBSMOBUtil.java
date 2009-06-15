package org.openbravo.ddlutils.util;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.dynabean.SqlDynaBean;
import org.apache.ddlutils.dynabean.SqlDynaClass;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.apache.log4j.Logger;
import org.apache.tools.ant.BuildException;
import org.openbravo.ddlutils.task.DatabaseUtils;
import org.openbravo.service.dataset.DataSetService;
import org.openbravo.utils.CheckSum;

public class DBSMOBUtil {

  private Vector<ModuleRow> allModules = new Vector<ModuleRow>();
  private Vector<ModuleRow> activeModules = new Vector<ModuleRow>();
  private HashMap<String, Vector<String>> dependencies = new HashMap<String, Vector<String>>();
  private HashMap<String, Vector<String>> incdependencies = new HashMap<String, Vector<String>>();
  private Vector<String> dirTemplateModules = new Vector<String>();
  private HashMap<String, Vector<String>> prefixDependencies = new HashMap<String, Vector<String>>();
  private Vector<String> idTemplates = new Vector<String>();
  private Vector<String> idModulesToExport = new Vector<String>();
  private Vector<String> idDependantModules = new Vector<String>();

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
    final String dirMod = dirTemplateModules.get(i);
    for (final ModuleRow row : allModules)
      if (row.dir.equalsIgnoreCase(dirMod))
        return row;
    return null;
  }

  public ModuleRow getIndustryTemplateId(int ind) {
    return getRow(idTemplates.get(ind));
  }

  public int getIndustryTemplateCount() {
    return idTemplates.size();
  }

  public int getModuleCount() {
    return allModules.size();
  }

  public int getActiveModuleCount() {
    return activeModules.size();
  }

  public int getTemplateModuleCount() {
    return dirTemplateModules.size();
  }

  public boolean listDependsOnTemplate(String list) {
    final StringTokenizer st = new StringTokenizer(list, ",");
    while (st.hasMoreElements()) {
      String modDir = st.nextToken().trim();
      ModuleRow targetRow = getRowFromDir(modDir);
      if (targetRow.type.equalsIgnoreCase("T"))
        return true;
      for (ModuleRow row : allModules) {
        if (row.type.equalsIgnoreCase("T"))
          if (isDependant(row, targetRow))
            return true;
      }
    }
    return false;
  }

  public void getModules(Platform platform, String excludeobjects) {
    getDependencies(platform);
    getInclusiveDependencies(platform);
    final String sql = "SELECT * FROM AD_MODULE";

    final Connection connection = platform.borrowConnection();
    ResultSet resultSet = null;
    try {
      final PreparedStatement statement = connection.prepareStatement(sql);
      statement.execute();
      resultSet = statement.getResultSet();
      if (resultSet.next()) {
      } else {
        throw new BuildException("Module information not found in database: table AD_MODULE empty.");
      }
    } catch (final Exception e) {
      throw new BuildException(
          "Module information not found in database (error while trying to read table AD_MODULE).");
    }

    try {
      do {
        final ModuleRow modRow = new ModuleRow();
        // modRow.value=readStringFromRS(resultSet, "DB_PREFIX");
        modRow.prefixes = new Vector<String>();
        modRow.exceptions = new Vector<ExceptionRow>();
        modRow.othersexceptions = new Vector<ExceptionRow>();
        modRow.name = readStringFromRS(resultSet, "NAME");
        modRow.isInDevelopment = readStringFromRS(resultSet, "ISINDEVELOPMENT");
        modRow.dir = readStringFromRS(resultSet, "JAVAPACKAGE");
        modRow.idMod = readStringFromRS(resultSet, "AD_MODULE_ID");
        modRow.type = readStringFromRS(resultSet, "TYPE");
        if (modRow.isInDevelopment != null && modRow.isInDevelopment.equalsIgnoreCase("Y")) {
          activeModules.add(modRow);
        }
        allModules.add(modRow);
        final String sqlPrefix = "SELECT * FROM AD_MODULE_DBPREFIX WHERE AD_MODULE_ID='"
            + modRow.idMod + "'";
        final PreparedStatement statementPrefix = connection.prepareStatement(sqlPrefix);
        statementPrefix.execute();
        final ResultSet rsPrefix = statementPrefix.getResultSet();
        while (rsPrefix.next()) {
          final String prefix = readStringFromRS(rsPrefix, "NAME");
          modRow.prefixes.add(prefix);
        }

        final String sqlExceptions = "SELECT * FROM AD_EXCEPTIONS WHERE AD_MODULE_ID='"
            + modRow.idMod + "'";
        final PreparedStatement statementExceptions = connection.prepareStatement(sqlExceptions);
        statementExceptions.execute();
        final ResultSet resExceptions = statementExceptions.getResultSet();
        while (resExceptions.next()) {
          final ExceptionRow ex = new ExceptionRow();
          ex.name1 = readStringFromRS(resExceptions, "NAME1");
          ex.name2 = readStringFromRS(resExceptions, "NAME2");
          ex.type = readStringFromRS(resExceptions, "TYPE");
          modRow.exceptions.add(ex);
        }

        final String sqlExceptions2 = "SELECT * FROM AD_EXCEPTIONS WHERE AD_MODULE_ID<>'"
            + modRow.idMod + "'";
        final PreparedStatement statementExceptions2 = connection.prepareStatement(sqlExceptions2);
        statementExceptions2.execute();
        final ResultSet resExceptions2 = statementExceptions2.getResultSet();
        while (resExceptions2.next()) {
          final ExceptionRow ex = new ExceptionRow();
          ex.name1 = readStringFromRS(resExceptions2, "NAME1");
          ex.name2 = readStringFromRS(resExceptions2, "NAME2");
          ex.type = readStringFromRS(resExceptions2, "TYPE");
          modRow.othersexceptions.add(ex);
        }

      } while (resultSet.next());

      for (int i = 0; i < allModules.size(); i++) {
        final ExcludeFilter filter = DatabaseUtils.getExcludeFilter(excludeobjects);
        for (final String s : allModules.get(i).prefixes)
          filter.addPrefix(s);
        final Vector<String> otherMods = new Vector<String>();
        final Vector<String> otherActiveMods = new Vector<String>();
        for (int j = 0; j < allModules.size(); j++)
          if (!allModules.get(j).dir.equalsIgnoreCase(allModules.get(i).dir)) {
            if (allModules.get(j).isInDevelopment != null
                && allModules.get(j).isInDevelopment.equals("Y"))
              otherActiveMods.addAll(allModules.get(j).prefixes);
            otherMods.addAll(allModules.get(j).prefixes);
          }
        filter.addOthersPrefixes(otherMods);
        filter.addOtherActivePrefixes(otherActiveMods);
        filter.addDependencies(prefixDependencies);
        filter.setName(allModules.get(i).dir);
        for (final ExceptionRow row : allModules.get(i).exceptions)
          filter.addException(row);
        for (final ExceptionRow row : allModules.get(i).othersexceptions)
          filter.addOthersException(row);
        allModules.get(i).filter = filter;
      }

    } catch (final Exception e) {
      e.printStackTrace();
      throw new BuildException("Error while trying to fetch module information from database: "
          + e.getMessage());
    }
  }

  private String readStringFromRS(ResultSet rs, String columnName) throws SQLException {
    String value = "";
    try {
      value = rs.getString(columnName);
    } catch (final Exception e) {
      value = rs.getString(columnName.toLowerCase());
    }
    return value;
  }

  public static String getDBRevision(Platform platform) {
    final String sql = "SELECT * FROM AD_SYSTEM_INFO";

    final Connection connection = platform.borrowConnection();
    ResultSet resultSet = null;
    try {
      final PreparedStatement statement = connection.prepareStatement(sql);
      statement.execute();
      resultSet = statement.getResultSet();
    } catch (final Exception e) {
      System.out.println(e.getMessage());
      throw new BuildException("Code revision id not found in database");
    }
    try {
      if (resultSet.next()) {
      } else {
        throw new BuildException("Code revision id not found in database");
      }
    } catch (final Exception e) {
      throw new BuildException("Code revision id not found in database");
    }
    String databaseRevision = "0";
    try {
      databaseRevision = resultSet.getString("CODE_REVISION");
    } catch (final Exception e) {
      try {
        databaseRevision = resultSet.getString("code_revision");
      } catch (final Exception er) {
        System.out.println("Error while trying to fetch code revision id from database.");
      }
    }
    return databaseRevision;
  }

  public static void verifyRevision(Platform platform, String codeRevision, Logger _log) {
    String databaseRevision = getDBRevision(platform);

    _log.info("Database code revision id: " + databaseRevision);

    _log.info("Source code revision id: " + codeRevision);
    if (codeRevision.equals("0")) {
      _log.info("Mercurial code revision id not found.");
    } else if (!filterRevision(codeRevision).equals(filterRevision(databaseRevision))) {
      throw new BuildException(
          "Database revision id differs from the source code revision id. A hg update to your previous revision ID should be performed before exporting: hg update -r REV");
    }
  }

  private static String filterRevision(String revision) {
    if (revision.charAt(revision.length() - 1) == '+')
      return revision.substring(0, revision.length() - 1);
    return revision;
  }

  public static Vector<File> loadFilesFromFolder(String folders) {
    final StringTokenizer strTokFol = new StringTokenizer(folders, ",");
    final Vector<File> files = new Vector<File>();

    while (strTokFol.hasMoreElements()) {
      final String folder = strTokFol.nextToken();
      final File[] fileArray = DatabaseUtils.readFileArray(new File(folder));
      for (int i = 0; i < fileArray.length; i++) {
        files.add(fileArray[i]);
      }
    }
    return files;
  }

  public void getIncDependenciesForModuleList(String list) {
    final StringTokenizer st = new StringTokenizer(list, ",");
    while (st.hasMoreElements()) {
      getIncludedModulesInModule(st.nextToken().trim(), idModulesToExport);
    }
  }

  public boolean isIncludedInExportList(ModuleRow row) {
    return idModulesToExport.contains(row.idMod);
  }

  public void getModulesForIndustryTemplate(String templateDir, Vector<String> idList) {
    final String tempId = getId(templateDir);
    dirTemplateModules.add(templateDir);
    if (!idList.contains(tempId))
      idList.add(tempId);
    if (incdependencies.get(tempId) != null) {
      for (final String id : incdependencies.get(tempId)) {
        if (!idList.contains(id)) {
          getModulesForIndustryTemplate(getDir(id), idList);
        }
      }
    }
  }

  public void getIncludedModulesInModule(String moduleDir, Vector<String> idList) {
    final ModuleRow row = getRowFromDir(moduleDir);
    if (!idList.contains(row.idMod))
      idList.add(row.idMod);
    if (incdependencies.get(row.idMod) != null) {
      for (final String id : incdependencies.get(row.idMod)) {
        if (!idList.contains(id)) {
          getIncludedModulesInModule(getRow(id).name, idList);
        }
      }
    }
  }

  public boolean isDependant(ModuleRow module, ModuleRow targetModule) {
    if (dependencies.get(module.idMod) == null)
      return false;
    if (dependencies.get(module.idMod).contains(targetModule.idMod))
      return true;
    for (String idDepMod : dependencies.get(module.idMod)) {
      if (isDependant(getRow(idDepMod), targetModule))
        return true;
    }
    return false;
  }

  public void generateIndustryTemplateTree() {
    for (final ModuleRow row : allModules) {
      if (row.type.equalsIgnoreCase("T") && incdependencies.get(row.idMod) == null) {
        // We've found an Industry Template which is not included in any
        // other one.
        // We will find the Industry Templates which are included in it
        recursiveTemplateLoader(row.idMod);
        idTemplates.add(row.idMod);
      }
    }
  }

  private void recursiveTemplateLoader(String idDepTemplate) {
    for (final ModuleRow row : allModules) {
      if (row.type.equalsIgnoreCase("T")) {
        final Vector<String> dep = incdependencies.get(row.idMod);
        if (dep != null && dep.contains(idDepTemplate)) {
          recursiveTemplateLoader(row.idMod);
          idTemplates.add(row.idMod);
        }
      }
    }
  }

  public String getNameOfActiveIndustryTemplate() {
    for (final ModuleRow row : allModules)
      if (row.type.equals("T") && row.isInDevelopment.equals("Y"))
        return row.dir;
    return null;
  }

  private void getDependencyHashMap() {
    for (final ModuleRow row : allModules) {
      getDependencyForPrefix(row.dir, new Vector<String>(), row);
    }
  }

  private void getDependencyForPrefix(String dir, Vector<String> idList, ModuleRow row) {
    if (prefixDependencies.get(dir) == null)
      prefixDependencies.put(dir, new Vector<String>());
    prefixDependencies.get(dir).addAll(row.prefixes);
    idList.add(row.idMod);
    if (dependencies.get(row.idMod) != null) {
      for (final String id : dependencies.get(row.idMod)) {
        if (!idList.contains(id))
          getDependencyForPrefix(dir, idList, getRow(id));
      }
    }

  }

  private void getDependencies(Platform platform) {
    final String query = "SELECT * from AD_MODULE_DEPENDENCY";
    final Connection connection = platform.borrowConnection();
    ResultSet resultSet = null;
    try {
      final PreparedStatement statement = connection.prepareStatement(query);
      statement.execute();
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        final String ad_module_id = readStringFromRS(resultSet, "AD_MODULE_ID");
        final String ad_dependent_module_id = readStringFromRS(resultSet, "AD_DEPENDENT_MODULE_ID");
        if (!dependencies.containsKey(ad_dependent_module_id))
          dependencies.put(ad_dependent_module_id, new Vector<String>());
        dependencies.get(ad_dependent_module_id).add(ad_module_id);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new BuildException("Problem while loading module dependencies.");
    }
  }

  private void getInclusiveDependencies(Platform platform) {
    final String query = "SELECT * from AD_MODULE_DEPENDENCY WHERE ISINCLUDED='Y'";
    final Connection connection = platform.borrowConnection();
    ResultSet resultSet = null;
    try {
      final PreparedStatement statement = connection.prepareStatement(query);
      statement.execute();
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        final String ad_module_id = readStringFromRS(resultSet, "AD_MODULE_ID");
        final String ad_dependent_module_id = readStringFromRS(resultSet, "AD_DEPENDENT_MODULE_ID");
        if (!incdependencies.containsKey(ad_dependent_module_id))
          incdependencies.put(ad_dependent_module_id, new Vector<String>());
        incdependencies.get(ad_dependent_module_id).add(ad_module_id);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new BuildException("Problem while loading module dependencies.");
    }
  }

  public String getDir(String modId) {
    for (final ModuleRow row : allModules)
      if (row.idMod.equalsIgnoreCase(modId))
        return row.dir;
    return null;
  }

  public String getId(String dir) {
    for (final ModuleRow row : allModules) {
      if (row.dir.equalsIgnoreCase(dir))
        return row.idMod;
    }
    return null;
  }

  public ModuleRow getRow(String modId) {
    for (final ModuleRow row : allModules) {
      if (row.idMod.equalsIgnoreCase(modId))
        return row;
    }
    return null;
  }

  public ModuleRow getRowFromName(String name) {
    for (final ModuleRow row : allModules) {
      if (row.name.equalsIgnoreCase(name))
        return row;
    }
    return null;
  }

  public ModuleRow getRowFromDir(String dir) {
    for (final ModuleRow row : allModules) {
      if (row.dir.equalsIgnoreCase(dir))
        return row;
    }
    return null;
  }

  public boolean hasBeenModified(Platform platform, boolean updateCRC) {
    try {
      final Connection connection = platform.borrowConnection();
      PreparedStatement statementDate = connection
          .prepareStatement("SELECT last_dbupdate from AD_SYSTEM_INFO");
      statementDate.execute();
      ResultSet rsDate = statementDate.getResultSet();
      rsDate.next();
      Timestamp date = rsDate.getTimestamp(1);

      System.out.println("Checking if database structure was modified locally.");
      String sql;
      if (updateCRC)
        sql = "SELECT ad_db_modified('Y') FROM DUAL";
      else
        sql = "SELECT ad_db_modified('N') FROM DUAL";

      PreparedStatement statement = connection.prepareStatement(sql);
      statement.execute();
      ResultSet rs = statement.getResultSet();
      rs.next();
      String answer = rs.getString(1);
      if (answer.equalsIgnoreCase("Y"))
        return true;

      System.out.println("Checking if data has changed in the application dictionary.");
      boolean datachange = DataSetService.getInstance().hasChanged(
          DataSetService.getInstance().getDataSetByValue("ADCS"), date);

      if (datachange)
        return true;
      else
        return false;

    } catch (Exception e) {
      e.printStackTrace();
      System.out
          .println("There was a problem verifying the changes in the database. Updating the database anyway...");
    }
    return false;

  }

  public void updateCRC(Platform platform) {
    String sql = "SELECT ad_db_modified('Y') FROM DUAL";
    final Connection connection = platform.borrowConnection();
    try {
      PreparedStatement statement = connection.prepareStatement(sql);
      statement.execute();
    } catch (Exception e) {
      System.out.println("There was a problem updating the CRC in the database.");
      e.printStackTrace();

    } finally {
      platform.returnConnection(connection);
    }
  }

  public void deleteInstallTables(Platform platform, Database database) {
    String sql = "DELETE FROM AD_MODULE_INSTALL";
    String sql1 = "DELETE FROM AD_MODULE_DBPREFIX_INSTALL";
    String sql2 = "DELETE FROM AD_MODULE_DEPENDENCY_INST";
    try {
      final Connection connection = platform.borrowConnection();
      Iterator itCheck = platform.query(database,
          "SELECT * FROM AD_TABLE WHERE LOWER(TABLENAME)='ad_module_install'");
      if (!itCheck.hasNext()) {
        // Install tables do not exist. We shouldn't try to move the module data from them.
        return;
      }
      PreparedStatement statement = connection.prepareStatement(sql);
      statement.execute();
      statement = connection.prepareStatement(sql1);
      statement.execute();
      statement = connection.prepareStatement(sql2);
      statement.execute();
    } catch (Exception e) {
      System.out.println("There was a problem when deleting install table content.");
      e.printStackTrace();
    }
  }

  public void moveModuleDataFromInstTables(Platform platform, Database database,
      String modulePackages) {
    Connection connection = platform.borrowConnection();
    try {
      Iterator itCheck = platform.query(database,
          "SELECT * FROM AD_TABLE WHERE LOWER(TABLENAME)='ad_module_install'");
      if (!itCheck.hasNext()) {
        // Install tables do not exist. We shouldn't try to move the module data from them.
        return;
      }
      final StringTokenizer st = new StringTokenizer(modulePackages, ",");
      Table[] db_prefinst = { database.findTable("AD_MODULE_DBPREFIX_INSTALL") };
      Table[] mod_dep = { database.findTable("AD_MODULE_DEPENDENCY_INST") };
      Table[] mod_inst = { database.findTable("AD_MODULE_INSTALL") };
      Vector<String> moduleIds = new Vector<String>();
      Vector<SqlDynaBean> moduleRows = new Vector<SqlDynaBean>();
      Vector<SqlDynaBean> prefRows = new Vector<SqlDynaBean>();
      Vector<SqlDynaBean> depRows = new Vector<SqlDynaBean>();
      String modPacks = "";
      while (st.hasMoreElements()) {
        final String modPack = st.nextToken().trim();
        if (!modPacks.equals(""))
          modPacks += ",";
        modPacks += "'" + modPack + "'";
      }
      Iterator it = platform.query(database,
          "SELECT * FROM AD_MODULE_INSTALL WHERE JAVAPACKAGE IN (" + modPacks + ")", mod_inst);
      while (it.hasNext()) {
        SqlDynaBean moduleRow = (SqlDynaBean) it.next();
        String moduleId = moduleRow.get("AD_MODULE_ID").toString();
        moduleIds.add(moduleId);
        moduleRows.add(moduleRow);
      }
      for (String moduleId : moduleIds) {
        Iterator itPref = platform.query(database,
            "SELECT * FROM AD_MODULE_DBPREFIX_INSTALL WHERE AD_MODULE_ID='" + moduleId + "'",
            db_prefinst);
        if (itPref.hasNext()) {
          SqlDynaBean prefRow = (SqlDynaBean) itPref.next();
          prefRows.add(prefRow);
        }
        Iterator itDep = platform.query(database,
            "SELECT * FROM AD_MODULE_DEPENDENCY_INST WHERE AD_MODULE_ID='" + moduleId + "'",
            mod_dep);
        while (itDep.hasNext()) {
          SqlDynaBean depRow = (SqlDynaBean) itDep.next();
          depRows.add(depRow);
        }
      }

      for (SqlDynaBean moduleRow : moduleRows) {
        ((SqlDynaClass) moduleRow.getDynaClass()).resetDynaClass(database.findTable("AD_MODULE"));
        platform.updateinsert(connection, database, moduleRow);
      }

      for (SqlDynaBean prefRow : prefRows) {
        ((SqlDynaClass) prefRow.getDynaClass()).resetDynaClass(database
            .findTable("AD_MODULE_DBPREFIX"));
        platform.updateinsert(connection, database, prefRow);
      }
      for (SqlDynaBean depRow : depRows) {
        ((SqlDynaClass) depRow.getDynaClass()).resetDynaClass(database
            .findTable("AD_MODULE_DEPENDENCY"));
        platform.updateinsert(connection, database, depRow);
      }
      for (String moduleId : moduleIds) {
        // Deleting rows in _install tabless
        String sql = "DELETE FROM AD_MODULE_INSTALL WHERE AD_MODULE_ID='" + moduleId + "'";
        String sql1 = "DELETE FROM AD_MODULE_DBPREFIX_INSTALL WHERE AD_MODULE_ID='" + moduleId
            + "'";
        String sql2 = "DELETE FROM AD_MODULE_DEPENDENCY_INST WHERE AD_MODULE_ID='" + moduleId + "'";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.execute();
        statement = connection.prepareStatement(sql1);
        statement.execute();
        statement = connection.prepareStatement(sql2);
        statement.execute();
      }

    } catch (Exception e) {
      System.out.println("Error while moving data from install tables.");
      e.printStackTrace();
    } finally {
      platform.returnConnection(connection);
    }
  }

  public static void writeCheckSumInfo(String obDir) {
    CheckSum cs = new CheckSum(obDir);
    cs.calculateCheckSum("md5.db.all");
  }

  private static String readCheckSumInfo(String obDir) {
    CheckSum cs = new CheckSum(obDir);
    return cs.getCheckSum("md5.db.all");
  }

  public static boolean verifyCheckSum(String obDir) {
    String oldCS = readCheckSumInfo(obDir);
    if ("0".equals(oldCS)) {
      System.out.println("CheckSum value not found in properties file. CheckSum test not done.");
      return true;
    }
    CheckSum cs = new CheckSum(obDir);
    String newCS = cs.calculateCheckSumWithoutSaving("md5.db.all");
    return newCS.equals(oldCS);

  }
}
