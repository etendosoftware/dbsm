/*
 ************************************************************************************
 * Copyright (C) 2001-2020 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.ddlutils.util;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.ColumnDataChange;
import org.apache.ddlutils.alteration.ColumnRequiredChange;
import org.apache.ddlutils.alteration.ColumnSizeChange;
import org.apache.ddlutils.alteration.DataChange;
import org.apache.ddlutils.alteration.ModelChange;
import org.apache.ddlutils.alteration.RemoveCheckChange;
import org.apache.ddlutils.alteration.RemoveIndexChange;
import org.apache.ddlutils.alteration.VersionInfo;
import org.apache.ddlutils.dynabean.SqlDynaBean;
import org.apache.ddlutils.dynabean.SqlDynaClass;
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DataToArraySink;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.apache.ddlutils.platform.ModelBasedResultSetIterator;
import org.apache.log4j.Logger;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.openbravo.ddlutils.task.DatabaseUtils;

public class DBSMOBUtil {

  private Vector<ModuleRow> allModules = new Vector<ModuleRow>();
  private Vector<ModuleRow> activeModules = new Vector<ModuleRow>();
  private HashMap<String, Vector<String>> dependencies = new HashMap<String, Vector<String>>();
  private HashMap<String, Vector<String>> incdependencies = new HashMap<String, Vector<String>>();
  private Vector<String> dirTemplateModules = new Vector<String>();
  private HashMap<String, Vector<String>> prefixDependencies = new HashMap<String, Vector<String>>();
  private Vector<String> idTemplates = new Vector<String>();
  private Vector<String> idModulesToExport = new Vector<String>();
  private List<String> sortedTemplates;

  private static DBSMOBUtil instance = null;

  public static DBSMOBUtil getInstance() {
    if (instance == null) {
      instance = new DBSMOBUtil();
    }
    return instance;
  }

  public static void resetInstance() {
    instance = new DBSMOBUtil();
  }

  public ModuleRow getModule(int i) {
    return allModules.get(i);
  }

  public ModuleRow getActiveModule(int i) {
    return activeModules.get(i);
  }

  public ModuleRow getTemplateModule(int i) {
    final String dirMod = dirTemplateModules.get(i);
    for (final ModuleRow row : allModules) {
      if (row.dir.equalsIgnoreCase(dirMod)) {
        return row;
      }
    }
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
      if (targetRow.type.equalsIgnoreCase("T")) {
        return true;
      }
      for (ModuleRow row : allModules) {
        if (row.type.equalsIgnoreCase("T")) {
          if (isDependant(row, targetRow)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public boolean moduleDependsOnTemplate(String module) {
    ModuleRow targetRow = getRowFromDir(module);
    for (ModuleRow row : allModules) {
      if (row.type.equalsIgnoreCase("T")) {
        if (isDependant(row, targetRow)) {
          return true;
        }
      }
    }
    return false;
  }

  @Deprecated
  public void getModules(Platform platform, String excludeobjects) {
    final ExcludeFilter filter = DatabaseUtils.getExcludeFilter(excludeobjects);
    getLog().warn(
        "The getModules(Platform, String) method is outdated and will not work with modules which add excluded filters to the model. The getModules(Platform, ExcludeFilter) method should be used instead.");
    getModules(platform, filter);
  }

  public void getModules(Platform platform, ExcludeFilter filterO) {
    getDependencies(platform);
    getInclusiveDependencies(platform);
    final String sql = "SELECT * FROM AD_MODULE ORDER BY NAME";

    Connection connection = platform.borrowConnection();
    ResultSet resultSet = null;
    try {
      final PreparedStatement statement = connection.prepareStatement(sql);
      statement.execute();
      resultSet = statement.getResultSet();
      if (resultSet.next()) {
      } else {
        throw new BuildException(
            "Module information not found in database: table AD_MODULE empty.");
      }
    } catch (final Exception e) {
      platform.returnConnection(connection);
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
        final ExcludeFilter filter = filterO.clone();
        for (final String s : allModules.get(i).prefixes) {
          filter.addPrefix(s);
        }
        final Vector<String> otherMods = new Vector<String>();
        final Vector<String> otherActiveMods = new Vector<String>();
        for (int j = 0; j < allModules.size(); j++) {
          if (!allModules.get(j).dir.equalsIgnoreCase(allModules.get(i).dir)) {
            if (allModules.get(j).isInDevelopment != null
                && allModules.get(j).isInDevelopment.equals("Y")) {
              otherActiveMods.addAll(allModules.get(j).prefixes);
            }
            otherMods.addAll(allModules.get(j).prefixes);
          }
        }
        filter.addOthersPrefixes(otherMods);
        filter.addOtherActivePrefixes(otherActiveMods);
        filter.addDependencies(prefixDependencies);
        filter.setName(allModules.get(i).dir);
        for (final ExceptionRow row : allModules.get(i).exceptions) {
          filter.addException(row);
        }
        for (final ExceptionRow row : allModules.get(i).othersexceptions) {
          filter.addOthersException(row);
        }
        allModules.get(i).filter = filter;
      }

    } catch (final Exception e) {
      e.printStackTrace();
      throw new BuildException(
          "Error while trying to fetch module information from database: " + e.getMessage());
    } finally {
      platform.returnConnection(connection);
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

  public static Map<String, String> getModulesVersion(Platform platform) {
    final String sql = "SELECT ad_module_id AS moduleid, version AS version FROM ad_module";
    final Connection connection = platform.borrowConnection();
    ResultSet resultSet = null;
    Map<String, String> moduleVersionMap = new HashMap<String, String>();
    try {
      final PreparedStatement statement = connection.prepareStatement(sql);
      statement.execute();
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        String moduleId = resultSet.getString("moduleid");
        String moduleVersion = resultSet.getString("version");
        moduleVersionMap.put(moduleId, moduleVersion);
      }
      resultSet.close();
    } catch (final Exception e) {
    } finally {
      platform.returnConnection(connection);
    }
    return moduleVersionMap;
  }

  public static Vector<File> loadFilesFromFolder(String folders) {
    final StringTokenizer strTokFol = new StringTokenizer(folders, ",");
    final Vector<File> files = new Vector<File>();

    while (strTokFol.hasMoreElements()) {
      final String folder = strTokFol.nextToken();
      final File[] fileArray = DatabaseUtils.readFileArray(new File(folder));
      for (int i = 0; i < fileArray.length; i++) {
        if (fileArray[i].getName().endsWith(".xml")
            && !fileArray[i].getAbsolutePath().contains("referencedData")) {
          files.add(fileArray[i]);
        }
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
    if (!idList.contains(tempId)) {
      idList.add(tempId);
    }
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
    if (!idList.contains(row.idMod)) {
      idList.add(row.idMod);
    }
    if (incdependencies.get(row.idMod) != null) {
      for (final String id : incdependencies.get(row.idMod)) {
        if (!idList.contains(id)) {
          getIncludedModulesInModule(getRow(id).dir, idList);
        }
      }
    }
  }

  public boolean isDependant(ModuleRow module, ModuleRow targetModule) {
    if (dependencies.get(module.idMod) == null) {
      return false;
    }
    if (dependencies.get(module.idMod).contains(targetModule.idMod)) {
      return true;
    }
    for (String idDepMod : dependencies.get(module.idMod)) {
      if (isDependant(getRow(idDepMod), targetModule)) {
        return true;
      }
    }
    return false;
  }

  public void checkTemplateExportIsPossible(Logger log) {
    int numTemplatesInDevelopment = 0;
    ModuleRow rowT = null;
    for (final ModuleRow row : allModules) {
      if (row.type.equals("T") && row.isInDevelopment.equals("Y")) {
        numTemplatesInDevelopment++;
        rowT = row;
      }
    }
    if (numTemplatesInDevelopment == 0) {
      log.error("Error: no Industry Template in development.");
      System.exit(1);
    } else if (numTemplatesInDevelopment > 1) {
      log.error("Error: more than one Industry Template in development.");
      System.exit(1);
    }
    for (final ModuleRow row : allModules) {
      if ("T".equals(row.type) && isDependant(rowT, row)) {
        log.error(
            "The Industry Template being developed is not the last Industry Template in the hierarchy. An Industry Template can only be exported when no other Industry Templates depend on it.");
        System.exit(1);
      }
    }
  }

  public String getNameOfActiveIndustryTemplate() {
    for (final ModuleRow row : allModules) {
      if (row.type.equals("T") && row.isInDevelopment.equals("Y")) {
        return row.dir;
      }
    }
    return null;
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
        if (!dependencies.containsKey(ad_dependent_module_id)) {
          dependencies.put(ad_dependent_module_id, new Vector<String>());
        }
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
        if (!incdependencies.containsKey(ad_dependent_module_id)) {
          incdependencies.put(ad_dependent_module_id, new Vector<String>());
        }
        incdependencies.get(ad_dependent_module_id).add(ad_module_id);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new BuildException("Problem while loading module dependencies.");
    }
  }

  public String getDir(String modId) {
    for (final ModuleRow row : allModules) {
      if (row.idMod.equalsIgnoreCase(modId)) {
        return row.dir;
      }
    }
    return null;
  }

  public String getId(String dir) {
    for (final ModuleRow row : allModules) {
      if (row.dir.equalsIgnoreCase(dir)) {
        return row.idMod;
      }
    }
    return null;
  }

  public ModuleRow getRow(String modId) {
    for (final ModuleRow row : allModules) {
      if (row.idMod.equalsIgnoreCase(modId)) {
        return row;
      }
    }
    return null;
  }

  public ModuleRow getRowFromName(String name) {
    for (final ModuleRow row : allModules) {
      if (row.name.equalsIgnoreCase(name)) {
        return row;
      }
    }
    return null;
  }

  public ModuleRow getRowFromDir(String dir) {
    for (final ModuleRow row : allModules) {
      if (row.dir.equalsIgnoreCase(dir)) {
        return row;
      }
    }
    return null;
  }

  /**
   * @deprecated platform and updateCRC are not used at all. Replaced by
   *             {@link #hasBeenModified(OBDataset)} method.
   */
  @Deprecated
  public boolean hasBeenModified(Platform platform, OBDataset dataset, boolean updateCRC) {
    return hasBeenModified(dataset);
  }

  public boolean hasBeenModified(OBDataset dataset) {
    Connection connection = null;
    try {
      connection = getUnpooledConnection();
      // Execute the session config query before calling ad_db_modified, the same way it is done
      // when using the Module Management Console
      executeSessionConfigQuery(connection);

      getLog().info("Checking if database structure was modified locally.");
      if (databaseHasChanges()) {
        return true;
      }

      getLog().info("Checking if data has changed in the application dictionary.");
      return dataset.hasChanged(connection, new ArrayList<>());

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(
          "There was a problem verifying the changes in the database. Updating the database anyway...");
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        getLog().error("Error while closing connection", e);
      }
    }
    return false;
  }

  /**
   * @deprecated platform it is not used at all. Replaced by {@link #updateCRC()} method.
   */
  @Deprecated
  public void updateCRC(Platform platform) {
    updateCRC();
  }

  public void updateCRC() {
    Connection connection = null;
    try {
      connection = getUnpooledConnection();
      // Execute the session config query before calling ad_db_modified, the same way it is done
      // when using the Module Management Console
      executeSessionConfigQuery(connection);
      String sql = "SELECT ad_db_modified('Y') FROM DUAL";
      PreparedStatement statement = connection.prepareStatement(sql);
      statement.execute();
    } catch (Exception e) {
      System.out.println("There was a problem updating the CRC in the database.");
      e.printStackTrace();
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        getLog().error("Error while closing connection", e);
      }
    }
  }

  /**
   * This method checks if database has changes by invoking ad_db_modified function. In any case the
   * checksum is not updated.
   * 
   * @return true if database structure have been modified.
   */
  public boolean databaseHasChanges() {
    Connection connection = null;
    boolean hasBeenChanges = false;
    try {
      connection = getUnpooledConnection();
      PreparedStatement statement = isAdDbModified(connection);
      ResultSet rs = statement.getResultSet();
      rs.next();
      String answer = rs.getString(1);
      if (answer.equalsIgnoreCase("Y")) {
        hasBeenChanges = true;
      }
    } catch (Exception e) {
      getLog().error("There was a problem checking the CRC in the database.", e);
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        getLog().error("Error while closing connection", e);
      }
    }
    return hasBeenChanges;
  }

  private PreparedStatement isAdDbModified(Connection connection) throws SQLException {
    String sql = "SELECT ad_db_modified('N') FROM DUAL";
    PreparedStatement statement = connection.prepareStatement(sql);
    statement.execute();
    return statement;
  }

  private Connection getUnpooledConnection() {
    Connection connection = null;
    try {
      Properties obProps = getOpenbravoProperties();
      String strURL = obProps.getProperty("bbdd.url");
      if (obProps.getProperty("bbdd.rdbms").equalsIgnoreCase("POSTGRE")) {
        strURL += "/" + obProps.getProperty("bbdd.sid");
      }
      connection = DriverManager.getConnection(strURL, obProps.getProperty("bbdd.user"),
          obProps.getProperty("bbdd.password"));
    } catch (SQLException e) {
      getLog().error("Error while retrieving an unpooled connection: ", e);
    }
    return connection;
  }

  public void deleteInstallTables(Platform platform, Database database) {
    String sql = "DELETE FROM AD_MODULE_INSTALL";
    String sql1 = "DELETE FROM AD_MODULE_DBPREFIX_INSTALL";
    String sql2 = "DELETE FROM AD_MODULE_DEPENDENCY_INST";
    final Connection connection = platform.borrowConnection();
    try {
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
      ((ModelBasedResultSetIterator) itCheck).cleanUp();
    } catch (Exception e) {
      System.out.println("There was a problem when deleting install table content.");
      e.printStackTrace();
    } finally {
      platform.returnConnection(connection);
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
      ((ModelBasedResultSetIterator) itCheck).cleanUp();
      Table[] db_prefinst = { database.findTable("AD_MODULE_DBPREFIX_INSTALL") };
      Table[] mod_dep = { database.findTable("AD_MODULE_DEPENDENCY_INST") };
      Table[] mod_inst = { database.findTable("AD_MODULE_INSTALL") };
      Vector<String> moduleIds = new Vector<String>();
      Vector<SqlDynaBean> moduleRows = new Vector<SqlDynaBean>();
      Vector<SqlDynaBean> prefRows = new Vector<SqlDynaBean>();
      Vector<SqlDynaBean> depRows = new Vector<SqlDynaBean>();
      String modPacks = "";
      String sqlmodule = "SELECT * FROM AD_MODULE_INSTALL ";
      if (modulePackages != null) {
        final StringTokenizer st = new StringTokenizer(modulePackages, ",");
        while (st.hasMoreElements()) {
          final String modPack = st.nextToken().trim();
          if (!modPacks.equals("")) {
            modPacks += ",";
          }
          modPacks += "'" + modPack + "'";
        }
        sqlmodule += "WHERE JAVAPACKAGE IN (" + modPacks + ")";
      }
      Iterator it = platform.query(database, sqlmodule, mod_inst);
      while (it.hasNext()) {
        SqlDynaBean moduleRow = (SqlDynaBean) it.next();
        String moduleId = moduleRow.get("AD_MODULE_ID").toString();
        moduleIds.add(moduleId);
        moduleRows.add(moduleRow);
      }
      ((ModelBasedResultSetIterator) it).cleanUp();
      for (String moduleId : moduleIds) {
        Iterator itPref = platform.query(database,
            "SELECT * FROM AD_MODULE_DBPREFIX_INSTALL WHERE AD_MODULE_ID='" + moduleId + "'",
            db_prefinst);
        if (itPref.hasNext()) {
          SqlDynaBean prefRow = (SqlDynaBean) itPref.next();
          prefRows.add(prefRow);
        }
        ((ModelBasedResultSetIterator) itPref).cleanUp();
        Iterator itDep = platform.query(database,
            "SELECT * FROM AD_MODULE_DEPENDENCY_INST WHERE AD_MODULE_ID='" + moduleId + "'",
            mod_dep);
        while (itDep.hasNext()) {
          SqlDynaBean depRow = (SqlDynaBean) itDep.next();
          depRows.add(depRow);
        }
        ((ModelBasedResultSetIterator) itDep).cleanUp();
      }

      for (SqlDynaBean moduleRow : moduleRows) {
        ((SqlDynaClass) moduleRow.getDynaClass())
            .resetDynaClassWithoutMissingProperties(database.findTable("AD_MODULE"));
        platform.updateinsert(connection, database, moduleRow);
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
    org.openbravo.utils.CheckSum cs = new org.openbravo.utils.CheckSum(obDir);
    cs.calculateCheckSum("md5.db.all");
  }

  private static String readCheckSumInfo(String obDir) {
    org.openbravo.utils.CheckSum cs = new org.openbravo.utils.CheckSum(obDir);
    return cs.getCheckSum("md5.db.all");
  }

  public static boolean verifyCheckSum(String obDir) {
    String oldCS = readCheckSumInfo(obDir);
    if ("0".equals(oldCS)) {
      System.out.println("CheckSum value not found in properties file. CheckSum test not done.");
      return true;
    }
    org.openbravo.utils.CheckSum cs = new org.openbravo.utils.CheckSum(obDir);
    String newCS = cs.calculateCheckSumWithoutSaving("md5.db.all");
    return newCS.equals(oldCS);

  }

  public static BasicDataSource getDataSource(String driver, String url, String user,
      String password) {
    BasicDataSource ds = new BasicDataSource();
    ds.setDriverClassName(driver);
    ds.setUrl(url);
    ds.setUsername(user);
    ds.setPassword(password);
    if (driver.contains("Oracle")) {
      ds.setValidationQuery("SELECT 1 FROM DUAL");
    } else {
      ds.setValidationQuery("SELECT 1");
    }
    ds.setTestOnBorrow(true);
    ds.setMaxWait(10000);

    return ds;
  }

  public static void setStatus(Connection connection, int status, Logger log) {
    try {
      PreparedStatement ps = connection
          .prepareStatement("UPDATE ad_system_info SET system_status=?");
      ps.setString(1, "RB" + status);
      ps.executeUpdate();
      PreparedStatement ps2 = connection.prepareStatement(
          "DELETE FROM ad_error_log where system_status=(select system_status from ad_system_info)");
      ps2.executeUpdate();
    } catch (Exception e) {
      log.warn("Couldn't update system status");
    }
  }

  public static void setStatus(Platform platform, int status, Logger log) {
    Connection connection = platform.borrowConnection();
    setStatus(connection, status, log);
    platform.returnConnection(connection);
  }

  public List<String> getSortedTemplates(DatabaseData databaseData) {
    allModules = new Vector<ModuleRow>();
    dependencies = new HashMap<String, Vector<String>>();
    Vector<DynaBean> moduleDBs = databaseData.getRowsFromTable("AD_MODULE");
    if (moduleDBs == null) {
      moduleDBs = new Vector<DynaBean>();
    }
    Vector<DynaBean> moduleDepDBs = databaseData.getRowsFromTable("AD_MODULE_DEPENDENCY");
    if (moduleDepDBs == null) {
      moduleDepDBs = new Vector<DynaBean>();
    }

    for (DynaBean mod : moduleDBs) {
      final ModuleRow modRow = new ModuleRow();
      // modRow.value=readStringFromRS(resultSet, "DB_PREFIX");
      modRow.prefixes = new Vector<String>();
      modRow.exceptions = new Vector<ExceptionRow>();
      modRow.othersexceptions = new Vector<ExceptionRow>();
      modRow.name = readPropertyFromDynaBean(mod, "NAME");
      modRow.isInDevelopment = readPropertyFromDynaBean(mod, "ISINDEVELOPMENT");
      modRow.dir = readPropertyFromDynaBean(mod, "JAVAPACKAGE");
      modRow.idMod = readPropertyFromDynaBean(mod, "AD_MODULE_ID");
      modRow.type = readPropertyFromDynaBean(mod, "TYPE");
      allModules.add(modRow);
    }
    for (DynaBean dep : moduleDepDBs) {
      final String ad_module_id = readPropertyFromDynaBean(dep, "AD_MODULE_ID");
      final String ad_dependent_module_id = readPropertyFromDynaBean(dep, "AD_DEPENDENT_MODULE_ID");
      if (!dependencies.containsKey(ad_dependent_module_id)) {
        dependencies.put(ad_dependent_module_id, new Vector<String>());
      }
      dependencies.get(ad_dependent_module_id).add(ad_module_id);
    }

    Vector<ModuleRow> templates = new Vector<ModuleRow>();
    for (int i = 0; i < allModules.size(); i++) {
      if (allModules.get(i).type.equals("T")) {
        templates.add(allModules.get(i));
      }
    }
    Vector<String> sortedTemplates = new Vector<String>();
    while (templates.size() > 0) {
      // We try to add one template
      ModuleRow template = null;
      for (int i = 0; i < templates.size(); i++) {
        if (!moduleDependsOnTemplate(templates.get(i).dir)) {
          template = templates.get(i);
          break;
        }
      }
      if (template == null) {
        System.out.println("Circular dependency found when loading configuration scripts.");
        break;
      } else {
        sortedTemplates.add(template.dir);
        template.type = "M";
        templates.remove(template);
      }
    }
    return sortedTemplates;
  }

  private static String readPropertyFromDynaBean(DynaBean db, String property) {
    return db.get(property) == null ? null : db.get(property).toString();
  }

  public void loadDataStructures(Platform platform, DatabaseData databaseOrgData,
      Database originaldb, Database db, String modulesBaseDir, String datafilter, File input) {
    loadDataStructures(platform, databaseOrgData, originaldb, db, modulesBaseDir, datafilter, input,
        false);
  }

  public void loadDataStructures(Platform platform, DatabaseData databaseOrgData,
      Database originaldb, Database db, String modulesBaseDir, String datafilter, File input,
      boolean strict) {
    loadDataStructures(platform, databaseOrgData, originaldb, db, modulesBaseDir, datafilter, input,
        strict, true);
  }

  public void loadDataStructures(Platform platform, DatabaseData databaseOrgData,
      Database originaldb, Database db, String modulesBaseDir, String datafilter, File input,
      boolean strict, boolean applyConfigScriptData) {

    loadDataStructures(databaseOrgData, db, new String[] {modulesBaseDir}, datafilter, input);
  }

  public void loadDataStructures(DatabaseData databaseOrgData, Database db, String[] modulesBaseDirList,
      String datafilter, File input) {
    getLog().debug("loadDataStructures - dirs to scan " + Arrays.toString(modulesBaseDirList));
    final Vector<File> files = new Vector<File>();
    File[] sourceFiles = input.listFiles();
    for (int i = 0; i < sourceFiles.length; i++) {
      if (sourceFiles[i].isFile() && sourceFiles[i].getName().endsWith(".xml")) {
        files.add(sourceFiles[i]);
      }
    }

    if (modulesBaseDirList != null) {
      final String token = datafilter;
      for (String modulesBaseDir : modulesBaseDirList) {
        final DirectoryScanner dirScanner = new DirectoryScanner();
        dirScanner.setBasedir(new File(modulesBaseDir));
        final String[] dirFilterA = token.split(",");
        dirScanner.setIncludes(dirFilterA);
        dirScanner.scan();
        final String[] incDirs = dirScanner.getIncludedDirectories();
        for (int j = 0; j < incDirs.length; j++) {
          final File dirFolder = new File(modulesBaseDir, incDirs[j] + "/");
          final File[] fileArray = DatabaseUtils.readFileArray(dirFolder);
          for (int i = 0; i < fileArray.length; i++) {
            if (fileArray[i].getName().endsWith(".xml")) {
              files.add(fileArray[i]);
            }
          }
        }
      }
    }
    getLog().debug("loadDataStructures - files to read " + Arrays.toString(files.toArray()));
    readDataIntoDatabaseData(db, databaseOrgData, files);
  }

  public void readDataIntoDatabaseData(Database db, DatabaseData databaseOrgData,
      List<File> files) {

    final DatabaseDataIO dbdio = new DatabaseDataIO();
    dbdio.setEnsureFKOrder(false);
    final DataReader dataReader = dbdio.getConfiguredCompareDataReader(db);
    getLog().info("Loading data from XML files");
    for (int i = 0; i < files.size(); i++) {
      try {
        getLog().debug("Parsing file " + files.get(i).getAbsolutePath());
        dataReader.getSink().start();
        final String tablename = files.get(i)
            .getName()
            .substring(0, files.get(i).getName().length() - 4);
        final Vector<DynaBean> vectorDynaBeans = ((DataToArraySink) dataReader.getSink())
            .getVector();
        dataReader.parse(files.get(i));
        databaseOrgData.insertDynaBeansFromVector(tablename, vectorDynaBeans);
        dataReader.getSink().end();
      } catch (final Exception e) {
        getLog().error("Error while parsing file: " + files.get(i).getAbsolutePath());
        e.printStackTrace();
      }
    }
  }

  public void applyConfigScripts(Platform platform, DatabaseData databaseOrgData, Database db,
      String modulesBaseDir, boolean strict, boolean applyConfigScriptData) {

    // This will update the modules dir
    ModulesUtil.checkCoreInSources(ModulesUtil.coreInSources());

    // This dir should be the root of the project
    String rootWorkDir = ModulesUtil.getProjectRootDir();

    getLog().info("Working dir 'applyConfigScripts': " + rootWorkDir);
    getLog().info("Loading and applying configuration scripts");
    sortedTemplates = DBSMOBUtil.getInstance().getSortedTemplates(databaseOrgData);
    for (String template : sortedTemplates) {
      boolean isApplied = isApplied(platform, template);
      if (isApplied) {
        if (applyConfigScriptData) {
          getLog().info("Applying data part of configuration script: " + template);
        } else {
          getLog().info("Applying structure part of configuration script: " + template);
        }
      }

      File configScript = null;

      getLog().info("Checking template for 'applyConfigScripts': " + template);
      for (String moduleDir : ModulesUtil.moduleDirs) {
        configScript = new File(new File(rootWorkDir + "/" + moduleDir),
                template + "/src-db/database/configScript.xml");
        if(configScript.exists()) {
          break;
        }
      }

      if (configScript != null) {
        applyConfigScript(configScript, platform, databaseOrgData, db, strict, applyConfigScriptData,
                isApplied);
      }
    }
  }

  public void applyConfigScripts(List<String> scripts, Platform platform,
      DatabaseData databaseOrgData, Database db) {
    for (String script : scripts) {
      applyConfigScript(new File(script), platform, databaseOrgData, db, true, true, true);
    }

  }

  private void applyConfigScript(File configScript, Platform platform, DatabaseData databaseOrgData,
      Database db, boolean strict, boolean applyConfigScriptData, boolean isApplied) {
    if (!configScript.exists()) {
      getLog().info(
          "Couldn't find configuration script for template: " + configScript.getAbsolutePath());
      return;
    }
    DatabaseIO dbIO = new DatabaseIO();
    getLog().info("Loading configuration script: " + configScript.getAbsolutePath());
    Vector<Change> changes = dbIO.readChanges(configScript);
    boolean isOldConfigScript = isOldConfigScript(changes);
    boolean isOB3 = isOB3(platform);
    for (Change change : changes) {
      if (change instanceof ModelChange) {
        ((ModelChange) change).apply(db, platform.isDelimitedIdentifierModeOn());
      } else if (change instanceof DataChange && applyConfigScriptData && isApplied) {
        if (!isOldConfigScript || !isOB3 || isValidChange(change)) {
          boolean applied = ((DataChange) change).apply(databaseOrgData,
              platform.isDelimitedIdentifierModeOn());
          if (strict && !applied) {
            throw new BuildException("Change " + change
                + " of the configuration script for the template " + configScript.getAbsolutePath()
                + " could't be applied, and as the configuration script is being applied in 'strict' mode, the process has been stopped. You can now either execute the task with '-Dstrict.template.application=no', or fix the configuration script (by either changing it manually or reexporting it with an updated environment).");
          }
        } else {
          System.out.println(
              "no -> " + change + " ->" + (!isOldConfigScript || !isOB3 || isValidChange(change)));
        }
      }
      getLog().debug(change);
    }
  }

  public static boolean isApplied(Platform platform, String template) {
    Connection con = null;
    try {
      con = platform.borrowConnection();
      PreparedStatement ps = con.prepareStatement(
          "SELECT ISCONFIGSCRIPTAPPLIED FROM AD_MODULE WHERE JAVAPACKAGE='" + template + "'");
      ps.execute();
      ResultSet rs = ps.getResultSet();
      rs.next();
      String st = rs.getString(1);
      if (st.equalsIgnoreCase("N")) {
        return false;
      } else {
        return true;
      }
    } catch (Exception e) {
      // In case we cannot read the column (maybe it doesn't exist)
      // we will apply the template
      return true;
    } finally {
      if (con != null) {
        platform.returnConnection(con);
      }
    }
  }

  public void removeSortedTemplates(Platform platform, DatabaseData databaseOrgData,
      String basedir) {
    for (int i = sortedTemplates.size() - 1; i >= 0; i--) {
      boolean isindevelopment = false;
      for (int j = 0; j < activeModules.size(); j++) {
        if (activeModules.get(j).dir.equals(sortedTemplates.get(i))) {
          isindevelopment = true;
        }
      }
      if (isindevelopment) {
        continue;
      }
      if (!isApplied(platform, sortedTemplates.get(i))) {
        continue;
      }
      File configScript = new File(new File(basedir),
          "/" + sortedTemplates.get(i) + "/src-db/database/configScript.xml");
      if (configScript.exists()) {
        DatabaseIO dbIO = new DatabaseIO();
        getLog().debug(
            "Reversing data part of configuration script: " + configScript.getAbsolutePath());
        Vector<Change> changes = dbIO.readChanges(configScript);
        boolean isOldConfigScript = isOldConfigScript(changes);
        boolean isOB3 = isOB3(platform);
        for (Change change : changes) {
          if (change instanceof ColumnDataChange) {
            getLog().debug("Reversing change " + change);
            if (!isOldConfigScript || !isOB3 || isValidChange(change)) {
              boolean applied = ((ColumnDataChange) change).applyInReverse(databaseOrgData,
                  platform.isDelimitedIdentifierModeOn());
              if (!applied) {
                throw new BuildException("Reversing the change " + ((ColumnDataChange) change)
                    + " in configuration script for template " + sortedTemplates.get(i)
                    + " couldn't be applied, and therefore, the export.database couldn't be completed. Fix (or remove) the configuration script, and then try the export again.");
              }
              getLog().debug(change);
            }
          }
        }
      } else {
        getLog().info("Couldn't find configuration script for template: " + sortedTemplates.get(i)
            + " (file: " + configScript.getAbsolutePath() + ")");
      }
    }
  }

  public void removeSortedTemplates(Platform platform, Database database, String basedir) {
    DatabaseData dbData = new DatabaseData(database);
    removeSortedTemplates(platform, database, dbData, basedir);
  }

  public void removeSortedTemplates(Platform platform, Database database,
      DatabaseData databaseOrgData, String basedir) {
    if (sortedTemplates == null) {
      sortedTemplates = getSortedTemplates(databaseOrgData);
    }
    for (int i = sortedTemplates.size() - 1; i >= 0; i--) {
      boolean isindevelopment = false;
      for (int j = 0; j < activeModules.size(); j++) {
        if (activeModules.get(j).dir.equals(sortedTemplates.get(i))) {
          isindevelopment = true;
        }
      }
      if (isindevelopment) {
        continue;
      }
      if (!isApplied(platform, sortedTemplates.get(i))) {
        continue;
      }
      File configScript = new File(new File(basedir),
          "/" + sortedTemplates.get(i) + "/src-db/database/configScript.xml");
      if (configScript.exists()) {
        DatabaseIO dbIO = new DatabaseIO();
        getLog().info(
            "Reversing model part of configuration script: " + configScript.getAbsolutePath());
        Vector<Change> changes = dbIO.readChanges(configScript);
        for (Change change : changes) {
          if (change instanceof RemoveCheckChange) {
            ((RemoveCheckChange) change).applyInReverse(database, false);
          } else if (change instanceof ColumnSizeChange) {
            ((ColumnSizeChange) change).applyInReverse(database, false);
          } else if (change instanceof ColumnRequiredChange) {
            ((ColumnRequiredChange) change).applyInReverse(database, false);
          } else if (change instanceof RemoveIndexChange) {
            ((RemoveIndexChange) change).applyInReverse(database, false);
          }
          getLog().debug(change);
        }
      } else {
        getLog().info("Couldn't find configuration script for template: " + sortedTemplates.get(i)
            + " (file: " + configScript.getAbsolutePath() + ")");
      }
    }
    for (int tableIdx = 0; tableIdx < database.getTableCount(); tableIdx++) {
      database.getTable(tableIdx).sortIndices(false);
      database.getTable(tableIdx).sortChecks(false);
    }
  }

  public ExcludeFilter getExcludeFilter(File rootDir) {
    ExcludeFilter ex = new ExcludeFilter();
    try {

      getLog().info("Exclude filter rootDir: " + rootDir.getAbsolutePath());
      /**
       * When the core is in JAR the rootDir should be in '/build/etendo'.
       * Loads the filter in the 'src-db' dir.
       */
      ex.fillFromFile(new File(rootDir, "src-db/database/model/excludeFilter.xml"));

      // Update dirs where find the modules
      ModulesUtil.checkCoreInSources(ModulesUtil.coreInSources());

      File auxRootDir = rootDir;

      // Core in JAR
      if (!ModulesUtil.coreInSources) {
        auxRootDir = new File(ModulesUtil.getProjectRootDir());
      }

      for (String moduleDir : ModulesUtil.moduleDirs) {
        File f = new File(auxRootDir, moduleDir);
        File[] mods = f.listFiles();
        if (mods != null) {
          for (File mod : mods) {
            File fex = new File(mod, "src-db/database/model/excludeFilter.xml");
            if (fex.exists()) {
              ex.fillFromFile(fex);
            }
          }
        }
      }
    } catch (Exception e) {
      getLog().error("Error while reading excludeFilter file", e);
    }
    return ex;
  }

  private Logger getLog() {
    return Logger.getLogger(getClass());
  }

  private static boolean isOldConfigScript(Vector<Change> changes) {
    for (Change change : changes) {
      if (change instanceof VersionInfo) {
        String version = ((VersionInfo) change).getVersion();
        return !isOB3Version(version);
      }
    }
    return true;
  }

  private static boolean isOB3Version(String version) {
    int versionN = Integer.parseInt(version.substring(0, 1));
    if (versionN >= 3) {
      return true;
    } else {
      return false;
    }
  }

  private boolean isOB3(Platform platform) {
    String version = getOBVersion(platform);

    if (version == null) {
      return false;
    }
    return isOB3Version(version);
  }

  public String getOBVersion(Platform platform) {

    Connection con = null;
    try {
      con = platform.borrowConnection();
      PreparedStatement ps = con
          .prepareStatement("SELECT VERSION FROM AD_MODULE WHERE AD_MODULE_ID='0'");
      ps.execute();
      ResultSet rs = ps.getResultSet();
      rs.next();
      String st = rs.getString(1);
      return st;
    } catch (Exception e) {
      return null;
    } finally {
      if (con != null) {
        platform.returnConnection(con);
      }
    }
  }

  private boolean isValidChange(Change change) {
    if (!(change instanceof ColumnDataChange)) {
      return true;
    }
    return !(((ColumnDataChange) change).getColumnname().equals("SEQNO")
        && ((ColumnDataChange) change).getTablename().equalsIgnoreCase("AD_FIELD"));
  }

  /**
   * Returns true if the config script was exported in an Openbravo version earlier than OB 3 MP0
   * 
   * @return
   */
  public static boolean isOldConfigScript(File file) {
    DatabaseIO dbIO = new DatabaseIO();
    Vector<Change> changes = dbIO.readChanges(file);
    return isOldConfigScript(changes);
  }

  /**
   * Executes the sessionConfig query defined in Openbravo.properties
   * 
   * @param connection
   */
  private void executeSessionConfigQuery(Connection connection) {
    try {
      Properties props = getOpenbravoProperties();
      String sessionConfigQuery = props.getProperty("bbdd.sessionConfig");
      PreparedStatement statement = connection.prepareStatement(sessionConfigQuery);
      statement.execute();
    } catch (Exception e) {
      System.out.println("Error while executing the sessionConfig query");
      e.printStackTrace();
    }
  }

  private static Properties getOpenbravoProperties() {
    Properties props = new Properties();
    String workingDir = System.getProperty("user.dir");
    try {
      // the workingDir can be either the openbravoRoot or openbravoRoot/src-db/database
      File propertiesFile = new File(workingDir + "/config/Openbravo.properties");
      if (!propertiesFile.exists()) {
        propertiesFile = new File(workingDir + "/../../config/Openbravo.properties");
      }

      /**
       * The core is in jar
       */
      if (!propertiesFile.exists()) {
        propertiesFile = new File (ModulesUtil.getProjectRootDir() + "/config/Openbravo.properties");
      }

      props.load(new FileInputStream(propertiesFile));
    } catch (Exception e) {
      System.out.println("Error while obtaining the Openbravo.properties file");
      e.printStackTrace();
    }
    return props;
  }
}
