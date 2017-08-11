/*
 ************************************************************************************
 * Copyright (C) 2001-2017 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.ddlutils.task;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.io.DatabaseFilter;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.io.DynamicDatabaseFilter;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.apache.log4j.Logger;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.ddlutils.util.DBSMOBUtil;

public class DatabaseUtils {

  private static final Logger log = Logger.getLogger(DatabaseUtils.class);

  private static final String SOURCEDATA_PATH = "/src-db/database/sourcedata/";
  private static final String AD_MODULE_NAME_FILE = "AD_MODULE.xml";
  private static final String AD_MODULE_DEP_NAME_FILE = "AD_MODULE_DEPENDENCY.xml";

  /** Creates a new instance of DatabaseUtils */
  private DatabaseUtils() {
  }

  /**
   * Read the model from the XML.This method is required to maintain backwards compatibility and to
   * ensures that the API is not broken.
   */
  public static Database readDatabase(File f) {
    Platform platform = createPlatform();
    return readDatabase(f, platform, System.getProperty("user.dir"), true, false, true, false);
  }

  /**
   * Read the model and apply the configScripts if proceed in order to have a model with the
   * supported modifications defined in any configScript.
   * 
   * @param file
   *          The file to be loaded as a database model.
   * @param platform
   *          it is used to performing queries and manipulations into the database-related.
   * @param basedir
   *          it is a complete path to the base directory.
   * @param strict
   *          if it is true and a DataChange is not applied properly, an exception is raised .
   * @param applyConfigScriptData
   *          true if data part (DataChange) of the configScripts should be applied.
   * @param applyConfigScript
   *          true if configScripts should be applied into the model.
   * @param loadModelFromXML
   *          true if database is not mounted and information should be obtained from XML files.
   */
  public static Database readDatabase(File file, Platform platform, String basedir, boolean strict,
      boolean applyConfigScriptData, boolean applyConfigScript, boolean loadModelFromXML) {

    Database d = readDatabase_noChecks(file);
    try {
      d.initialize();
    } catch (Exception e) {
      System.out.println("Warning: " + e.getMessage());
    }
    if (applyConfigScript) {
      return applyConfigScriptsIntoModel(platform, basedir, strict, applyConfigScriptData, d,
          loadModelFromXML);
    }
    return d;
  }

  /**
   * ConfigScripts are applied taking into account which templates are active. This information
   * could be obtain from database or XML.
   */
  private static Database applyConfigScriptsIntoModel(Platform platform, String basedir,
      boolean strict, boolean applyConfigScriptData, Database d, boolean readFromXML) {
    final DatabaseData databaseOrgDataPartialModel = new DatabaseData(d);
    if (readFromXML) {
      readDataModuleInfo(d, databaseOrgDataPartialModel, basedir);
    } else {
      final DBSMOBUtil util = DBSMOBUtil.getInstance();
      ExcludeFilter excludeFilter = DBSMOBUtil.getInstance().getExcludeFilter(
          new File(getValidBasedir(basedir)));
      util.getModules(platform, excludeFilter);
      util.generateIndustryTemplateTree();
    }
    DBSMOBUtil.getInstance().applyConfigScripts(platform, databaseOrgDataPartialModel, d,
        getValidBasedir(basedir), strict, applyConfigScriptData);
    return d;
  }

  /**
   * This method ensures that path should be sources basedir instead of modules basedir.
   */
  private static String getValidBasedir(String path) {
    if (path.endsWith("modules/")) {
      log.info("Basedir " + path + "is updated properly to " + path.concat("../"));
      return path.concat("../");
    }
    return path;
  }

  /**
   * Read the data for AD_MODULE and AD_MODULE_DEPENDENCY from XML to be able to apply all the
   * configScripts defined in the template modules when it isn't exists a database yet: Install
   * source task,...
   */
  private static void readDataModuleInfo(Database d, DatabaseData dbdata, String path) {
    log.debug("Loading data for AD_MODULE and AD_MODULE_DEPENDENCY from XML files");
    Vector<File> dirs = new Vector<File>();
    addModuleFilesIfExists(dirs, path);

    File modules = new File(path, "/modules");
    for (File moduleDir : modules.listFiles()) {
      addModuleFilesIfExists(dirs, moduleDir.getAbsolutePath());
    }

    DBSMOBUtil.getInstance().readDataIntoDatabaseData(d, dbdata, dirs);
  }

  private static void addModuleFilesIfExists(Vector<File> dirs, String path) {
    addFileIfExists(dirs, path, AD_MODULE_NAME_FILE);
    addFileIfExists(dirs, path, AD_MODULE_DEP_NAME_FILE);
  }

  /**
   * This method adds a valid file to the vector dirs. All the files added will be used to read data
   * from the files.
   * 
   * @param dirs
   *          is used to storage all valid files.
   * @param path
   *          in where file is located.
   * @param nameFile
   *          name of the target file.
   */
  private static void addFileIfExists(Vector<File> dirs, String path, String nameFile) {
    final File file = new File(path, SOURCEDATA_PATH + nameFile);
    if (file.exists()) {
      dirs.add(file);
    }
  }

  public static Database readDatabaseNoInit(File f) {

    Database d = readDatabase_noChecks(f);
    return d;
  }

  public static Database readDatabaseNoInit(File[] f) {

    Database d = readDatabase_noChecks(f[0]);
    for (int i = 1; i < f.length; i++) {
      d.mergeWith(readDatabase_noChecks(f[i]));
    }

    return d;
  }

  /**
   * Read the model from the XML.This method is required to maintain backwards compatibility and to
   * ensures that the API is not broken.
   */
  public static Database readDatabase(File[] f) {
    Platform platform = createPlatform();
    String sourcePath = OBPropertiesProvider.getInstance().getOpenbravoProperties()
        .getProperty("source.path");
    return readDatabase(f, platform, sourcePath, true, false, true, false);
  }

  private static Platform createPlatform() {
    Properties obProp = OBPropertiesProvider.getInstance().getOpenbravoProperties();
    String driver = obProp.getProperty("bbdd.driver");
    String url = obProp.getProperty("bbdd.rdbms").equals("POSTGRE") ? obProp
        .getProperty("bbdd.url") + "/" + obProp.getProperty("bbdd.sid") : obProp
        .getProperty("bbdd.url");
    String user = obProp.getProperty("bbdd.user");
    String password = obProp.getProperty("bbdd.password");
    BasicDataSource datasource = DBSMOBUtil.getDataSource(driver, url, user, password);
    Platform platform = PlatformFactory.createNewPlatformInstance(datasource);
    return platform;
  }

  /**
   * Read the model and apply the configScripts if proceed in order to have a model with the
   * supported modifications defined in any configScript.
   * 
   * @param file
   *          The files to be loaded as a database model.
   * @param platform
   *          it is used to performing queries and manipulations into the database-related.
   * @param basedir
   *          it is a complete path to the base directory.
   * @param strict
   *          if it is true and a DataChange is not applied properly, an exception is raised .
   * @param applyConfigScriptData
   *          true if data part (DataChange) of the configScripts should be applied.
   * @param applyConfigScript
   *          true if configScripts should be applied into the model.
   * @param loadModelFromXML
   *          true if database is not mounted and information should be obtained from XML files.
   */
  public static Database readDatabase(File[] f, Platform platform, String basedir, boolean strict,
      boolean applyConfigScriptData, boolean applyConfigScript, boolean loadModelFromXML) {

    Database d = readDatabase_noChecks(f[0]);
    for (int i = 1; i < f.length; i++) {
      d.mergeWith(readDatabase_noChecks(f[i]));
    }
    d.initialize();
    if (applyConfigScript) {
      return applyConfigScriptsIntoModel(platform, basedir, strict, applyConfigScriptData, d,
          loadModelFromXML);
    }
    return d;
  }

  public static void saveDatabase(File f, Database model) {

    try {

      Writer w = new OutputStreamWriter(new FileOutputStream(f), "UTF-8");
      new DatabaseIO().write(model, w);
      w.flush();
      w.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static File[] readFileArray(File f) {
    return readFileArrayOfType(f, new XMLFiles());
  }

  public static File[] readCopyFileArray(File f) {
    return readFileArrayOfType(f, new CopyFiles());
  }

  private static File[] readFileArrayOfType(File folder, FileFilter filter) {
    if (folder.isDirectory()) {
      ArrayList<File> fileslist = new ArrayList<File>();
      File[] directoryfiles = folder.listFiles(filter);
      for (File file : directoryfiles) {
        File[] ff = readFileArray(file);
        for (File fileint : ff) {
          fileslist.add(fileint);
        }
      }
      return fileslist.toArray(new File[fileslist.size()]);
    } else {
      return new File[] { folder };
    }
  }

  private static Database readDatabase_noChecks(File f) {

    if (f.isDirectory()) {
      if (f.getAbsolutePath().contains(".svn") || f.getAbsolutePath().contains(".hg")) {
        return new Database();
      } else {
        // create an empty database
        Database d = new Database();
        d.setName(f.getName());

        // gets the list and sort
        File[] filelist = f.listFiles(new XMLFiles());
        Arrays.sort(filelist, new FilesComparator());

        for (File file : filelist) {
          d.mergeWith(readDatabase_noChecks(file));
        }

        return d;
      }
    } else {
      if (f.getName().equals("excludeFilter.xml")) {
        return new Database();
      }
      DatabaseIO dbIO = new DatabaseIO();
      dbIO.setValidateXml(false);
      Database db = dbIO.readplain(f);
      if (f.getAbsolutePath().contains("modifiedTables"))
        db.moveTablesToModified();
      return db;
    }
  }

  public static Database cropDatabase(Database sourcedb, Database targetdb, String sobjectlist) {

    Database cropdb = null;
    try {
      cropdb = (Database) sourcedb.clone();

      StringTokenizer st = new StringTokenizer(sobjectlist, ",");

      while (st.hasMoreTokens()) {
        moveObject(cropdb, targetdb, st.nextToken().trim());
      }

    } catch (CloneNotSupportedException e) {
    }

    cropdb.initialize(); // throws an exception if inconsistent
    return cropdb;
  }

  public static void moveObject(Database cropdb, Database targetdb, String sobject) {

    cropdb.removeTable(cropdb.findTable(sobject));
    cropdb.addTable(targetdb.findTable(sobject));

    cropdb.removeSequence(cropdb.findSequence(sobject));
    cropdb.addSequence(targetdb.findSequence(sobject));

    cropdb.removeView(cropdb.findView(sobject));
    cropdb.addView(targetdb.findView(sobject));

    cropdb.removeFunction(cropdb.findFunction(sobject));
    cropdb.addFunction(targetdb.findFunction(sobject));

    cropdb.removeTrigger(cropdb.findTrigger(sobject));
    cropdb.addTrigger(targetdb.findTrigger(sobject));
  }

  public static String readFile(File f) throws IOException {

    StringBuffer s = new StringBuffer();
    BufferedReader br = new BufferedReader(new FileReader(f));

    String line;
    while ((line = br.readLine()) != null) {
      s.append(line);
      s.append('\n');
    }
    br.close();
    return s.toString();
  }

  /**
   * Needs to be kept as external code in module org.openbravo.ezattributes is calling it
   */
  @Deprecated
  public static DatabaseFilter getDynamicDatabaseFilter(String filter, Database database) {
    try {
      DynamicDatabaseFilter dbfilter = (DynamicDatabaseFilter) Class.forName(filter).newInstance();
      dbfilter.init(database);
      return dbfilter;
    } catch (InstantiationException ex) {
      throw new BuildException(ex);
    } catch (IllegalAccessException ex) {
      throw new BuildException(ex);
    } catch (ClassNotFoundException ex) {
      throw new BuildException(ex);
    }
  }

  @Deprecated
  public static ExcludeFilter getExcludeFilter(String filtername) {
    try {
      return (ExcludeFilter) Class.forName(filtername).newInstance();
    } catch (InstantiationException ex) {
      throw new BuildException(ex);
    } catch (IllegalAccessException ex) {
      throw new BuildException(ex);
    } catch (ClassNotFoundException ex) {
      throw new BuildException(ex);
    }
  }

  private static class XMLFiles implements FileFilter {
    public boolean accept(File pathname) {
      return pathname.isDirectory() || (pathname.isFile() && pathname.getName().endsWith(".xml"));
    }
  }

  private static class CopyFiles implements FileFilter {
    public boolean accept(File pathname) {
      return pathname.isDirectory() || (pathname.isFile() && pathname.getName().endsWith(".copy"));
    }
  }

  private static class FilesComparator implements Comparator<File> {
    public int compare(File a, File b) {

      if (a.isDirectory() && !b.isDirectory()) {
        return -1;
      } else if (!a.isDirectory() && b.isDirectory()) {
        return 1;
      } else {
        return a.getName().compareTo(b.getName());
      }
    }
  }

  /**
   * Reads the model recursively from core + modules.
   * 
   * @param model
   *          should point to <erpBaseDir>/src-db/database/model
   * @param basedir
   *          <erpBaseDir>/modules
   * @param dirFilter
   *          i.e. \*\/src-db/database/model
   * @return Database object representing the loaded model
   */
  // TODO: centralize other copies in update.database (+xml) also into DatabaseUtils
  static Database readDatabaseModel(Platform platform, File model, String basedir, String dirFilter) {

    if (basedir == null) {
      log.info("Basedir for additional files not specified. Updating database with just Core.");
      return DatabaseUtils.readDatabase(model, platform, basedir, true, true, false, false);
    }

    // We read model files using the filter, obtaining a file array. The models will be merged to
    // create a final target model.
    final Vector<File> dirs = new Vector<File>();
    dirs.add(model);
    final DirectoryScanner dirScanner = new DirectoryScanner();
    dirScanner.setBasedir(new File(basedir));
    final String[] dirFilterA = { dirFilter };
    dirScanner.setIncludes(dirFilterA);
    dirScanner.scan();
    final String[] incDirs = dirScanner.getIncludedDirectories();
    for (int j = 0; j < incDirs.length; j++) {
      final File dirF = new File(basedir, incDirs[j]);
      dirs.add(dirF);
    }
    final File[] fileArray = new File[dirs.size()];
    for (int i = 0; i < dirs.size(); i++) {
      fileArray[i] = dirs.get(i);
    }
    return DatabaseUtils.readDatabase(fileArray, platform, basedir, true, true, false, false);
  }
}
