/*
 ************************************************************************************
 * Copyright (C) 2001-2006 Openbravo S.L.U.
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
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.ddlutils.io.DatabaseFilter;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.io.DynamicDatabaseFilter;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.apache.log4j.Logger;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;

public class DatabaseUtils {
  private static final Logger log = Logger.getLogger(DatabaseUtils.class);

  /** Creates a new instance of DatabaseUtils */
  private DatabaseUtils() {
  }

  public static Database readDatabase(File f) {

    Database d = readDatabase_noChecks(f);
    try {
      d.initialize();
    } catch (Exception e) {
      System.out.println("Warning: " + e.getMessage());
    }
    return d;
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

  public static Database readDatabase(File[] f) {

    Database d = readDatabase_noChecks(f[0]);
    for (int i = 1; i < f.length; i++) {
      d.mergeWith(readDatabase_noChecks(f[i]));
    }
    d.initialize();
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

    if (f.isDirectory()) {

      ArrayList<File> fileslist = new ArrayList<File>();

      File[] directoryfiles = f.listFiles(new XMLFiles());
      for (File file : directoryfiles) {
        File[] ff = readFileArray(file);
        for (File fileint : ff) {
          fileslist.add(fileint);
        }
      }

      return fileslist.toArray(new File[fileslist.size()]);
    } else {
      return new File[] { f };
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
  static Database readDatabaseModel(File model, String basedir, String dirFilter) {
    Database db = null;
    if (basedir == null) {
      log.info("Basedir for additional files not specified. Updating database with just Core.");
      db = DatabaseUtils.readDatabase(model);
    } else {
      // We read model files using the filter, obtaining a file array.
      // The models will be merged
      // to create a final target model.
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
      db = DatabaseUtils.readDatabase(fileArray);
    }
    return db;
  }

}
