package org.openbravo.ddlutils.task;

import java.io.File;
import java.io.StringWriter;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DataToArraySink;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;

public class CompareData extends BaseDatabaseTask {

  private File model;
  private String data;
  private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

  private File prescript = null;
  private File postscript = null;

  private String filter = "org.apache.ddlutils.io.NoneDatabaseFilter";

  private File originalmodel;
  private boolean failonerror = false;

  private String object = null;

  @Override
  public void doExecute() {

    final BasicDataSource ds = new BasicDataSource();
    ds.setDriverClassName(getDriver());
    ds.setUrl(getUrl());
    ds.setUsername(getUser());
    ds.setPassword(getPassword());

    final Platform platform = PlatformFactory.createNewPlatformInstance(ds);

    getLog().info("Loading model from XML files");
    final Database originaldb = DatabaseUtils.readDatabase(getModel());

    final DatabaseDataIO dbdio = new DatabaseDataIO();
    dbdio.setEnsureFKOrder(false);
    dbdio.setDatabaseFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), originaldb));

    DataReader dataReader = null;
    dataReader = dbdio.getConfiguredCompareDataReader(originaldb);

    final String folders = getData();

    final StringTokenizer strTokFol = new StringTokenizer(folders, ",");

    final Vector<File> files = new Vector<File>();

    while (strTokFol.hasMoreElements()) {
      final String folder = strTokFol.nextToken();
      final File[] fileArray = DatabaseUtils.readFileArray(new File(folder));
      for (int i = 0; i < fileArray.length; i++) {
        files.add(fileArray[i]);
      }
    }
    getLog().info("Loading data from XML files");
    final DatabaseData databaseData = new DatabaseData(originaldb);
    for (int i = 0; i < files.size(); i++) {
      try {
        dataReader.getSink().start();
        final String tablename = files.get(i).getName().substring(0,
            files.get(i).getName().length() - 4);
        final Vector<DynaBean> vectorDynaBeans = ((DataToArraySink) dataReader.getSink())
            .getVector();
        dataReader.parse(files.get(i));
        databaseData.insertDynaBeansFromVector(tablename, vectorDynaBeans);
        dataReader.getSink().end();
      } catch (final Exception e) {
        System.out.println(e.getLocalizedMessage());
      }
    }

    getLog().info("Loading model from current database");
    final Database currentdb = platform.loadModelFromDatabase(DatabaseUtils
        .getExcludeFilter(excludeobjects));

    final DataComparator dataComparator = new DataComparator(platform.getSqlBuilder()
        .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
    dataComparator.setFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), originaldb));
    dataComparator.compare(originaldb, currentdb, platform, databaseData);

    final Vector<Change> changes = dataComparator.getChanges();
    for (int i = 0; i < changes.size(); i++)
      System.out.println(changes.get(i));
    System.out.println("=======================");
    try {
      final StringWriter stringWriter = new StringWriter();
      platform.getSqlBuilder().setWriter(stringWriter);
      platform.getSqlBuilder().processDataChanges(currentdb, databaseData, changes);
      System.out.println(stringWriter.toString());
    } catch (final Exception e) {
      System.out.println("oops: " + e.getMessage());
    }

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

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
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
}