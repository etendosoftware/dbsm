package org.openbravo.ddlutils.task;

import java.io.File;
import java.sql.Connection;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.AddFunctionChange;
import org.apache.ddlutils.alteration.AddSequenceChange;
import org.apache.ddlutils.alteration.AddTriggerChange;
import org.apache.ddlutils.alteration.AddViewChange;
import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.ColumnOnCreateDefaultValueChange;
import org.apache.ddlutils.alteration.DataChange;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.alteration.ModelChange;
import org.apache.ddlutils.alteration.RemoveFunctionChange;
import org.apache.ddlutils.alteration.RemoveTriggerChange;
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DataToArraySink;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.io.DatabaseFilter;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;
import org.openbravo.ddlutils.util.DBSMOBUtil;

public class AlterCustomizedDatabaseData extends BaseDatabaseTask {

  private String excludeobjects = "org.apache.ddlutils.platform.ExcludeFilter";

  private File model;
  private File orgModel;
  private String data;
  private String orgData;

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

    getLog().info("Loading original model from XML files");
    final Database originaldb = DatabaseUtils.readDatabase(getOrgModel());

    final DatabaseDataIO dbdio = new DatabaseDataIO();
    dbdio.setEnsureFKOrder(false);
    dbdio.setDatabaseFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), originaldb));

    DataReader dataReader = dbdio.getConfiguredCompareDataReader(originaldb);

    final Vector<File> files = DBSMOBUtil.loadFilesFromFolder(getOrgData());

    getLog().info("Loading original data from XML files");
    final DatabaseData databaseOrgData = new DatabaseData(originaldb);
    for (int i = 0; i < files.size(); i++) {
      try {
        dataReader.getSink().start();
        final String tablename = files.get(i).getName().substring(0,
            files.get(i).getName().length() - 4);
        final Vector<DynaBean> vectorDynaBeans = ((DataToArraySink) dataReader.getSink())
            .getVector();
        dataReader.parse(files.get(i));
        databaseOrgData.insertDynaBeansFromVector(tablename, vectorDynaBeans);
        dataReader.getSink().end();
      } catch (final Exception e) {
        System.out.println(e.getLocalizedMessage());
      }
    }

    getLog().info("Loading model from current database");
    final Database currentdb = platform.loadModelFromDatabase(DatabaseUtils
        .getExcludeFilter(excludeobjects));

    Database currentcloneddb = null;

    try {
      currentcloneddb = (Database) currentdb.clone();
    } catch (final Exception e) {
      getLog().info("Model not cloned: " + e.getMessage());
      currentcloneddb = currentdb;
    }

    getLog().info("Finding customizations made to the database");
    final DataComparator dataComparator = new DataComparator(platform.getSqlBuilder()
        .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
    dataComparator.setFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), currentdb));
    dataComparator.compare(originaldb, currentdb, platform, databaseOrgData);

    final Vector<Change> dataCustomization = dataComparator.getChanges();
    for (int i = 0; i < dataCustomization.size(); i++)
      System.out.println(dataCustomization.get(i));
    System.out.println("=======================");

    getLog().info("Loading new version model from XML files");

    final Database newDb = DatabaseUtils.readDatabase(getModel());

    final DatabaseDataIO dbdioNew = new DatabaseDataIO();
    dbdioNew.setEnsureFKOrder(false);
    dbdioNew.setDatabaseFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), newDb));

    final DataReader dataReaderNew = dbdio.getConfiguredCompareDataReader(newDb);

    final Vector<File> filesNew = DBSMOBUtil.loadFilesFromFolder(getData());

    getLog().info("Loading new version data from XML files");
    final DatabaseData databaseNewData = new DatabaseData(newDb);
    for (int i = 0; i < filesNew.size(); i++) {
      try {
        dataReaderNew.getSink().start();
        final String tablename = filesNew.get(i).getName().substring(0,
            filesNew.get(i).getName().length() - 4);
        final Vector<DynaBean> vectorDynaBeans = ((DataToArraySink) dataReaderNew.getSink())
            .getVector();
        dataReaderNew.parse(filesNew.get(i));
        databaseNewData.insertDynaBeansFromVector(tablename, vectorDynaBeans);
        dataReaderNew.getSink().end();

      } catch (final Exception e) {
        System.out.println(e.getLocalizedMessage());
      }
    }

    getLog().info("Filtering customizations");

    final List modelChanges = dataComparator.getModelChangesList();
    for (int i = 0; i < modelChanges.size(); i++) {
      if (modelChanges.get(i) instanceof RemoveTriggerChange) {
        boolean cont = false;
        final RemoveTriggerChange removeTrigger = (RemoveTriggerChange) modelChanges.get(i);
        for (int j = 0; !cont && j < modelChanges.size(); j++) {

          if (modelChanges.get(j) instanceof AddTriggerChange
              && removeTrigger.getTrigger().getName().equalsIgnoreCase(
                  ((AddTriggerChange) modelChanges.get(j)).getNewTrigger().getName())) {
            final AddTriggerChange addTrigger = (AddTriggerChange) modelChanges.get(j);
            modelChanges.remove(addTrigger);
            modelChanges.remove(removeTrigger);
            i--;
            i--;
            cont = true;
          }
        }
      } else if (modelChanges.get(i) instanceof RemoveFunctionChange) {
        boolean cont = false;
        final RemoveFunctionChange removeFunction = (RemoveFunctionChange) modelChanges.get(i);
        for (int j = 0; !cont && j < modelChanges.size(); j++) {

          if (modelChanges.get(j) instanceof AddFunctionChange
              && removeFunction.getFunction().getName().equalsIgnoreCase(
                  ((AddFunctionChange) modelChanges.get(j)).getNewFunction().getName())) {
            final AddFunctionChange addFunction = (AddFunctionChange) modelChanges.get(j);
            modelChanges.remove(addFunction);
            modelChanges.remove(removeFunction);
            i--;
            i--;
            cont = true;
          }
        }
      } else if (modelChanges.get(i) instanceof AddViewChange) {
        final AddViewChange addView = (AddViewChange) modelChanges.get(i);
        if (newDb.findView(addView.getNewView().getName()) != null) {
          modelChanges.remove(addView);
        }
      } else if (modelChanges.get(i) instanceof AddSequenceChange) {
        modelChanges.remove(modelChanges.get(i));
        i--;
      } else if (modelChanges.get(i) instanceof ColumnOnCreateDefaultValueChange) {
        modelChanges.remove(modelChanges.get(i));
        i--;
      }
    }

    getLog().info("Applying model customizations to the new version");

    final Iterator itModelChanges = dataComparator.getModelChanges();
    while (itModelChanges.hasNext()) {
      final ModelChange modelChange = (ModelChange) itModelChanges.next();
      try {
        modelChange.apply(newDb, platform.isDelimitedIdentifierModeOn());
      } catch (final Exception e) {
        getLog().error("Couldn't apply customization: " + modelChange);
      }
    }

    getLog().info("Applying data customizations to the new version");
    final Iterator itDataChanges = dataCustomization.iterator();
    while (itDataChanges.hasNext()) {
      ((DataChange) itDataChanges.next()).apply(databaseNewData, platform
          .isDelimitedIdentifierModeOn());
    }

    /*
     * getLog().info("Comparing customized new version to existing database") ;
     * 
     * DataComparator finalDataComparator=new DataComparator(platform.getSqlBuilder
     * ().getPlatformInfo(),platform.isDelimitedIdentifierModeOn()); dataComparator
     * .setFilter(DatabaseUtils.getDynamicDatabaseFilter(getFilter(), originaldb));
     * dataComparator.compare(currentdb, newDb, platform, databaseNewData);
     */
    getLog().info("Updating existing database");

    final Connection connection = platform.borrowConnection();
    try {

      // execute the pre-script
      if (getPrescript() == null) {
        // try to execute the default prescript
        final File fpre = new File(getModel(), "prescript-" + platform.getName() + ".sql");
        if (fpre.exists()) {
          getLog().info("Executing default prescript");
          platform.evaluateBatch(DatabaseUtils.readFile(fpre), true);
        }
      } else {
        platform.evaluateBatch(DatabaseUtils.readFile(getPrescript()), true);
      }

      getLog().info("Updating database model");
      platform.alterTables(currentdb, newDb, !isFailonerror());

      getLog().info("Updating database data");
      dataReader = dbdio.getConfiguredCompareDataReader(newDb);
    } catch (final Exception e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
    // dataReader.getSink().start();

    getLog().debug("Disabling foreign keys");
    platform.disableAllFK(connection, currentdb, !isFailonerror());
    getLog().debug("Disabling triggers");
    platform.disableAllTriggers(connection, newDb, !isFailonerror());
    final DatabaseFilter filter = DatabaseUtils.getDynamicDatabaseFilter(getFilter(), originaldb);
    if (filter != null) {
      final String[] tablenames = filter.getTableNames();
      final String[] tableFilters = new String[tablenames.length];
      int ind = 0;
      for (final String table : tablenames) {
        tableFilters[ind] = filter.getTableFilter(table);
        ind++;
      }
      platform.deleteDataFromTable(connection, newDb, tablenames, tableFilters, !isFailonerror());

    }
    // StringWriter stringWriter=new StringWriter();
    // platform.getSqlBuilder().setWriter(stringWriter);
    for (int i = 0; i < newDb.getTableCount(); i++) {
      final Table table = newDb.getTable(i);
      getLog().debug("Inserting data from table " + table.getName());
      final Vector<DynaBean> rowsTable = databaseNewData.getRowsFromTable(table.getName());
      if (rowsTable != null) {
        for (int j = 0; j < rowsTable.size(); j++) {
          final DynaBean row = rowsTable.get(j);
          try {
            platform.upsert(connection, newDb, row);
          } catch (final Exception e) {
            getLog().info("Error. Row " + row + " couldn't be inserted. " + e.getMessage());
          }
        }
      }
    }
    getLog().debug("Removing invalid rows.");
    platform.deleteInvalidConstraintRows(newDb, !isFailonerror());
    getLog().debug("Executing update final script (NOT NULLs and dropping temporal tables");
    platform.alterTablesPostScript(currentcloneddb, newDb, !isFailonerror(), null, null);
    getLog().debug("Enabling Foreign Keys and Triggers");
    platform.enableAllFK(connection, currentdb, !isFailonerror());
    platform.enableAllTriggers(connection, newDb, !isFailonerror()); // <-
    // we
    // use
    // currentdb
    // so
    // that
    // we
    // don't
    // try
    // to
    // activate
    // FKs
    // that
    // have
    // not
    // been
    // created
    // yet.
    try {
      connection.close();
      if (getPostscript() == null) {
        // try to execute the default prescript
        final File fpost = new File(getModel(), "postscript-" + platform.getName() + ".sql");
        if (fpost.exists()) {
          getLog().info("Executing default postscript");
          platform.evaluateBatch(DatabaseUtils.readFile(fpost), true);
        }
      } else {
        platform.evaluateBatch(DatabaseUtils.readFile(getPostscript()), true);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      System.out.println("Exception: " + e.getMessage());
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

}