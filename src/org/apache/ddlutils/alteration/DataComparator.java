package org.apache.ddlutils.alteration;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.dynabean.SqlDynaClass;
import org.apache.ddlutils.dynabean.SqlDynaProperty;
import org.apache.ddlutils.io.DatabaseFilter;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;
import org.openbravo.base.model.Property;
import org.openbravo.base.structure.BaseOBObject;
import org.openbravo.dal.core.DalUtil;
import org.openbravo.model.ad.utility.DataSet;
import org.openbravo.model.ad.utility.DataSetColumn;
import org.openbravo.model.ad.utility.DataSetTable;
import org.openbravo.service.dataset.DataSetService;

public class DataComparator {

  /** The log for this comparator. */
  private final Log _log = LogFactory.getLog(ModelComparator.class);

  /** The platform information. */
  private PlatformInfo _platformInfo;
  /** Whether comparison is case sensitive. */
  private boolean _caseSensitive;
  private Vector<Change> dataChanges = new Vector<Change>();
  private DatabaseFilter _databasefilter = null;
  private List modelChanges;
  private DataSet dataset;
  private HashMap<String, DataSetTable> tableMap = null;
  private HashMap<String, HashMap<String, DataSetColumn>> columnMap = null;

  public DataComparator(PlatformInfo platformInfo, boolean caseSensitive) {
    _platformInfo = platformInfo;
    _caseSensitive = caseSensitive;
  }

  public void setFilter(DatabaseFilter filter) {
    _databasefilter = filter;
  }

  public void compareUsingDAL(Database originaldb, Database currentdb, Platform platform,
      DatabaseData originalData, String datasetName, String moduleIds) {
    ModelComparator modelComparator = new ModelComparator(_platformInfo, _caseSensitive);
    modelChanges = modelComparator.compare(originaldb, currentdb);

    // First, we will find the common tables
    Vector<Table> commonTables = new Vector<Table>();
    Vector<Table> newTables = new Vector<Table>();

    Table[] tables = currentdb.getTables();

    DataSetService service = DataSetService.getInstance();
    dataset = service.getDataSetByValue(datasetName);
    List<DataSetTable> tableList = dataset.getDataSetTableList();
    tableMap = new HashMap<String, DataSetTable>();
    columnMap = new HashMap<String, HashMap<String, DataSetColumn>>();
    for (DataSetTable table : tableList) {
      tableMap.put(table.getTable().getDBTableName().toUpperCase(), table);
      HashMap<String, DataSetColumn> columnsT = new HashMap<String, DataSetColumn>();
      List<DataSetColumn> columnList = table.getDataSetColumnList();
      for (DataSetColumn column : columnList)
        columnsT.put(column.getColumn().getDBColumnName().toUpperCase(), column);
      columnMap.put(table.getTable().getDBTableName().toUpperCase(), columnsT);
    }

    String[] tablenames = new String[tableList.size()];
    for (int i = 0; i < tableList.size(); i++)
      tablenames[i] = tableList.get(i).getTable().getDBTableName();

    for (int i = 0; i < tables.length; i++) {
      boolean include = true;
      if (tablenames != null) {
        include = false;
        int j = 0;
        while (j < tablenames.length && !tablenames[j].equalsIgnoreCase(tables[i].getName()))
          j++;
        if (j < tablenames.length)
          include = true;
      }
      if (include) {
        commonTables.add(tables[i]);
      }
    }
    for (int i = 0; i < modelChanges.size(); i++) {
      if (modelChanges.get(i) instanceof AddTableChange) {
        Table table = ((AddTableChange) modelChanges.get(i)).getNewTable();
        commonTables.remove(table);
        newTables.add(table);
      }
    }

    // Now we will compare tables. If tables are equivalent in both models,
    // we will compare rows.
    for (int i = 0; i < commonTables.size(); i++) {
      Table table = commonTables.get(i);
      _log.info("Comparing table: " + table.getName());
      try {

        if (table.getPrimaryKeyColumns() == null || table.getPrimaryKeyColumns().length == 0) {
          _log.warn("Error: Table " + table.getName()
              + " could not be compared because it doesn't have primary key.");
          continue;
        }
        int indC = 0;
        Vector<ModelChange> changesToTable = new Vector<ModelChange>();
        while (indC < modelChanges.size()) {
          ModelChange change = (ModelChange) modelChanges.get(indC);
          if (change instanceof AddColumnChange) {
            if (((AddColumnChange) change).getChangedTable().getName().equalsIgnoreCase(
                table.getName()))
              changesToTable.add(change);
          } else if (change instanceof RemoveColumnChange) {
            if (((RemoveColumnChange) change).getChangedTable().getName().equalsIgnoreCase(
                table.getName())) {
              changesToTable.add(change);
            }
          }
          indC++;
        }
        if (!changesToTable.isEmpty()) {
          for (indC = 0; indC < changesToTable.size(); indC++) {
            ModelChange change = changesToTable.get(indC);
            if (change instanceof RemoveColumnChange) {
              // A column has been deleted in the database. We
              // have to
              // delete it from the original model
              // because if not we will have an error
              _log
                  .debug("A column has been deleted in the database and we have to delete it in our original model.");
              // ((RemoveColumnChange)change).apply(originaldb,
              // false);
            } else if (change instanceof AddColumnChange) {
              // We will read all the values of the new column,
              // and
              // add them
              // as ColumnDataChanges
              Iterator answer = null;

              Table tableC = ((AddColumnChange) change).getChangedTable();
              Column columnC = ((AddColumnChange) change).getNewColumn();
              answer = readRowsFromTableDAL(tableMap.get(tableC.getName().toUpperCase()), moduleIds);
              while (answer != null && answer.hasNext()) {
                BaseOBObject db = (BaseOBObject) answer.next();
                List<Property> exportableProperties = service.getExportableProperties(db, tableMap
                    .get(tableC.getName()), tableMap.get(tableC.getName()).getDataSetColumnList());
                Object value = null;
                for (Property property : exportableProperties)
                  if (property.getColumnName().equalsIgnoreCase(columnC.getName()))
                    value = db.get(property.getName());
                if (value != null) {
                  String val;
                  if (db.get(columnC.getName()) == null)
                    val = null;
                  else
                    val = db.get(columnC.getName()).toString();
                  dataChanges.add(new ColumnDataChange(tableC, columnC, null, val, db.getId()));
                } else
                  _log.warn("Column " + columnC.getName() + " of table " + tableC.getName()
                      + " wasn't exported because it wasn't exportable.");
              }
            }
          }
        }

        // Tables can now be compared.
        Iterator answer = null;
        answer = readRowsFromTableDAL(tableMap.get(table.getName().toUpperCase()), moduleIds);
        Vector<DynaBean> rowsOriginalData = originalData.getRowsFromTable(table.getName());
        // We now have the rows of the table in the database (answer)
        // and the rows in the XML files (HashMap originalData)
        compareTablesDAL(originaldb, table, rowsOriginalData, answer);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    for (int i = 0; i < newTables.size(); i++) {
      Table table = newTables.get(i);
      Connection connection = platform.borrowConnection();
      Iterator answer = null;
      answer = readRowsFromTableDAL(tableMap.get(table.getName().toUpperCase()), moduleIds);
      while (answer != null && answer.hasNext()) {
        // Each row of a new table is a new row
        BaseOBObject db = (BaseOBObject) answer.next();
        dataChanges.add(new AddRowDALChange(table, db));
      }
      try {
        if (!connection.isClosed())
          connection.close();
      } catch (Exception ex) {
        // _log.error(ex.getLocalizedMessage());
      }

    }
    Collections.sort(dataChanges, new ChangeComparator());
  }

  public void compareUsingDALToUpdate(Database newdb, Platform platform, DatabaseData originalData,
      String datasetName, String moduleIds) {
    // ModelComparator modelComparator = new ModelComparator(_platformInfo,
    // _caseSensitive);
    // modelChanges = modelComparator.compare(currentdb, newdb);

    DataSetService service = DataSetService.getInstance();
    dataset = service.getDataSetByValue(datasetName);
    if (dataset == null) {
      _log.error("Error: dataset " + datasetName + " not found in database.");
      return;
    }
    List<DataSetTable> tableList = dataset.getDataSetTableList();
    tableMap = new HashMap<String, DataSetTable>();
    columnMap = new HashMap<String, HashMap<String, DataSetColumn>>();
    for (DataSetTable table : tableList) {
      tableMap.put(table.getTable().getDBTableName().toUpperCase(), table);
      HashMap<String, DataSetColumn> columnsT = new HashMap<String, DataSetColumn>();
      List<DataSetColumn> columnList = table.getDataSetColumnList();

      for (DataSetColumn column : columnList)
        columnsT.put(column.getColumn().getDBColumnName().toUpperCase(), column);
      columnMap.put(table.getTable().getDBTableName().toUpperCase(), columnsT);
    }

    String[] tablenames = new String[tableList.size()];
    for (int i = 0; i < tableList.size(); i++)
      tablenames[i] = tableList.get(i).getTable().getDBTableName();

    for (DataSetTable table : tableList) {
      _log.info("Comparing table " + table.getTable().getName());
      // Tables can now be compared.
      Vector<DynaBean> rowsOriginalData = originalData.getRowsFromTable(table.getTable()
          .getDBTableName());
      // We now have the rows of the table in the database (answer)
      // and the rows in the XML files (HashMap originalData)
      compareTablesDALForUpdate(newdb, table, newdb.findTable(table.getTable().getDBTableName()),
          service.getExportableObjects(table, moduleIds), rowsOriginalData);
    }

    Collections.sort(dataChanges, new ChangeComparator());

  }

  private class ChangeComparator implements Comparator<Change> {
    public int compare(Change o1, Change o2) {
      if (o1 instanceof RemoveRowChange && o2 instanceof AddRowChange)
        return -1;
      else if (o1 instanceof RemoveRowDALChange && o2 instanceof AddRowChange)
        return -1;
      else if (o1 instanceof RemoveRowDALChange && o2 instanceof AddRowDALChange)
        return -1;
      else if (o1 instanceof RemoveRowChange && o2 instanceof AddRowDALChange)
        return -1;
      else if (o1 instanceof AddRowChange && o2 instanceof RemoveRowChange)
        return 1;
      else if (o1 instanceof AddRowDALChange && o2 instanceof RemoveRowChange)
        return 1;
      else if (o1 instanceof AddRowChange && o2 instanceof RemoveRowDALChange)
        return 1;
      else if (o1 instanceof AddRowDALChange && o2 instanceof RemoveRowDALChange)
        return 1;
      else if (o1 instanceof RemoveRowChange && o2 instanceof ColumnDataChange)
        return -1;
      else if (o1 instanceof RemoveRowDALChange && o2 instanceof ColumnDataChange)
        return -1;
      else if (o1 instanceof AddRowChange && o2 instanceof ColumnDataChange)
        return 1;
      else if (o1 instanceof AddRowDALChange && o2 instanceof ColumnDataChange)
        return 1;
      else
        return 0;
    }
  }

  public void compare(Database originaldb, Database currentdb, Platform platform,
      DatabaseData originalData) {
    ModelComparator modelComparator = new ModelComparator(_platformInfo, _caseSensitive);
    modelChanges = modelComparator.compare(originaldb, currentdb);

    // First, we will find the common tables
    Vector<Table> commonTables = new Vector<Table>();
    Vector<Table> newTables = new Vector<Table>();

    Table[] tables = currentdb.getTables();

    String[] tablenames = null;
    if (_databasefilter != null)
      tablenames = _databasefilter.getTableNames();
    for (int i = 0; i < tables.length; i++) {
      boolean include = true;
      if (tablenames != null) {
        include = false;
        int j = 0;
        while (j < tablenames.length && !tablenames[j].equals(tables[i].getName()))
          j++;
        if (j < tablenames.length)
          include = true;
      }
      if (include) {
        commonTables.add(tables[i]);
      }
    }
    for (int i = 0; i < modelChanges.size(); i++) {
      if (modelChanges.get(i) instanceof AddTableChange) {
        Table table = ((AddTableChange) modelChanges.get(i)).getNewTable();
        commonTables.remove(table);
        newTables.add(table);
      }
    }

    // Now we will compare tables. If tables are equivalent in both models,
    // we will compare rows.
    for (int i = 0; i < commonTables.size(); i++) {
      Table table = commonTables.get(i);
      if (table.getPrimaryKeyColumns() == null || table.getPrimaryKeyColumns().length == 0) {
        _log.warn("Error: Table " + table.getName()
            + " could not be compared because it doesn't have primary key.");
        continue;
      }
      int indC = 0;
      Vector<ModelChange> changesToTable = new Vector<ModelChange>();
      while (indC < modelChanges.size()) {
        ModelChange change = (ModelChange) modelChanges.get(indC);
        if (change instanceof AddColumnChange) {
          if (((AddColumnChange) change).getChangedTable().getName().equalsIgnoreCase(
              table.getName()))
            changesToTable.add(change);
        } else if (change instanceof RemoveColumnChange) {
          if (((RemoveColumnChange) change).getChangedTable().getName().equalsIgnoreCase(
              table.getName())) {
            changesToTable.add(change);
          }
        }
        indC++;
      }
      if (!changesToTable.isEmpty()) {
        for (indC = 0; indC < changesToTable.size(); indC++) {
          ModelChange change = changesToTable.get(indC);
          if (change instanceof RemoveColumnChange) {
            // A column has been deleted in the database. We have to
            // delete it from the original model
            // because if not we will have an error
            _log
                .debug("A column has been deleted in the database and we have to delete it in our original model.");
            // ((RemoveColumnChange)change).apply(originaldb,
            // false);
          } else if (change instanceof AddColumnChange) {
            // We will read all the values of the new column, and
            // add them
            // as ColumnDataChanges
            Connection connection = platform.borrowConnection();
            Iterator answer = null;

            Table tableC = ((AddColumnChange) change).getChangedTable();
            Column columnC = ((AddColumnChange) change).getNewColumn();
            answer = readRowsFromTable(connection, platform, currentdb, tableC, _databasefilter);
            while (answer != null && answer.hasNext()) {
              DynaBean db = (DynaBean) answer.next();
              Object value = null;
              try {
                value = db.get(columnC.getName());
              } catch (Exception e) {
                value = db.get(columnC.getName().toLowerCase());
              }
              String val;
              if (db.get(columnC.getName()) == null)
                val = null;
              else
                val = db.get(columnC.getName()).toString();
              dataChanges.add(new ColumnDataChange(tableC, columnC, null, val, currentdb
                  .getDynaClassFor(db).getPrimaryKeyProperties()));
            }
            try {
              if (!connection.isClosed())
                connection.close();
            } catch (Exception ex) {
              // _log.error(ex.getLocalizedMessage());
            }
          }
        }
      }

      // Tables can now be compared.
      Connection connection = platform.borrowConnection();
      Iterator answer = null;

      answer = readRowsFromTable(connection, platform, currentdb, table, _databasefilter);
      Vector<DynaBean> rowsOriginalData = originalData.getRowsFromTable(table.getName());
      // We now have the rows of the table in the database (answer)
      // and the rows in the XML files (HashMap originalData)
      compareTables(originaldb, table, rowsOriginalData, answer);
      try {
        if (!connection.isClosed())
          connection.close();
      } catch (Exception ex) {
        // _log.error(ex.getLocalizedMessage());
      }
    }
    for (int i = 0; i < newTables.size(); i++) {
      Table table = newTables.get(i);
      Connection connection = platform.borrowConnection();
      Iterator answer = null;
      answer = readRowsFromTable(connection, platform, currentdb, table, _databasefilter);
      while (answer != null && answer.hasNext()) {
        // Each row of a new table is a new row
        DynaBean db = (DynaBean) answer.next();
        dataChanges.add(new AddRowChange(table, db));
      }
      try {
        if (!connection.isClosed())
          connection.close();
      } catch (Exception ex) {
        // _log.error(ex.getLocalizedMessage());
      }

    }

  }

  private Iterator readRowsFromTable(Connection connection, Platform platform, Database model,
      Table table, DatabaseFilter filter) {
    if (table.getPrimaryKeyColumns() == null || table.getPrimaryKeyColumns().length == 0) {
      _log.error("Table " + table.getName() + " cannot be read because it has no primary key.");
      return null;
    }
    Table[] atables = { table };
    Statement statement = null;
    ResultSet resultSet = null;
    try {
      statement = connection.createStatement();

      String sqlstatement = "SELECT * FROM " + table.getName();
      if (filter != null && filter.getTableFilter(table.getName()) != null
          && !filter.getTableFilter(table.getName()).equals("")) {
        sqlstatement += " WHERE " + filter.getTableFilter(table.getName()) + " ";
        sqlstatement += " ORDER BY ";
        for (int j = 0; j < table.getPrimaryKeyColumns().length; j++) {
          if (j > 0)
            sqlstatement += ",";
          sqlstatement += table.getPrimaryKeyColumns()[j].getName();
        }
        resultSet = statement.executeQuery(sqlstatement);
        return platform.createResultSetIterator(model, resultSet, atables);
      } else
        return null;
    } catch (SQLException ex) {
      _log.error(ex.getLocalizedMessage());
      return null;
      // throw new DatabaseOperationException("Error while performing a
      // query", ex);
    }
  }

  private void compareTables(Database model, Table table, Vector<DynaBean> originalData,
      Iterator iteratorTable) {
    if (iteratorTable == null) {
      _log.warn("Error while reading table " + table.getName()
          + ". Probably it doesn't have primary key.");
      return;
    }
    if (originalData == null || originalData.size() == 0) {
      // There is no data in the XML files. We add data from the database
      // and leave
      if (iteratorTable.hasNext()) {
        DynaBean dbNew = (DynaBean) iteratorTable.next();
        while (iteratorTable.hasNext()) {
          dataChanges.add(new AddRowChange(table, dbNew));
          // System.out.println("Row will be added: "+dbNew);
          dbNew = (DynaBean) iteratorTable.next();
        }
        dataChanges.add(new AddRowChange(table, dbNew));
      }
      return;
    }

    if (table.getPrimaryKeyColumns() == null || table.getPrimaryKeyColumns().length == 0) {
      _log.error("Cannot compare table " + table.getName()
          + " because it doesn't have a primary key");
      return;
    }

    if (!iteratorTable.hasNext()) {
      // There is no data in the table. Everything must be transformed
      // into RemoveRowChanges
      for (int i = 0; i < originalData.size(); i++) {
        dataChanges.add(new RemoveRowChange(table, originalData.get(i)));
      }
      return;
    }
    int indOrg = 0;
    DynaBean dbOrg = originalData.get(indOrg);
    DynaBean dbNew = (DynaBean) iteratorTable.next();
    while (indOrg < originalData.size() && iteratorTable.hasNext()) {
      dbOrg = originalData.get(indOrg);
      int comp = comparePKs(model, dbOrg, dbNew);
      if (comp == 0) // Rows have the same PKs, we have to compare them
      {
        compareRows(model, dbOrg, dbNew);
        dbNew = (DynaBean) iteratorTable.next();
        indOrg++;
      } else if (comp == -1) // Original model has additional rows, we
      // have to "delete" them
      {
        dataChanges.add(new RemoveRowChange(table, dbOrg));
        // System.out.println("_i.Row will be deleted: "+dbOrg);
        indOrg++;
      } else if (comp == 1) // Target model has additional rows, we have
      // to
      // "add" them
      {
        dataChanges.add(new AddRowChange(table, dbNew));
        // System.out.println("_i.Row will be added: "+dbNew);
        dbNew = (DynaBean) iteratorTable.next();

      } else if (comp == -2) {
        _log.error("Error: non numeric primary key in table " + table.getName() + ".");
        return;
      }
    }
    if (!iteratorTable.hasNext() && indOrg >= originalData.size()) {
      // We've exited the loop when both conditions have not been
      // fulfilled. This means that
      // the last row of the database has not been compared with anything,
      // and in fact is a new row.
      // We have to add it.
      dataChanges.add(new AddRowChange(table, dbNew));
    } else if (indOrg < originalData.size() && !iteratorTable.hasNext()) {
      // There are rows in the XML files, but not in the database. We have
      // to be careful with the last row of the database.
      while (indOrg < originalData.size()) {
        dbOrg = originalData.get(indOrg);
        if (dbNew != null) {
          int comp = comparePKs(model, dbOrg, dbNew);
          if (comp == 0) // Rows have the same PKs, we have to
          // compare them
          {
            compareRows(model, dbOrg, dbNew);
            dbNew = null;
          } else if (comp == -1) // Original model has additional
          // rows, we have to "delete" them
          {
            dataChanges.add(new RemoveRowChange(table, dbOrg));
            // System.out.println("_i.Row will be deleted: "+dbOrg);
          } else if (comp == 1) // Target model has additional rows,
          // we
          // have to "add" them
          {
            dataChanges.add(new AddRowChange(table, dbNew));
            // System.out.println("_i.Row will be added: "+dbNew);
          }
        } else {
          dataChanges.add(new RemoveRowChange(table, dbOrg));
          // System.out.println("Row will be deleted: "+dbOrg);
        }
        indOrg++;
      }
    } else if (iteratorTable.hasNext()) {
      // No rows remaining in the XML files. We will add all the remaining
      // rows of the database.

      while (iteratorTable.hasNext()) {
        dataChanges.add(new AddRowChange(table, dbNew));
        // System.out.println("Row will be added: "+dbNew);
        dbNew = (DynaBean) iteratorTable.next();
      }
      dataChanges.add(new AddRowChange(table, dbNew));
      // System.out.println("Row will be added: "+dbNew);
    }

  }

  private int comparePKs(Database model, DynaBean db1, DynaBean db2) {
    SqlDynaClass dynaClass = model.getDynaClassFor(db1);
    SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();
    for (int i = 0; i < primaryKeys.length; i++) {
      try {
        int pk1 = Integer.parseInt(db1.get(primaryKeys[i].getName()).toString());
        int pk2 = Integer.parseInt(db2.get(primaryKeys[i].getName()).toString());
        if (pk1 < pk2)
          return -1;
        else if (pk1 > pk2)
          return 1;
      } catch (Exception e) {
        String pk1 = db1.get(primaryKeys[i].getName()).toString();
        String pk2 = db2.get(primaryKeys[i].getName()).toString();
        if (pk1.compareTo(pk2) < 0)
          return -1;
        else if (pk1.compareTo(pk2) < 0)
          return 1;
        else if (pk1.compareTo(pk2) == 0)
          return 0;
        else
          return -2;
      }
    }
    return 0;

  }

  private void compareRows(Database model, DynaBean db1, DynaBean db2) {

    SqlDynaClass dynaClass = model.getDynaClassFor(db1);
    SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();
    SqlDynaProperty[] nonprimaryKeys = dynaClass.getNonPrimaryKeyProperties();
    Object[] pkVal = new Object[primaryKeys.length];
    String pk = "[";
    for (int i = 0; i < primaryKeys.length; i++) {
      pk += primaryKeys[i].getName() + "=" + db1.get(primaryKeys[i].getName()) + ";";
      pkVal[i] = db2.get(primaryKeys[i].getName());
    }
    pk += "]";
    Vector<String> tablesModel = new Vector<String>();
    for (int i = 0; i < nonprimaryKeys.length; i++) {
      Object v1 = db1.get(nonprimaryKeys[i].getName());
      Object v2 = db2.get(nonprimaryKeys[i].getName());
      if ((v1 == null && v2 != null) || (v1 != null && v2 == null)
          || (v1 != null && v2 != null && !v1.equals(v2))) {
        String val1;
        if (v1 == null)
          val1 = null;
        else
          val1 = v1.toString();
        String val2;
        if (v2 == null)
          val2 = null;
        else
          val2 = v2.toString();
        dataChanges.add(new ColumnDataChange(dynaClass.getTable(), nonprimaryKeys[i].getColumn(),
            val1, val2, pkVal));
        // System.out.println("Column change:
        // "+pk+"["+nonprimaryKeys[i].getName()+"]:"+v1+","+v2);
      }
    }
  }

  private Iterator readRowsFromTableDAL(DataSetTable table, String moduleIds) {
    if (table == null)
      return new Vector<BaseOBObject>().iterator();
    return DataSetService.getInstance().getExportableObjects(table, moduleIds).iterator();
  }

  private void compareTablesDAL(Database model, Table table, Vector<DynaBean> originalData,
      Iterator iteratorTable) {
    if (iteratorTable == null) {
      _log.warn("Error while reading table " + table.getName()
          + ". Probably it doesn't have primary key.");
      return;
    }
    if (originalData == null || originalData.size() == 0) {
      // There is no data in the XML files. We add data from the database
      // and leave
      if (iteratorTable.hasNext()) {
        BaseOBObject dbNew = (BaseOBObject) iteratorTable.next();
        while (iteratorTable.hasNext()) {
          dataChanges.add(new AddRowDALChange(table, dbNew));
          // System.out.println("Row will be added: "+dbNew);
          dbNew = (BaseOBObject) iteratorTable.next();
        }
        dataChanges.add(new AddRowDALChange(table, dbNew));
      }
      return;
    }

    if (table.getPrimaryKeyColumns() == null || table.getPrimaryKeyColumns().length == 0) {
      _log.error("Cannot compare table " + table.getName()
          + " because it doesn't have a primary key");
      return;
    }

    if (!iteratorTable.hasNext()) {
      // There is no data in the table. Everything must be transformed
      // into RemoveRowChanges
      for (int i = 0; i < originalData.size(); i++) {
        dataChanges.add(new RemoveRowChange(table, originalData.get(i)));
      }
      return;
    }
    int indOrg = 0;
    DynaBean dbOrg = originalData.get(indOrg);
    BaseOBObject dbNew = (BaseOBObject) iteratorTable.next();
    SqlDynaClass dynaClass = model.getDynaClassFor(dbOrg);
    SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();
    DataSetTable datasetTable = tableMap.get(dynaClass.getTable().getName());
    List<DataSetColumn> allColumns = datasetTable.getDataSetColumnList();
    List<Property> properties = DataSetService.getInstance().getExportableProperties(dbNew,
        datasetTable, allColumns);

    while (indOrg < originalData.size() && iteratorTable.hasNext()) {
      dbOrg = originalData.get(indOrg);
      int comp = comparePKs(model, dbOrg, dbNew, primaryKeys, properties);
      if (comp == 0) // Rows have the same PKs, we have to compare them
      {
        compareRows(model, dbOrg, dbNew, datasetTable, allColumns);
        dbNew = (BaseOBObject) iteratorTable.next();
        indOrg++;
      } else if (comp == -1) // Original model has additional rows, we
      // have to "delete" them
      {
        dataChanges.add(new RemoveRowChange(table, dbOrg));
        // System.out.println("_i.Row will be deleted: "+dbOrg);
        indOrg++;
      } else if (comp == 1) // Target model has additional rows, we have
      // to
      // "add" them
      {
        dataChanges.add(new AddRowDALChange(table, dbNew));
        // System.out.println("_i.Row will be added: "+dbNew);
        dbNew = (BaseOBObject) iteratorTable.next();

      } else if (comp == -2) {
        _log.error("Error: non numeric primary key in table " + table.getName() + ".");
        return;
      }
    }
    if (!iteratorTable.hasNext() && indOrg >= originalData.size()) {
      // We've exited the loop when both conditions have not been
      // fulfilled. This means that
      // the last row of the database has not been compared with anything,
      // and in fact is a new row.
      // We have to add it.
      dataChanges.add(new AddRowDALChange(table, dbNew));
    } else if (indOrg < originalData.size() && !iteratorTable.hasNext()) {
      // There are rows in the XML files, but not in the database. We have
      // to be careful with the last row of the database.
      while (indOrg < originalData.size()) {
        dbOrg = originalData.get(indOrg);
        if (dbNew != null) {
          int comp = comparePKs(model, dbOrg, dbNew, primaryKeys, properties);
          if (comp == 0) // Rows have the same PKs, we have to
          // compare them
          {
            compareRows(model, dbOrg, dbNew, datasetTable, allColumns);
            dbNew = null;
          } else if (comp == -1) // Original model has additional
          // rows, we have to "delete" them
          {
            dataChanges.add(new RemoveRowChange(table, dbOrg));
            // System.out.println("_i.Row will be deleted: "+dbOrg);
          } else if (comp == 1) // Target model has additional rows,
          // we
          // have to "add" them
          {
            dataChanges.add(new AddRowDALChange(table, dbNew));
            // System.out.println("_i.Row will be added: "+dbNew);
          }
        } else {
          dataChanges.add(new RemoveRowChange(table, dbOrg));
          // System.out.println("Row will be deleted: "+dbOrg);
        }
        indOrg++;
      }
    } else if (iteratorTable.hasNext()) {
      // No rows remaining in the XML files. We will add all the remaining
      // rows of the database.

      while (iteratorTable.hasNext()) {
        dataChanges.add(new AddRowDALChange(table, dbNew));
        // System.out.println("Row will be added: "+dbNew);
        dbNew = (BaseOBObject) iteratorTable.next();
      }
      dataChanges.add(new AddRowDALChange(table, dbNew));
      // System.out.println("Row will be added: "+dbNew);
    }
  }

  private int comparePKs(Database model, DynaBean db1, BaseOBObject db2,
      SqlDynaProperty[] primaryKeys, List<Property> properties) {
    BaseOBIDHexComparator comparator = new BaseOBIDHexComparator();
    for (int i = 0; i < primaryKeys.length; i++) {
      String pk1 = db1.get(primaryKeys[i].getName()).toString();
      String pk2 = "";
      for (Property property : properties)
        if (property.getColumnName() != null
            && property.getColumnName().equalsIgnoreCase(primaryKeys[i].getName())) {
          pk2 = db2.get(property.getName()).toString();
        }
      int c = comparator.compare(pk1, pk2);
      if (c != 0)
        return c;
    }
    return 0;

  }

  private void compareTablesDALForUpdate(Database model, DataSetTable datasetTable, Table table,
      Iterator iteratorDb, Vector<DynaBean> newData) {

    if (newData == null || newData.size() == 0) {
      // There is no data in the XML files. We remove data from the
      // database and leave
      while (iteratorDb.hasNext())
        dataChanges.add(new RemoveRowDALChange(table, (BaseOBObject) iteratorDb.next()));
      return;
    }
    if (!iteratorDb.hasNext()) {
      // There is no data in the table. Everything must be transformed
      // into AddRowChanges
      for (int i = 0; i < newData.size(); i++) {
        dataChanges.add(new AddRowChange(table, newData.get(i)));
      }
      return;
    }

    int indNew = 0;
    DynaBean dbNew = newData.get(indNew);
    BaseOBObject dbOrg = (BaseOBObject) iteratorDb.next();
    SqlDynaClass dynaClass = model.getDynaClassFor(dbNew);
    SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();

    List<DataSetColumn> allColumns = datasetTable.getDataSetColumnList();
    List<Property> properties = DataSetService.getInstance().getExportableProperties(dbOrg,
        datasetTable, allColumns);
    int obPending = 1;

    while (indNew < newData.size() && iteratorDb.hasNext()) {
      dbNew = newData.get(indNew);
      int comp = comparePKs(model, dbOrg, dbNew, primaryKeys, properties);
      if (comp == 0) // Rows have the same PKs, we have to compare them
      {
        compareRows(model, dbOrg, dbNew, datasetTable, allColumns);
        indNew++;
        dbOrg = (BaseOBObject) iteratorDb.next();
        obPending = 2;
      } else if (comp == -1) // Original model has additional rows, we
      // have to "delete" them
      {
        dataChanges.add(new RemoveRowDALChange(table, dbOrg));
        dbOrg = (BaseOBObject) iteratorDb.next();
        obPending = 1;
      } else if (comp == 1) // Target model has additional rows, we have
      // to "add" them
      {
        dataChanges.add(new AddRowChange(table, dbNew));
        indNew++;
        obPending = 0;
      } else if (comp == -2) {
        _log.error("Error: problem while comparing primary key in table " + table.getName() + ".");
        return;
      }
    }
    if (obPending > 0) {
      // There is at least one row read from the database which wasn't
      // compared. We have to take this into account
      while (indNew < newData.size() && obPending > 0) {
        dbNew = newData.get(indNew);
        int comp = comparePKs(model, dbOrg, dbNew, primaryKeys, properties);
        if (comp == 0) // Rows have the same PKs, we have to compare
        // them
        {
          compareRows(model, dbOrg, dbNew, datasetTable, allColumns);
          indNew++;
          obPending = 0;
        } else if (comp == -1) // Original model has additional rows, we
        // have to "delete" them
        {
          dataChanges.add(new RemoveRowDALChange(table, dbOrg));
          obPending = 0;
        } else if (comp == 1) // Target model has additional rows, we
        // have to "add" them
        {
          dataChanges.add(new AddRowChange(table, dbNew));
        } else if (comp == -2) {
          _log
              .error("Error: problem while comparing primary key in table " + table.getName() + ".");
          return;
        }
      }
    }
    if (indNew < newData.size() && !iteratorDb.hasNext()) {
      // There are rows in the XML files, but not in the database. We have
      // to insert them
      while (indNew < newData.size()) {
        dataChanges.add(new AddRowChange(table, newData.get(indNew++)));
      }
    } else if (indNew >= newData.size()) {
      if (obPending > 0) {
        System.out.println(dbOrg);
        dataChanges.add(new RemoveRowDALChange(table, dbOrg));
      }
      // No rows remaining in the XML files. We will remove all the
      // remaining rows of the database.
      while (iteratorDb.hasNext())
        dataChanges.add(new RemoveRowDALChange(table, (BaseOBObject) iteratorDb.next()));
    }
  }

  private void compareTablesDALForUpdate(Database model, DataSetTable datasetTable, Table table,
      List<BaseOBObject> dbObjects, Vector<DynaBean> newData) {

    if (newData == null || newData.size() == 0) {
      // There is no data in the XML files. We remove data from the
      // database
      // and leave
      for (BaseOBObject object : dbObjects) {
        dataChanges.add(new RemoveRowDALChange(table, object));
      }
      return;
    }
    if (dbObjects.size() == 0) {
      // There is no data in the table. Everything must be transformed
      // into AddRowChanges

      for (int i = 0; i < newData.size(); i++) {
        dataChanges.add(new AddRowChange(table, newData.get(i)));
      }
      return;
    }

    int indOrg = 0;
    int indNew = 0;
    DynaBean dbNew = newData.get(indNew);
    BaseOBObject dbOrg = dbObjects.get(indOrg);
    SqlDynaClass dynaClass = model.getDynaClassFor(dbNew);
    SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();

    List<DataSetColumn> allColumns = datasetTable.getDataSetColumnList();
    List<Property> properties = DataSetService.getInstance().getExportableProperties(dbOrg,
        datasetTable, allColumns);

    while (indNew < newData.size() && indOrg < dbObjects.size()) {
      dbNew = newData.get(indNew);
      dbOrg = dbObjects.get(indOrg);
      int comp = comparePKs(model, dbOrg, dbNew, primaryKeys, properties);
      if (comp == 0) // Rows have the same PKs, we have to compare them
      {
        compareRows(model, dbOrg, dbNew, datasetTable, allColumns);
        indNew++;
        indOrg++;
      } else if (comp == -1) // Original model has additional rows, we
      // have to "delete" them
      {
        dataChanges.add(new RemoveRowDALChange(table, dbOrg));
        indOrg++;
      } else if (comp == 1) // Target model has additional rows, we have
      // to "add" them
      {
        dataChanges.add(new AddRowChange(table, dbNew));
        indNew++;
      } else if (comp == -2) {
        _log.error("Error: problem while comparing primary key in table " + table.getName() + ".");
        return;
      }
    }

    if (indNew < newData.size() && indOrg >= dbObjects.size()) {
      // There are rows in the XML files, but not in the database. We have
      // to insert them
      while (indNew < newData.size())
        dataChanges.add(new AddRowChange(table, newData.get(indNew++)));
    } else if (indNew >= newData.size() && indOrg < dbObjects.size()) {
      // No rows remaining in the XML files. We will remove all the
      // remaining rows of the database.
      while (indOrg < dbObjects.size())
        dataChanges.add(new RemoveRowDALChange(table, dbObjects.get(indOrg++)));
    }
  }

  public void compare(DatabaseData databaseDataOrg, DatabaseData databaseDataNew) {
    Database databaseOrg = databaseDataOrg.getDatabase();
    Database databaseNew = databaseDataNew.getDatabase();
    Vector<Table> commonTables = new Vector<Table>();
    Table[] tablesOrg = databaseOrg.getTables();
    Table[] tablesNew = databaseNew.getTables();
    for (int i = 0; i < tablesOrg.length; i++) {
      if (databaseNew.findTable(tablesOrg[i].getName()) == null) {
        // Table has been removed. We remove its data.
        Vector<DynaBean> rows = databaseDataOrg.getRowsFromTable(tablesOrg[i].getName());
        for (DynaBean bean : rows)
          dataChanges.add(new RemoveRowChange(tablesOrg[i], bean));
      } else
        commonTables.add(tablesOrg[i]);
    }

    for (int i = 0; i < tablesNew.length; i++) {
      if (databaseOrg.findTable(tablesNew[i].getName()) == null) {
        // Table has been added. We add its data.
        Vector<DynaBean> rows = databaseDataNew.getRowsFromTable(tablesNew[i].getName());
        for (DynaBean bean : rows)
          dataChanges.add(new AddRowChange(tablesNew[i], bean));
      }
    }

    for (Table table : commonTables)
      compareTables(databaseDataOrg, databaseDataNew, table.getName());

  }

  private void compareTables(DatabaseData databaseDataOrg, DatabaseData databaseDataNew,
      String tablename) {
    Table tableOrg = databaseDataOrg.getDatabase().findTable(tablename);
    Table tableNew = databaseDataNew.getDatabase().findTable(tablename);

    Vector<DynaBean> rowsOrg = databaseDataOrg.getRowsFromTable(tablename);
    Vector<DynaBean> rowsNew = databaseDataNew.getRowsFromTable(tablename);
    int indOrg = 0;
    int indNew = 0;
    while (indNew < rowsOrg.size() && indOrg < rowsNew.size()) {
      int comp = comparePKs(tableOrg, tableNew, rowsOrg.get(indOrg), rowsNew.get(indNew));
      if (comp == 0) // Rows have the same PKs, we have to compare them
      {
        compareRows(tableOrg, tableNew, rowsOrg.get(indOrg), rowsNew.get(indNew));
        indNew++;
        indOrg++;
      } else if (comp == -1) // Original model has additional rows, we
      // have to "delete" them
      {
        dataChanges.add(new RemoveRowChange(tableOrg, rowsOrg.get(indOrg)));
        indOrg++;
      } else if (comp == 1) // Target model has additional rows, we have
      // to "add" them
      {
        dataChanges.add(new AddRowChange(tableNew, rowsNew.get(indNew)));
        indNew++;
      } else if (comp == -2) {
        _log.error("Error: problem while comparing primary key in table " + tableOrg.getName()
            + ".");
        return;
      }
    }

    if (indNew < rowsNew.size() && indOrg >= rowsOrg.size()) {
      // There are rows in the target tables, but not in the original tables. We have
      // to insert them
      while (indNew < rowsNew.size())
        dataChanges.add(new AddRowChange(tableNew, rowsNew.get(indNew++)));
    } else if (indNew >= rowsNew.size() && indOrg < rowsOrg.size()) {
      // No rows remaining in the target table files. We will remove all the
      // remaining rows of the original table.
      while (indOrg < rowsOrg.size())
        dataChanges.add(new RemoveRowChange(tableOrg, rowsOrg.get(indOrg++)));
    }
  }

  private int comparePKs(Database model, BaseOBObject db1, DynaBean db2,
      SqlDynaProperty[] primaryKeys, List<Property> properties) {
    BaseOBIDHexComparator comparator = new BaseOBIDHexComparator();
    for (int i = 0; i < primaryKeys.length; i++) {
      String pk1 = "";
      String pk2 = db2.get(primaryKeys[i].getName()).toString();
      for (Property property : properties)
        if (property.getColumnName() != null
            && property.getColumnName().equalsIgnoreCase(primaryKeys[i].getName()))
          pk1 = db1.get(property.getName()).toString();
      int c = comparator.compare(pk1, pk2.toString());
      if (c != 0)
        return c;
    }
    return 0;

  }

  private int comparePKs(Table tableOrg, Table tableNew, DynaBean db1, DynaBean db2) {
    BaseOBIDHexComparator comparator = new BaseOBIDHexComparator();
    for (int i = 0; i < tableOrg.getPrimaryKeyColumns().length; i++) {
      String pk1 = db1.get(tableOrg.getPrimaryKeyColumns()[0].getName()).toString();
      String pk2 = db2.get(tableOrg.getPrimaryKeyColumns()[0].getName()).toString();
      int c = comparator.compare(pk1, pk2.toString());
      if (c != 0)
        return c;
    }
    return 0;

  }

  private static class BaseOBIDHexComparator implements Comparator<Object> {

    public int compare(Object o1, Object o2) {
      final String bob1 = o1.toString();
      final String bob2 = (String) o2;

      try {
        BigInteger bd1 = new BigInteger(bob1, 32);
        BigInteger bd2 = new BigInteger(bob2, 32);
        return bd1.compareTo(bd2);
      } catch (NumberFormatException n) {
        System.out.println("problem: " + n.getMessage());
        return 0;
      }
    }
  }

  private void compareRows(Database model, DynaBean db1, BaseOBObject db2, DataSetTable table,
      List<DataSetColumn> allColumns) {
    SqlDynaClass dynaClass = model.getDynaClassFor(db1);
    SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();
    SqlDynaProperty[] nonprimaryKeys = dynaClass.getNonPrimaryKeyProperties();
    List<Property> properties = DataSetService.getInstance().getExportableProperties(db2, table,
        allColumns);
    Object pkVal = db1.get(primaryKeys[0].getName());
    String pk = "[";
    for (int i = 0; i < primaryKeys.length; i++) {
      pk += primaryKeys[i].getName() + "=" + db1.get(primaryKeys[i].getName()) + ";";
    }
    pk += "]";
    Vector<String> tablesModel = new Vector<String>();
    for (int i = 0; i < nonprimaryKeys.length; i++) {
      Object v1 = db1.get(nonprimaryKeys[i].getName());
      Object v2 = null;
      String vs1 = null;
      String vs2 = null;
      for (Property property : properties) {
        if (property.getColumnName() != null
            && property.getColumnName().equalsIgnoreCase(nonprimaryKeys[i].getName())) {
          v2 = db2.get(property.getName());
          if (v2 instanceof BaseOBObject)
            v2 = DalUtil.getReferencedPropertyValue(property, v2);
          if (v2 instanceof Boolean) {
            if (((Boolean) v2).booleanValue())
              v2 = "Y";
            else
              v2 = "N";
          }
          if (v1 != null) {
            v1 = v1.toString();
            vs1 = v1.toString();
          }
          if (v2 != null) {
            v2 = v2.toString();
            vs2 = v2.toString();
          }
        }
      }
      if ((v1 == null && v2 != null) || (v1 != null && v2 == null)
          || (v1 != null && v2 != null && !v1.equals(v2))) {
        dataChanges.add(new ColumnDataChange(dynaClass.getTable(), nonprimaryKeys[i].getColumn(),
            vs1, vs2, pkVal));
        // System.out.println("Column change:
        // "+pk+"["+nonprimaryKeys[i].getName()+"]:"+v1+","+v2);
      }
    }
  }

  private void compareRows(Database model, BaseOBObject db1, DynaBean db2, DataSetTable table,
      List<DataSetColumn> allColumns) {
    SqlDynaClass dynaClass = model.getDynaClassFor(db2);
    SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();
    SqlDynaProperty[] nonprimaryKeys = dynaClass.getNonPrimaryKeyProperties();
    List<Property> properties = DataSetService.getInstance().getExportableProperties(db1, table,
        allColumns);
    Object pkVal = db2.get(primaryKeys[0].getName());
    String pk = "[";
    for (int i = 0; i < primaryKeys.length; i++) {
      pk += primaryKeys[i].getName() + "=" + db2.get(primaryKeys[i].getName()) + ";";
    }
    pk += "]";
    Vector<String> tablesModel = new Vector<String>();
    for (Property property : properties) {
      if (!property.isId()) {
        Object v1 = null;
        Object v2 = null;
        Column column = null;
        for (int i = 0; i < nonprimaryKeys.length; i++) {
          if (property.getColumnName() != null
              && property.getColumnName().equalsIgnoreCase(nonprimaryKeys[i].getName())) {
            v2 = db2.get(nonprimaryKeys[i].getName());
            column = nonprimaryKeys[i].getColumn();
            v1 = db1.get(property.getName());
            if (v1 instanceof BaseOBObject)
              v1 = DalUtil.getReferencedPropertyValue(property, v1);
            if (v1 instanceof Boolean) {
              if (((Boolean) v1).booleanValue())
                v1 = "Y";
              else
                v1 = "N";
            }

            /*
             * if (v1 != null) v1 = v1.toString(); if (v2 != null) v2 = v2.toString();
             */
          }
        }
        if (column != null) {
          if ((v1 == null && v2 != null) || (v1 != null && v2 == null)
              || (v1 != null && v2 != null && !v1.toString().equals(v2.toString()))) {
            String vs1 = null;
            String vs2 = null;
            if (v1 != null)
              vs1 = v1.toString();
            if (v2 != null)
              vs2 = v2.toString();
            dataChanges.add(new ColumnDataChange(dynaClass.getTable(), column, vs1, vs2, pkVal));
            // System.out.println("Column change:
            // "+pk+"["+nonprimaryKeys[i].getName()+"]:"+v1+","+v2);
          }
        }
      }
    }
  }

  private void compareRows(Table tableOrg, Table tableNew, DynaBean db1, DynaBean db2) {

    for (int i = 0; i < tableOrg.getColumnCount(); i++) {
      if (!tableOrg.getColumn(i).isPrimaryKey()) {
        if (tableNew.findColumn(tableOrg.getColumn(i).getName()) != null) {
          Object v1 = db1.get(tableOrg.getColumn(i).getName());
          Object v2 = db2.get(tableOrg.getColumn(i).getName());
          String vs1 = v1 == null ? null : v1.toString();
          String vs2 = v2 == null ? null : v2.toString();
          if (!(vs1 == null && vs2 == null)
              && ((vs1 == null && vs2 != null) || (vs1 != null && vs2 == null) || !vs1.equals(vs2)))
            dataChanges.add(new ColumnDataChange(tableOrg, tableOrg.getColumn(i), vs1, vs2, db1
                .get(tableOrg.getPrimaryKeyColumns()[0].getName())));
        } else {
          // Column doesn't exist in new table
        }
      }
    }

    for (int i = 0; i < tableNew.getColumnCount(); i++) {
      if (!tableNew.getColumn(i).isPrimaryKey()) {
        if (tableOrg.findColumn(tableNew.getColumn(i).getName()) != null) {
          // Column exists in both, so it was taken into account in the previous loop
        } else {
          String vs2 = db2.get(tableNew.getColumn(i).getName()) == null ? null : db2.get(
              tableNew.getColumn(i).getName()).toString();
          // Column doesn't exist. We'll add its values.
          dataChanges.add(new ColumnDataChange(tableNew, tableNew.getColumn(i), null, vs2, db2
              .get(tableNew.getPrimaryKeyColumns()[0].getName())));
        }
      }
    }
  }

  public Vector<Change> getChanges() {
    return dataChanges;
  }

  public Iterator getModelChanges() {
    return modelChanges.iterator();
  }

  public List getModelChangesList() {
    return modelChanges;
  }
}
