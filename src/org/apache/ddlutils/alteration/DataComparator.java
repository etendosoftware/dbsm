package org.apache.ddlutils.alteration;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Comparator;
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
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.io.DatabaseFilter;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;
import org.openbravo.ddlutils.util.OBDataset;
import org.openbravo.ddlutils.util.OBDatasetTable;

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

  public DataComparator(PlatformInfo platformInfo, boolean caseSensitive) {
    _platformInfo = platformInfo;
    _caseSensitive = caseSensitive;
  }

  public void setFilter(DatabaseFilter filter) {
    _databasefilter = filter;
  }

  private class ChangeComparator implements Comparator<Change> {
    public int compare(Change o1, Change o2) {
      if (o1 instanceof RemoveRowChange && o2 instanceof AddRowChange)
        return -1;
      else if (o1 instanceof AddRowChange && o2 instanceof RemoveRowChange)
        return 1;
      else if (o1 instanceof RemoveRowChange && o2 instanceof ColumnDataChange)
        return -1;
      else if (o1 instanceof AddRowChange && o2 instanceof ColumnDataChange)
        return 1;
      else
        return 0;
    }
  }

  public void compare(Database originaldb, Database currentdb, Platform platform,
      DatabaseData oldData, OBDataset dataset, String moduleId) throws SQLException {

    ModelComparator modelComparator = new ModelComparator(_platformInfo, _caseSensitive);
    modelChanges = modelComparator.compare(originaldb, currentdb);
    Table[] tables = currentdb.getTables();

    List<OBDatasetTable> tableList = dataset.getTableList();

    // Now we will compare tables. If tables are equivalent in both models,
    // we will compare rows.
    for (OBDatasetTable dsTable : tableList) {
      Table table = currentdb.findTable(dsTable.getName());
      // Tables can now be compared.
      Connection connection = platform.borrowConnection();
      DatabaseDataIO dbIO = new DatabaseDataIO();
      Vector<DynaBean> rowsNewData = dbIO.readRowsFromTableList(connection, platform, currentdb,
          table, dsTable, moduleId);
      if (rowsNewData == null) {
        _log.error("Couldn't read rows from table " + table.getName());
        throw new SQLException("Couldn't read rows from table " + table.getName());
      }
      Vector<DynaBean> rowsOldData = oldData.getRowsFromTable(table.getName());
      compareTablesToUpdate(currentdb, table, dsTable, rowsOldData, rowsNewData);
      try {
        if (!connection.isClosed())
          connection.close();
      } catch (Exception ex) {
        // _log.error(ex.getLocalizedMessage());
      }
    }

  }

  public void compareToUpdate(Database currentdb, Platform platform, DatabaseData newData,
      OBDataset dataset, String moduleId) throws SQLException {

    Table[] tables = currentdb.getTables();

    List<OBDatasetTable> tableList = dataset.getTableList();

    // Now we will compare tables. If tables are equivalent in both models,
    // we will compare rows.
    for (OBDatasetTable dsTable : tableList) {
      Table table = currentdb.findTable(dsTable.getName());
      // Tables can now be compared.
      Connection connection = platform.borrowConnection();
      DatabaseDataIO dbIO = new DatabaseDataIO();
      Vector<DynaBean> rowsOldData = dbIO.readRowsFromTableList(connection, platform, currentdb,
          table, dsTable, moduleId);
      if (rowsOldData == null) {
        _log.error("Couldn't read rows from table " + table.getName());
        throw new SQLException("Couldn't read rows from table " + table.getName());
      }
      Vector<DynaBean> rowsNewData = newData.getRowsFromTable(table.getName());
      compareTablesToUpdate(currentdb, table, dsTable, rowsOldData, rowsNewData);
      try {
        if (!connection.isClosed())
          connection.close();
      } catch (Exception ex) {
        // _log.error(ex.getLocalizedMessage());
      }
    }

  }

  private void compareTablesToUpdate(Database model, Table table, OBDatasetTable dsTable,
      Vector<DynaBean> rowsOrg, Vector<DynaBean> rowsNew) {
    if (rowsOrg == null && rowsNew == null)
      return;
    if (rowsOrg == null)
      rowsOrg = new Vector<DynaBean>();
    if (rowsNew == null)
      rowsNew = new Vector<DynaBean>();

    int indOrg = 0;
    int indNew = 0;
    while (indOrg < rowsOrg.size() && indNew < rowsNew.size()) {
      int comp = comparePKs(table, rowsOrg.get(indOrg), rowsNew.get(indNew));
      if (comp == 0) // Rows have the same PKs, we have to compare them
      {
        compareRows(model, dsTable, rowsOrg.get(indOrg), rowsNew.get(indNew));
        indNew++;
        indOrg++;
      } else if (comp == -1) // Original model has additional rows, we
      // have to "delete" them
      {
        dataChanges.add(new RemoveRowChange(table, rowsOrg.get(indOrg)));
        indOrg++;
      } else if (comp == 1) // Target model has additional rows, we have
      // to "add" them
      {
        dataChanges.add(new AddRowChange(table, rowsNew.get(indNew)));
        indNew++;
      } else if (comp == -2) {
        _log.error("Error: problem while comparing primary key in table " + table.getName() + ".");
        return;
      }
    }

    if (indNew < rowsNew.size() && indOrg >= rowsOrg.size()) {
      // There are rows in the target tables, but not in the original tables. We have
      // to insert them
      while (indNew < rowsNew.size())
        dataChanges.add(new AddRowChange(table, rowsNew.get(indNew++)));
    } else if (indNew >= rowsNew.size() && indOrg < rowsOrg.size()) {
      // No rows remaining in the target table files. We will remove all the
      // remaining rows of the original table.
      while (indOrg < rowsOrg.size())
        dataChanges.add(new RemoveRowChange(table, rowsOrg.get(indOrg++)));
    }

  }

  private void compareRows(Database model, OBDatasetTable table, DynaBean db1, DynaBean db2) {

    SqlDynaClass dynaClass = model.getDynaClassFor(db1);
    SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();
    SqlDynaProperty[] nonprimaryKeys = dynaClass.getNonPrimaryKeyProperties();
    Object pkVal = db2.get(primaryKeys[0].getName());
    Vector<String> tablesModel = new Vector<String>();
    for (int i = 0; i < nonprimaryKeys.length; i++) {
      if (table.includesColumn(nonprimaryKeys[i].getName())) {
        Object v1 = db1.get(nonprimaryKeys[i].getName());
        Object v2 = db2.get(nonprimaryKeys[i].getName());
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
        if ((val1 == null && val2 != null) || (val1 != null && val2 == null)
            || (val1 != null && val2 != null && !val1.equals(val2))) {
          dataChanges.add(new ColumnDataChange(dynaClass.getTable(), nonprimaryKeys[i].getColumn(),
              val1, val2, pkVal));
          // System.out.println("Column change:
          // "+pk+"["+nonprimaryKeys[i].getName()+"]:"+v1+","+v2);
        }
      }
    }
  }

  private void compareRows(Database model, DynaBean db1, DynaBean db2) {

    SqlDynaClass dynaClass = model.getDynaClassFor(db1);
    SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();
    SqlDynaProperty[] nonprimaryKeys = dynaClass.getNonPrimaryKeyProperties();
    Object pkVal = db2.get(primaryKeys[0].getName());
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
        if (rows == null)
          rows = new Vector<DynaBean>();
        for (DynaBean bean : rows)
          dataChanges.add(new RemoveRowChange(tablesOrg[i], bean));
      } else
        commonTables.add(tablesOrg[i]);
    }

    for (int i = 0; i < tablesNew.length; i++) {
      if (databaseOrg.findTable(tablesNew[i].getName()) == null) {
        // Table has been added. We add its data.
        Vector<DynaBean> rows = databaseDataNew.getRowsFromTable(tablesNew[i].getName());
        if (rows == null)
          rows = new Vector<DynaBean>();
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

    if (rowsOrg == null && rowsNew == null)
      return;
    if (rowsOrg == null)
      rowsOrg = new Vector<DynaBean>();
    if (rowsNew == null)
      rowsNew = new Vector<DynaBean>();

    int indOrg = 0;
    int indNew = 0;
    while (indOrg < rowsOrg.size() && indNew < rowsNew.size()) {
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

  private int comparePKs(Table table, DynaBean db1, DynaBean db2) {
    BaseOBIDHexComparator comparator = new BaseOBIDHexComparator();
    for (int i = 0; i < table.getPrimaryKeyColumns().length; i++) {
      String pk1 = db1.get(table.getPrimaryKeyColumns()[0].getName()).toString();
      String pk2 = db2.get(table.getPrimaryKeyColumns()[0].getName()).toString();
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
