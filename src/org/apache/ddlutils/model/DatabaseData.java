package org.apache.ddlutils.model;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.ddlutils.dynabean.SqlDynaProperty;
import org.apache.ddlutils.io.DataToArraySink;

public class DatabaseData {

  protected Database _model;
  protected HashMap<String, Vector<DynaBean>> _databaseBeans;
  private boolean strictMode = false;

  public DatabaseData(Database model) {
    _model = model;
    _databaseBeans = new HashMap<String, Vector<DynaBean>>();
  }

  public void insertDynaBeansFromVector(String tablename, Vector<DynaBean> vector) {
    if (_databaseBeans.containsKey(tablename.toUpperCase())) {
      _databaseBeans.get(tablename.toUpperCase()).addAll(vector);
    } else {
      _databaseBeans.put(tablename.toUpperCase(), vector);
    }
    DataToArraySink.sortArray(_model, _databaseBeans.get(tablename));
  }

  public Vector<DynaBean> getRowsFromTable(String tablename) {
    return _databaseBeans.get(tablename.toUpperCase());
  }

  public Set<String> getTableNames() {
    return _databaseBeans.keySet();
  }

  public void setStrictMode(boolean strictMode) {
    this.strictMode = strictMode;
  }

  public boolean removeRow(Table table, DynaBean row) {
    boolean changeDone = false;
    System.out.println("Trying to remove row " + row + "f rom table " + table);
    Vector<DynaBean> rows = getRowsFromTable(table.getName());
    if (rows == null) {
      System.out.println("Error. Trying to remove row in table " + table.getName()
          + ". The table doesn't exist, or is empty.");
      return changeDone;
    }
    SqlDynaProperty[] primaryKeys = _model.getDynaClassFor(row).getPrimaryKeyProperties();
    int i = 0;
    while (i < rows.size() && !row.equals(rows.get(i))) {
      i++;
    }
    if (i < rows.size() && row.equals(rows.get(i))) {
      rows.remove(i);
      changeDone = true;
    } else {
      System.out.println(
          "We haven't found the row we wanted to remove. We will search by just primary key.");
      i = 0;
      boolean found = false;
      while (i < rows.size() && !found) {
        found = true;
        SqlDynaProperty[] primaryKeysA = _model.getDynaClassFor(rows.get(i))
            .getPrimaryKeyProperties();
        for (int j = 0; j < primaryKeys.length && found; j++) {
          if (!row.get(primaryKeys[j].getName())
              .equals(rows.get(i).get(primaryKeysA[j].getName()))) {
            found = false;
          }
        }
        i++;
      }
      if (found) {
        System.out.println(
            "We found a row with the same Primary Key. We will remove it despite it was not exactly the same.");
        rows.remove(i - 1);
        changeDone = true;
      } else {
        String error = "We didn't found the row that we wanted to change. Table:[" + table.getName()
            + "] PK[: ";
        for (i = 0; i < primaryKeys.length; i++) {
          if (i > 0) {
            error += ",";
          }
          error += row.get(primaryKeys[i].getName());
        }
        System.out.println(error + "]");
      }
    }
    return changeDone;
  }

  public boolean addRow(Table table, DynaBean row, boolean reorder) {
    boolean changeDone = false;
    if (_model.findTable(table.getName()) == null) {
      System.out.println(
          "Error: impossible to add row in table " + table + ", as the table doesn't exist.");
    } else {
      if (!_databaseBeans.containsKey(table.getName().toUpperCase())) {
        _databaseBeans.put(table.getName().toUpperCase(), new Vector<DynaBean>());
      }
      _databaseBeans.get(table.getName().toUpperCase()).add(row);
      changeDone = true;
      if (reorder) {
        DataToArraySink.sortArray(_model, _databaseBeans.get(table.getName()));
      }
    }
    return changeDone;
  }

  public boolean changeRow(Table table, Column column, Object[] primaryKeys, Object oldValue,
      Object newValue) {
    boolean changeDone = true;
    if (table == null) {
      System.out.println("Error: impossible to change row in table, as the table doesn't exist.");
    } else {
      if (column == null) {
        System.out.println(
            "Error: impossible to change row in table " + table + ", as the column doesn't exist.");
      } else {
        Vector<DynaBean> rows = getRowsFromTable(table.getName());
        if (rows == null) {
          // we return true in this case, because this means that the row for this change doesn't
          // belong to the module being exported
          return true;
        }
        int i = 0;
        boolean found = false;
        while (i < rows.size() && !found) {
          found = true;
          // SqlDynaProperty[]
          // primaryKeysCols=_model.getDynaClassFor(rows.get(i)).getPrimaryKeyProperties();
          Column[] primaryKeysCols = table.getPrimaryKeyColumns();
          Object[] primaryKeyA = new Object[primaryKeysCols.length];
          for (int j = 0; j < primaryKeyA.length; j++) {
            primaryKeyA[j] = rows.get(i).get(primaryKeysCols[j].getName());
          }
          for (int j = 0; j < primaryKeys.length && found; j++) {
            if ((primaryKeys[j] == null && primaryKeyA[j] != null)
                || (primaryKeys[j] != null && primaryKeyA[j] == null)
                || !primaryKeys[j].toString().equals(primaryKeyA[j].toString())) {
              found = false;
            }
          }
          i++;
        }
        if (found) {
          Object currentValue = rows.get(i - 1).get(column.getName());
          if (!(oldValue == null && currentValue == null)
              && ((oldValue == null && currentValue != null)
                  || (oldValue != null && currentValue == null)
                  || (!currentValue.toString().equals(oldValue.toString())))) {
            String error = "Warning: old value in row not equal to expected one. Table:["
                + table.getName() + "] PK[: ";
            for (int j = 0; j < primaryKeys.length; j++) {
              if (j > 0) {
                error += ",";
              }
              error += primaryKeys[j];
            }
            System.out.println(
                error + "] Old Value found: " + currentValue + " Old value expected " + oldValue);
            if (strictMode) {
              return false;
            }
          }
          rows.get(i - 1).set(column.getName(), newValue);
          changeDone = true;
        } else {
          String error = "We didn't found the row that we wanted to change. Table:["
              + table.getName() + "] PK[: ";
          for (i = 0; i < primaryKeys.length; i++) {
            if (i > 0) {
              error += ",";
            }
            error += primaryKeys[i];
          }
          System.out.println(error + "]");
        }

      }
    }
    return changeDone;
  }

  public boolean changeRowInReverse(Table table, Column column, Object primaryKeys, Object oldValue,
      Object newValue) {
    Vector<DynaBean> rows = getRowsFromTable(table.getName());
    int i = 0;
    boolean found = false;
    boolean changeDone = true;
    if (rows == null) {
      // we return true in this case, because this means that the row for this change doesn't belong
      // to the module being exported
      return true;
    }
    while (i < rows.size() && !found) {
      Column[] primaryKeysCols = table.getPrimaryKeyColumns();
      Object primaryKeyA = rows.get(i).get(primaryKeysCols[0].getName());
      if (primaryKeys.toString().equals(primaryKeyA.toString())) {
        found = true;
      }
      i++;
    }
    if (found) {
      Object currentValue = rows.get(i - 1).get(column.getName());
      if (!(newValue == null && currentValue == null) && ((newValue == null && currentValue != null)
          || (newValue != null && currentValue == null)
          || (!currentValue.toString().equals(newValue.toString())))) {
        // We cannot reverse this change, as the value in the database is different from the value
        // we expected
        changeDone = false;
      } else {
        rows.get(i - 1).set(column.getName(), oldValue);
        changeDone = true;
      }
    }
    return changeDone;
  }

  public void reorderAllTables() {
    for (int i = 0; i < _model.getTableCount(); i++) {
      DataToArraySink.sortArray(_model, getRowsFromTable(_model.getTable(i).getName()));
    }
  }

  public Database getDatabase() {
    return _model;
  }

  /**
   * Searches for a value of a field in a list of DynaBeans
   * 
   * @param vector
   *          List to be searched
   * @param name
   *          Name of a field
   * @param property
   *          Value of the field
   * @return The first DynaBean having this value in the specified field
   */
  public static DynaBean searchDynaBean(List<DynaBean> vector, String name, String property) {
    for (DynaBean bean : vector) {
      if (bean.get(property).toString().equalsIgnoreCase(name)) {
        return bean;
      }
    }
    return null;
  }

  /**
   * Searches for a value of a field in a list of DynaBeans
   * 
   * @param vector
   *          List to be searched
   * @param name
   *          Name of a field
   * @param property
   *          Value of the field
   * @return List of all DynaBean having this value in the specified field
   */
  public static List<DynaBean> searchDynaBeans(List<DynaBean> vector, String name,
      String property) {
    Vector<DynaBean> dbs = new Vector<DynaBean>();
    for (DynaBean bean : vector) {
      if (bean.get(property).toString().equalsIgnoreCase(name)) {
        dbs.add(bean);
      }
    }
    return dbs;
  }

}
