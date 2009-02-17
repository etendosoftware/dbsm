package org.apache.ddlutils.model;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.ddlutils.dynabean.SqlDynaProperty;
import org.apache.ddlutils.io.DataToArraySink;

public class DatabaseData {

    protected Database _model;
    protected HashMap<String, Vector<DynaBean>> _databaseBeans;

    public DatabaseData(Database model) {
        _model = model;
        _databaseBeans = new HashMap<String, Vector<DynaBean>>();
    }

    public void insertDynaBeansFromVector(String tablename,
            Vector<DynaBean> vector) {
        if (_databaseBeans.containsKey(tablename.toUpperCase()))
            _databaseBeans.get(tablename.toUpperCase()).addAll(vector);
        else
            _databaseBeans.put(tablename.toUpperCase(), vector);
        DataToArraySink.sortArray(_model, _databaseBeans.get(tablename));
    }

    public Vector<DynaBean> getRowsFromTable(String tablename) {
        return _databaseBeans.get(tablename.toUpperCase());
    }

    public Set<String> getTableNames() {
        return _databaseBeans.keySet();
    }

    public void removeRow(Table table, DynaBean row) {
        System.out.println("Trying to remove row " + row + "f rom table "
                + table);
        Vector<DynaBean> rows = getRowsFromTable(table.getName());
        if (rows == null) {
            System.out.println("Error. Trying to remove row in table "
                    + table.getName()
                    + ". The table doesn't exist, or is empty.");
            return;
        }
        SqlDynaProperty[] primaryKeys = _model.getDynaClassFor(row)
                .getPrimaryKeyProperties();
        int i = 0;
        while (i < rows.size() && !row.equals(rows.get(i)))
            i++;
        if (i < rows.size() && row.equals(rows.get(i)))
            rows.remove(i);
        else {
            System.out
                    .println("We haven't found the row we wanted to remove. We will search by just primary key.");
            i = 0;
            boolean found = false;
            while (i < rows.size() && !found) {
                found = true;
                SqlDynaProperty[] primaryKeysA = _model.getDynaClassFor(
                        rows.get(i)).getPrimaryKeyProperties();
                for (int j = 0; j < primaryKeys.length && found; j++) {
                    if (!row.get(primaryKeys[j].getName()).equals(
                            rows.get(i).get(primaryKeysA[j].getName())))
                        found = false;
                }
                i++;
            }
            if (found) {
                System.out
                        .println("We found a row with the same Primary Key. We will remove it despite it was not exactly the same.");
                rows.remove(i - 1);
            } else {
                String error = "We didn't found the row that we wanted to change. Table:["
                        + table.getName() + "] PK[: ";
                for (i = 0; i < primaryKeys.length; i++) {
                    if (i > 0)
                        error += ",";
                    error += row.get(primaryKeys[i].getName());
                }
                System.out.println(error + "]");
            }
        }
    }

    public void addRow(Table table, DynaBean row, boolean reorder) {
        System.out.println("Trying to add row " + row + " in table " + table);
        if (_model.findTable(table.getName()) == null) {
            System.out.println("Error: impossible to add row in table " + table
                    + ", as the table doesn't exist.");
        } else {
            if (!_databaseBeans.containsKey(table.getName().toUpperCase())) {
                _databaseBeans.put(table.getName().toUpperCase(),
                        new Vector<DynaBean>());
            }
            _databaseBeans.get(table.getName().toUpperCase()).add(row);
            if (reorder)
                DataToArraySink.sortArray(_model, _databaseBeans.get(table
                        .getName()));
        }
    }

    public void changeRow(Table table, Column column, Object[] primaryKeys,
            Object oldValue, Object newValue) {
        if (table == null) {
            System.out
                    .println("Error: impossible to change row in table, as the table doesn't exist.");
        } else {
            if (column == null) {
                System.out.println("Error: impossible to change row in table "
                        + table + ", as the column doesn't exist.");
            } else {
                Vector<DynaBean> rows = getRowsFromTable(table.getName());
                int i = 0;
                boolean found = false;
                while (i < rows.size() && !found) {
                    found = true;
                    // SqlDynaProperty[]
                    // primaryKeysCols=_model.getDynaClassFor(rows.get(i)).getPrimaryKeyProperties();
                    Column[] primaryKeysCols = table.getPrimaryKeyColumns();
                    Object[] primaryKeyA = new Object[primaryKeysCols.length];
                    for (int j = 0; j < primaryKeyA.length; j++)
                        primaryKeyA[j] = rows.get(i).get(
                                primaryKeysCols[j].getName());
                    for (int j = 0; j < primaryKeys.length && found; j++)
                        if ((primaryKeys[j] == null && primaryKeyA[j] != null)
                                || (primaryKeys[j] != null && primaryKeyA[j] == null)
                                || !primaryKeys[j].toString().equals(
                                        primaryKeyA[j].toString()))
                            found = false;
                    i++;
                }
                if (found) {
                    Object currentValue = rows.get(i - 1).get(column.getName());
                    if (!(oldValue == null && currentValue == null)
                            && ((oldValue == null && currentValue != null)
                                    || (oldValue != null && currentValue == null) || (!currentValue.toString()
                                    .equals(oldValue.toString())))) {
                        String error = "Warning: old value in row not equal to expected one. Table:["
                                + table.getName() + "] PK[: ";
                        for (int j = 0; j < primaryKeys.length; j++) {
                            if (j > 0)
                                error += ",";
                            error += primaryKeys[j];
                        }
                        System.out.println(error + "] Old Value found: "
                                + currentValue + " Old value expected "
                                + oldValue);
                    }
                    rows.get(i - 1).set(column.getName(), newValue);
                } else {
                    String error = "We didn't found the row that we wanted to change. Table:["
                            + table.getName() + "] PK[: ";
                    for (i = 0; i < primaryKeys.length; i++) {
                        if (i > 0)
                            error += ",";
                        error += primaryKeys[i];
                    }
                    System.out.println(error + "]");
                }

            }
        }
    }

    public void reorderAllTables() {
        for (int i = 0; i < _model.getTableCount(); i++)
            DataToArraySink.sortArray(_model, getRowsFromTable(_model.getTable(
                    i).getName()));
    }

    public Database getDatabase() {
        return _model;
    }
}
