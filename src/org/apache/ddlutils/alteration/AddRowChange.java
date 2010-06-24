package org.apache.ddlutils.alteration;

import org.apache.commons.beanutils.DynaBean;
import org.apache.ddlutils.dynabean.SqlDynaClass;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;

public class AddRowChange implements DataChange {

  Table _table;
  DynaBean _row;

  public AddRowChange(Table table, DynaBean row) {
    _table = table;
    _row = row;
  }

  public boolean apply(DatabaseData databaseData, boolean caseSensitive) {
    SqlDynaClass dynaClass = (SqlDynaClass) _row.getDynaClass();
    dynaClass.resetDynaClass(databaseData.getDatabase().findTable(_table.getName()));
    return (databaseData.addRow(_table, _row, false));
  }

  public boolean applyInReverse(DatabaseData databaseData, boolean caseSensitive) {
    // Not implemented, as a configuration script cannot contain this kind of change
    return false;
  }

  public String toString() {
    return "New row in table [" + _table.getName() + "]: <" + _row + ">";
  }

  public DynaBean getRow() {
    return _row;
  }

  public Table getTable() {
    return _table;
  }

}