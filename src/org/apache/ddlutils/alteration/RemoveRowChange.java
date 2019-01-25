package org.apache.ddlutils.alteration;

import org.apache.commons.beanutils.DynaBean;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;

public class RemoveRowChange implements DataChange {

  Table _table;
  DynaBean _row;

  public RemoveRowChange(Table table, DynaBean row) {
    _table = table;
    _row = row;
  }

  @Override
  public boolean apply(DatabaseData databaseData, boolean caseSensitive) {
    return (databaseData.removeRow(_table, _row));
  }

  @Override
  public boolean applyInReverse(DatabaseData databaseData, boolean caseSensitive) {
    // Not implemented, as a configuration script cannot contain this kind of change
    return false;
  }

  @Override
  public String toString() {
    return "Row removed from table [" + _table.getName() + "]: <" + _row + ">";
  }

  public DynaBean getRow() {
    return _row;
  }

  public Table getTable() {
    return _table;
  }

}