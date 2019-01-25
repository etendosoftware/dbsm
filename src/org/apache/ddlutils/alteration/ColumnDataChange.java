package org.apache.ddlutils.alteration;

import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;

public class ColumnDataChange implements DataChange {

  Table _table;
  Column _column;
  String _oldValue;
  String _newValue;
  Object _pkRow;
  String _tablename;
  String _columnname;

  public ColumnDataChange() {

  }

  public ColumnDataChange(String tablename, String columnname, String oldValue, Object pkRow) {
    this._tablename = tablename;
    this._columnname = columnname;
    this._oldValue = oldValue;
    this._pkRow = pkRow;
  }

  public ColumnDataChange(Table table, Column column, String oldValue, String newValue,
      Object pkRow) {
    _table = table;
    _column = column;
    _oldValue = oldValue;
    _newValue = newValue;
    _pkRow = pkRow;
    _tablename = table.getName();
    _columnname = column.getName();

  }

  @Override
  public boolean apply(DatabaseData databaseData, boolean caseSensitive) {
    if (_table == null) {
      _table = databaseData.getDatabase().findTable(_tablename);
    }
    if (_column == null) {
      _column = _table.findColumn(_columnname);
    }
    return (databaseData.changeRow(_table, _column, new Object[] { _pkRow }, _oldValue, _newValue));
  }

  @Override
  public boolean applyInReverse(DatabaseData databaseData, boolean caseSensitive) {
    if (_table == null) {
      _table = databaseData.getDatabase().findTable(_tablename);
    }
    if (_column == null) {
      _column = _table.findColumn(_columnname);
    }
    return (databaseData.changeRowInReverse(_table, _column, _pkRow, _oldValue, _newValue));
  }

  @Override
  public String toString() {
    return "[" + _tablename + "." + _columnname + " <" + _oldValue + "->" + _newValue + "> PK: "
        + _pkRow + "]";
  }

  public Table getTable() {
    return _table;
  }

  public Column getColumn() {
    return _column;
  }

  public String getOldValue() {
    return _oldValue;
  }

  public String getNewValue() {
    return _newValue;
  }

  public Object getPrimaryKey() {
    return _pkRow;
  }

  public String getColumnname() {
    return _columnname;
  }

  public String getTablename() {
    return _tablename;
  }

  public Object getPkRow() {
    return _pkRow;
  }

  public void setTablename(String tablename) {
    _tablename = tablename;
  }

  public void setColumnname(String columnname) {
    _columnname = columnname;
  }

  public void setPkRow(Object row) {
    _pkRow = row;
  }

  public void setOldValue(String oldValue) {
    _oldValue = oldValue;
  }

  public void setNewValue(String newValue) {
    _newValue = newValue;
  }

  public boolean equals(ColumnDataChange otherChange) {

    return this._tablename.equals(otherChange._tablename)
        && this._columnname.equals(otherChange._columnname)
        && ((_oldValue == null && otherChange._oldValue == null) || (_oldValue != null
            && otherChange._oldValue != null && this._oldValue.equals(otherChange._oldValue)))
        && ((_newValue == null && otherChange._newValue == null) || (_newValue != null
            && otherChange._newValue != null && this._newValue.equals(otherChange._newValue)))
        && this._pkRow.equals(otherChange._pkRow);
  }
}