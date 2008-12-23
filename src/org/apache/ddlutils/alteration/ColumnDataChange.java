package org.apache.ddlutils.alteration;

import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;

public class ColumnDataChange implements DataChange {

    Table _table;
    Column _column;
    Object _oldValue;
    Object _newValue;
    Object _pkRow;
    String _tablename;
    String _columnname;

    public ColumnDataChange() {

    }

    public ColumnDataChange(String tablename, String columnname,
            Object oldValue, Object pkRow) {
        this._tablename = tablename;
        this._columnname = columnname;
        this._oldValue = oldValue;
        this._pkRow = pkRow;
    }

    public ColumnDataChange(Table table, Column column, Object oldValue,
            Object newValue, Object pkRow) {
        _table = table;
        _column = column;
        _oldValue = oldValue;
        _newValue = newValue;
        _pkRow = pkRow;
        _tablename = table.getName();
        _columnname = column.getName();

    }

    public void apply(DatabaseData databaseData, boolean caseSensitive) {
        // databaseData.changeRow(_table, _column, _pkRow, _oldValue,
        // _newValue);
    }

    @Override
    public String toString() {
        String string = "Change in column [" + _columnname + "] in table ["
                + _tablename + "]: PK:" + _pkRow;
        return string + " Old Value: <" + _oldValue + "> New Value: <"
                + _newValue + ">";
    }

    public Table getTable() {
        return _table;
    }

    public Column getColumn() {
        return _column;
    }

    public Object getOldValue() {
        return _oldValue;
    }

    public Object getNewValue() {
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

    public void setOldValue(Object oldValue) {
        _oldValue = oldValue;
    }

    public void setNewValue(Object newValue) {
        _newValue = newValue;
    }
}