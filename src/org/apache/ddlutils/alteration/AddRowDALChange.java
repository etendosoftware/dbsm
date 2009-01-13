package org.apache.ddlutils.alteration;

import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;
import org.openbravo.base.structure.BaseOBObject;

public class AddRowDALChange implements DataChange {

    Table _table;
    BaseOBObject _row;

    public AddRowDALChange(Table table, BaseOBObject row) {
        _table = table;
        _row = row;
    }

    public void apply(DatabaseData databaseData, boolean caseSensitive) {
        // databaseData.addRow(_table, _row, false);
    }

    public String toString() {
        return "New row in table [" + _table.getName() + "]: <" + _row + ">";
    }

    public BaseOBObject getRow() {
        return _row;
    }

    public Table getTable() {
        return _table;
    }

}