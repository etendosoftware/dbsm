package org.apache.ddlutils.alteration;

import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;
import org.openbravo.base.structure.BaseOBObject;

public class RemoveRowDALChange implements DataChange {

    Table _table;
    BaseOBObject _row;

    public RemoveRowDALChange(Table table, BaseOBObject row) {
        _table = table;
        _row = row;
    }

    public void apply(DatabaseData databaseData, boolean caseSensitive) {
        // databaseData.removeRow(_table, _row);
    }

    public String toString() {
        return "Row removed from table [" + _table.getName() + "]: <" + _row
                + ">";
    }

    public BaseOBObject getRow() {
        return _row;
    }

    public Table getTable() {
        return _table;
    }

}