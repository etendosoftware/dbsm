package org.apache.ddlutils.alteration;

import java.util.HashMap;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;

public class RemoveRowChange implements DataChange{
	
	Table _table;
	DynaBean _row;
	
	public RemoveRowChange(Table table, DynaBean row)
	{
		_table=table;
		_row=row;
	}
	
	public void apply(DatabaseData databaseData, boolean caseSensitive)
	{
		databaseData.removeRow(_table, _row);
	}
	
	public String toString()
	{
		return "Row removed from table ["+_table.getName()+"]: <"+_row+">";
	}
	
	public DynaBean getRow()
	{
		return _row;
	}
	
	public Table getTable()
	{
		return _table;
	}
	
	
}