package org.apache.ddlutils.alteration;

import java.util.HashMap;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;

public class AddRowChange implements DataChange{
	
	Table _table;
	DynaBean _row;
	
	public AddRowChange(Table table, DynaBean row)
	{
		_table=table;
		_row=row;
	}
	
	public void apply(DatabaseData databaseData, boolean caseSensitive)
	{
		databaseData.addRow(_table, _row, false);
	}
	
	public String toString()
	{
		return "New row in table ["+_table.getName()+"]: <"+_row+">";
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