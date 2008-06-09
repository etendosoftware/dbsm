package org.apache.ddlutils.alteration;

import java.util.HashMap;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;

public class AddRowChange implements DataChange{
	
	Table _table;
	DynaBean _row;
	
	public AddRowChange(Table table, DynaBean row)
	{
		_table=table;
		_row=row;
	}
	
	public void apply(HashMap<String, Vector<DynaBean>> databaseBeans, boolean caseSensitive)
	{
		
	}
	
	public String toString()
	{
		return "New row in table ["+_table.getName()+"]: <"+_row+">";
	}
	
	
}