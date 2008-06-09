package org.apache.ddlutils.alteration;

import java.util.HashMap;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;

public class ColumnDataChange implements DataChange{
	
	Table _table;
	Column _column;
	Object _oldValue;
	Object _newValue;
	
	public ColumnDataChange(Table table, Column column, Object oldValue, Object newValue)
	{
		_table=table;
		_column=column;
		_oldValue=oldValue;
		_newValue=newValue;
	}
	
	public void apply(HashMap<String, Vector<DynaBean>> databaseBeans, boolean caseSensitive)
	{
		
	}
	
	public String toString()
	{
		return "Change in column ["+_column.getName()+"] in table ["+_table.getName()+"]: Old Value: <"+_oldValue+"> New Value: <"+_newValue+">";
	}
	
	
}