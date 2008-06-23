package org.apache.ddlutils.alteration;

import java.util.HashMap;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.ddlutils.dynabean.SqlDynaProperty;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.apache.ddlutils.model.Table;

public class ColumnDataChange implements DataChange{
	
	Table _table;
	Column _column;
	Object _oldValue;
	Object _newValue;
	Object[] _pkRow;
	
	public ColumnDataChange(Table table, Column column, Object oldValue, Object newValue, Object[] pkRow)
	{
		_table=table;
		_column=column;
		_oldValue=oldValue;
		_newValue=newValue;
		_pkRow=pkRow;
	}
	
	public void apply(DatabaseData databaseData, boolean caseSensitive)
	{
		databaseData.changeRow(_table, _column, _pkRow, _oldValue, _newValue);
	}
	
	public String toString()
	{
		String string="Change in column ["+_column.getName()+"] in table ["+_table.getName()+"]: PK:";
		for(int i=0;i<_pkRow.length;i++)
			string+=_pkRow[i];
		return string+" Old Value: <"+_oldValue+"> New Value: <"+_newValue+">";
	}
	
	public Table getTable()
	{
		return _table;
	}
	
	public Column getColumn()
	{
		return _column;
	}
	
	public Object getOldValue()
	{
		return _oldValue;
	}
	
	public Object getNewValue()
	{
		return _newValue;
	}
	
	public Object[] getPrimaryKey()
	{
		return _pkRow;
	}
}