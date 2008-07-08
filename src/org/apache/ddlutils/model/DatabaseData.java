package org.apache.ddlutils.model;

import java.io.File;
import java.util.HashMap;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.ddlutils.dynabean.SqlDynaProperty;
import org.apache.ddlutils.io.DataToArraySink;

public class DatabaseData{
	
	protected Database _model;
	protected HashMap<String, Vector<DynaBean>> _databaseBeans;
	
	public DatabaseData(Database model)
	{
		_model=model;
		_databaseBeans=new HashMap<String, Vector<DynaBean>>();
	}
	
	public void insertDynaBeansFromVector(String tablename,Vector<DynaBean> vector)
	{
		if(_databaseBeans.containsKey(tablename))
			_databaseBeans.get(tablename).addAll(vector);
		else
			_databaseBeans.put(tablename, vector);
		DataToArraySink.sortArray(_model, _databaseBeans.get(tablename));
	}
	
	public Vector<DynaBean> getRowsFromTable(String tablename)
	{
		return _databaseBeans.get(tablename);
	}
	
	public void removeRow(Table table, DynaBean row)
	{
		System.out.println("Trying to remove row "+row+"f rom table "+table);
		Vector<DynaBean> rows=_databaseBeans.get(table.getName());
		if(rows==null)
		{
			System.out.println("Error. Trying to remove row in table "+table.getName()+". The table doesn't exist, or is empty.");
			return;
		}
		SqlDynaProperty[] primaryKeys=_model.getDynaClassFor(row).getPrimaryKeyProperties();
		int i=0;
		while(i<rows.size() && !row.equals(rows.get(i)))
			i++;
		if(i<rows.size() && row.equals(rows.get(i)))
			rows.remove(i);
		else
		{
			System.out.println("We haven't found the row we wanted to remove. We will search by just primary key.");
			i=0;
			boolean found=false;
			while(i<rows.size() && !found)
			{
				found=true;
				SqlDynaProperty[] primaryKeysA=_model.getDynaClassFor(rows.get(i)).getPrimaryKeyProperties();
				for(int j=0;j<primaryKeys.length && found;j++)
				{
					if(!row.get(primaryKeys[j].getName()).equals(rows.get(i).get(primaryKeysA[j].getName())))
						found=false;
				}
				i++;
			}
			if(found)
			{
				System.out.println("We found a row with the same Primary Key. We will remove it despite it was not exactly the same.");
				rows.remove(i-1);
			}
			else
			{
				String error="We didn't found the row that we wanted to change. Table:["+table.getName()+"] PK[: ";
				for(i=0;i<primaryKeys.length;i++)
				{
					if(i>0) error+=",";
					error+=row.get(primaryKeys[i].getName());
				}
				System.out.println(error+"]");
			}
		}
	}
	
	public void addRow(Table table, DynaBean row, boolean reorder)
	{
		System.out.println("Trying to add row "+row+" in table "+table);
		if(_model.findTable(table.getName())==null)
		{
			System.out.println("Error: impossible to add row in table "+table+", as the table doesn't exist.");
		}
		else
		{
			if(!_databaseBeans.containsKey(table.getName()))
			{
				_databaseBeans.put(table.getName(), new Vector<DynaBean>());	
			}
			_databaseBeans.get(table.getName()).add(row);
			if(reorder)
				DataToArraySink.sortArray(_model, _databaseBeans.get(table.getName()));
		}
	}
	
	public void changeRow(Table table, Column column, Object[] primaryKeys, Object oldValue, Object newValue)
	{
		System.out.println("Trying to change table "+table.getName()+", column "+column.getName()+" from "+oldValue+" to "+newValue);
		if(_model.findTable(table.getName())==null)
		{
			System.out.println("Error: impossible to change row in table "+table+", as the table doesn't exist.");
		}
		else
		{
			if(_model.findTable(table.getName()).findColumn(column.getName())==null)
			{
				System.out.println("Error: impossible to change row in table "+table+", as the column "+column.getName()+" doesn't exist.");
			}
			else
			{
				Vector<DynaBean> rows=_databaseBeans.get(table.getName());
				int i=0;
				boolean found=false;
				while(i<rows.size() && !found)
				{
					found=true;
					SqlDynaProperty[] primaryKeysCols=_model.getDynaClassFor(rows.get(i)).getPrimaryKeyProperties();
					Object[] primaryKeyA=new Object[primaryKeysCols.length];
					for(int j=0;j<primaryKeyA.length;j++)
						primaryKeyA[j]=rows.get(i).get(primaryKeysCols[j].getName());
					for(int j=0;j<primaryKeys.length && found;j++)
						if(!primaryKeys[j].equals(primaryKeyA[j]))
							found=false;
					i++;
				}
				if(found)
				{
					Object currentValue=rows.get(i-1).get(column.getName());
					if(!(oldValue==null && currentValue==null) && ((oldValue==null && currentValue!=null) || (oldValue!=null && currentValue==null) || (!currentValue.equals(oldValue))))
					{
						String error="Warning: old value in row not equal to expected one. Table:["+table.getName()+"] PK[: ";
						for(i=0;i<primaryKeys.length;i++)
						{
							if(i>0) error+=",";
							error+=primaryKeys[i];
						}
						System.out.println(error+"] Old Value found: "+currentValue+" Old value expected "+oldValue);
					}
					rows.get(i-1).set(column.getName(), newValue);
				}
				else
				{
					String error="We didn't found the row that we wanted to change. Table:["+table.getName()+"] PK[: ";
					for(i=0;i<primaryKeys.length;i++)
					{
						if(i>0) error+=",";
						error+=primaryKeys[i];
					}
					System.out.println(error+"]");
				}
				
			}
		}
	}
	
	public void reorderAllTables()
	{
		for(int i=0;i<_model.getTableCount();i++)
			DataToArraySink.sortArray(_model, _databaseBeans.get(_model.getTable(i).getName()));
	}
}