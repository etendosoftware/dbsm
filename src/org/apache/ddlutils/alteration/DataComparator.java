package org.apache.ddlutils.alteration;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.dynabean.SqlDynaClass;
import org.apache.ddlutils.dynabean.SqlDynaProperty;
import org.apache.ddlutils.io.DatabaseFilter;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;

public class DataComparator {
	
    /** The log for this comparator. */
    private final Log _log = LogFactory.getLog(ModelComparator.class);

    /** The platform information. */
    private PlatformInfo _platformInfo;
    /** Whether comparison is case sensitive. */
    private boolean _caseSensitive;
    private Vector<DataChange> dataChanges=new Vector<DataChange>();
    private DatabaseFilter _databasefilter = null;
	
	public DataComparator(PlatformInfo platformInfo, boolean caseSensitive)
    {
        _platformInfo  = platformInfo;
        _caseSensitive = caseSensitive;
    }
	
	public void setFilter(DatabaseFilter filter)
	{
		_databasefilter=filter;
	}

	public void compare(Database originaldb, Database currentdb, Platform platform, HashMap<String, Vector<DynaBean>> originalData)
	{
		ModelComparator modelComparator=new ModelComparator(_platformInfo, _caseSensitive);
		List modelChanges=modelComparator.compare(originaldb, currentdb);
		System.out.println(_databasefilter);
		//First, we will find the common tables
		Vector<Table> commonTables=new Vector<Table>();
		Vector<Table> newTables=new Vector<Table>();
		
		Table[] tables=currentdb.getTables();
		
		String[] tablenames=null;
		if(_databasefilter!=null)
			tablenames=_databasefilter.getTableNames();
		for(int i=0;i<tables.length;i++)
		{
			boolean include=true;
			if(tablenames!=null)
			{
				include=false;
				int j=0;
				while(j<tablenames.length && !tablenames[j].equals(tables[i].getName()))
					j++;
				if(j<tablenames.length)
					include=true;
			}
			if(include)
			{
				commonTables.add(tables[i]);
			}
		}
		for(int i=0;i<modelChanges.size();i++)
		{
			if(modelChanges.get(i) instanceof AddTableChange)
			{
				Table table=((AddTableChange)modelChanges.get(i)).getNewTable();
				commonTables.remove(table);
				newTables.add(table);
			}
		}
		
		
		//Now we will compare tables. If tables are equivalent in both models, we will compare rows.
		for(int i=0;i<commonTables.size();i++)
		{
			Table table=commonTables.get(i);
			if(table.getPrimaryKeyColumns()==null || table.getPrimaryKeyColumns().length==0)
			{
				_log.warn("Error: Table "+table.getName()+" could not be compared because it doesn't have primary key.");
				continue;
			}
			_log.debug("Comparing table: "+table.getName());
			int indC=0;
			Vector<ModelChange> changesToTable=new Vector<ModelChange>();
			while(indC<modelChanges.size())
			{
				ModelChange change=(ModelChange)modelChanges.get(indC);
				if(change instanceof AddColumnChange)
				{
					if(((AddColumnChange)change).getChangedTable().getName().equalsIgnoreCase(table.getName()))
						changesToTable.add(change);
				}
				else if(change instanceof RemoveColumnChange)
				{
					if(((RemoveColumnChange)change).getChangedTable().getName().equalsIgnoreCase(table.getName()))
					{
						changesToTable.add(change);
					}
				}
				indC++;
			}
			if(!changesToTable.isEmpty())
			{
				for(indC=0;indC<changesToTable.size();indC++)
				{
					ModelChange change=changesToTable.get(indC);
					if(change instanceof RemoveColumnChange)
					{
						//A column has been deleted in the database. We have to delete it from the original model
						//because if not we will have an error 
						_log.debug("A column has been deleted in the database and we have to delete it in our original model.");
						((RemoveColumnChange)change).apply(originaldb, false);
					}
					else if(change instanceof AddColumnChange)
					{
						//We will read all the values of the new column, and add them
						//as ColumnDataChanges
						Connection connection=platform.borrowConnection();
				        Iterator   answer     = null;

						Table tableC=((AddColumnChange)change).getChangedTable();
						Column columnC=((AddColumnChange)change).getNewColumn();
						answer=readRowsFromTable(connection, platform, currentdb , tableC, _databasefilter);
						while(answer!=null && answer.hasNext())
						{
							DynaBean db=(DynaBean)answer.next();
							Object value=null;
							try{
								value=db.get(columnC.getName());
							}catch(Exception e)
							{
								value=db.get(columnC.getName().toLowerCase());
							}
							dataChanges.add(new ColumnDataChange(tableC,columnC,null,db.get(columnC.getName())));
						}
					}
				}
			}
			
			//Tables can now be compared.
			Connection connection=platform.borrowConnection();
	        Iterator   answer     = null;

			answer=readRowsFromTable(connection, platform, currentdb , table, _databasefilter);

			Vector<DynaBean> rowsOriginalData=originalData.get(table.getName());
			//We now have the rows of the table in the database (answer)
			//and the rows in the XML files (HashMap originalData)
			compareTables(originaldb, table, rowsOriginalData, answer);
			try
			{
				if(!connection.isClosed())
					connection.close();
			}catch(Exception ex)
			{
	            _log.error(ex.getLocalizedMessage());
			}
		}
		
		for(int i=0;i<newTables.size();i++)
		{
			Table table=newTables.get(i);
			Connection connection=platform.borrowConnection();
	        Iterator   answer     = null;

			answer=readRowsFromTable(connection, platform, currentdb , table, _databasefilter);

			while(answer.hasNext())
			{
				//Each row of a new table is a new row
				DynaBean db=(DynaBean)answer.next();
				dataChanges.add(new AddRowChange(table, db));
			}
			try
			{
			}catch(Exception ex)
			{
	            _log.error(ex.getLocalizedMessage());
			}
			
		}
		
	}
	
	private Iterator readRowsFromTable(Connection connection, Platform platform, Database model, Table table, DatabaseFilter filter)
	{
		if(table.getPrimaryKeyColumns()==null || table.getPrimaryKeyColumns().length==0)
		{
			_log.error("Table "+table.getName()+" cannot be read because it has no primary key.");
			return null;
		}

		Table[] atables={table};
		Statement  statement  = null;
        ResultSet  resultSet  = null;
        try
        {
            statement = connection.createStatement();

            String sqlstatement="SELECT * FROM "+table.getName();
            if(filter!=null && !filter.getTableFilter(table.getName()).equals(""))
            	sqlstatement+=" WHERE "+filter.getTableFilter(table.getName())+" ";
            sqlstatement+=" ORDER BY ";
            for(int j=0;j<table.getPrimaryKeyColumns().length;j++)
            {
            	if(j>0)
            		sqlstatement+=",";
            	sqlstatement+=table.getPrimaryKeyColumns()[j].getName();
            }
            resultSet = statement.executeQuery(sqlstatement);
        }
        catch (SQLException ex)
        {
            _log.error(ex.getLocalizedMessage());
//	            throw new DatabaseOperationException("Error while performing a query", ex);
        }
        return platform.createResultSetIterator(model, resultSet, atables);
	}
	
	
	private void compareTables(Database model, Table table, Vector<DynaBean> originalData, Iterator iteratorTable)
	{
		if(iteratorTable==null)
		{
			_log.warn("Error while reading table "+table.getName()+". Probably it doesn't have primary key.");
			return;
		}
		if(originalData==null || originalData.size()==0)
		{
			//There is no data in the XML files. We add data from the database and leave
			if(iteratorTable.hasNext())
			{
	        	DynaBean dbNew=(DynaBean)iteratorTable.next();
	            while(iteratorTable.hasNext())
	            {
	            	dataChanges.add(new AddRowChange(table, dbNew));
	            	//System.out.println("Row will be added: "+dbNew);
	            	dbNew=(DynaBean)iteratorTable.next();
	            }
	            dataChanges.add(new AddRowChange(table, dbNew));
			}
			return;
		}

        if (table.getPrimaryKeyColumns()==null || table.getPrimaryKeyColumns().length == 0)
        {
            _log.error("Cannot compare table "+table.getName()+" because it doesn't have a primary key");
            return;
        }

        if(!iteratorTable.hasNext())
        {
        	//There is no data in the table. Everything must be transformed into RemoveRowChanges
            for(int i=0;i<originalData.size();i++)
            {
            	dataChanges.add(new RemoveRowChange(table, originalData.get(i)));
            }
        	return;
        }
        int indOrg=0;
        DynaBean dbOrg=originalData.get(indOrg);
        DynaBean dbNew=(DynaBean)iteratorTable.next();
        while(indOrg<originalData.size() && iteratorTable.hasNext())
        {
        	dbOrg=originalData.get(indOrg);
        	int comp=comparePKs(model, dbOrg, dbNew);
        	if(comp==0) //Rows have the same PKs, we have to compare them
        	{
        		compareRows(model, dbOrg, dbNew);
            	dbNew=(DynaBean)iteratorTable.next();
            	indOrg++;
        	}
        	else if(comp==-1) //Original model has additional rows, we have to "delete" them
        	{
        		dataChanges.add(new RemoveRowChange(table, dbOrg));
        		//System.out.println("_i.Row will be deleted: "+dbOrg);
        		indOrg++;
        	}
        	else if(comp==1) //Target model has additional rows, we have to "add" them
        	{
        		dataChanges.add(new AddRowChange(table, dbNew));
        		//System.out.println("_i.Row will be added: "+dbNew);
        		dbNew=(DynaBean)iteratorTable.next();
        		
        	}
        	else if(comp==-2)
        	{
        		_log.error("Error: non numeric primary key in table "+table.getName()+".");
        		return;
        	}
        }
        if(!iteratorTable.hasNext() && indOrg>=originalData.size())
        {
        	//We've exited the loop when both conditions have not been fulfilled. This means that
        	//the last row of the database has not been compared with anything, and in fact is a new row.
        	//We have to add it.
        	dataChanges.add(new AddRowChange(table, dbNew));
        }
        else if(indOrg<originalData.size() && !iteratorTable.hasNext())
        {
        	//There are rows in the XML files, but not in the database. We have to be careful with the last row of the database.
            while(indOrg<originalData.size())
            {
            	dbOrg=originalData.get(indOrg);
            	if(dbNew!=null)
            	{
	                int comp=comparePKs(model, dbOrg, dbNew);
	            	if(comp==0) //Rows have the same PKs, we have to compare them
	            	{
	            		compareRows(model, dbOrg, dbNew);
	            		dbNew=null;
	            	}
	            	else if(comp==-1) //Original model has additional rows, we have to "delete" them
	            	{
	            		dataChanges.add(new RemoveRowChange(table, dbOrg));
	            		//System.out.println("_i.Row will be deleted: "+dbOrg);
	            	}
	            	else if(comp==1) //Target model has additional rows, we have to "add" them
	            	{
	            		dataChanges.add(new AddRowChange(table, dbNew));
	            		//System.out.println("_i.Row will be added: "+dbNew);
	            	}
            	}
            	else
            	{
            		dataChanges.add(new RemoveRowChange(table, dbOrg));
            		//System.out.println("Row will be deleted: "+dbOrg);
            	}
            	indOrg++;
            }
        }
        else if(iteratorTable.hasNext())
        {
        	//No rows remaining in the XML files. We will add all the remaining rows of the database.

            while(iteratorTable.hasNext())
            {
            	dataChanges.add(new AddRowChange(table, dbNew));
            	//System.out.println("Row will be added: "+dbNew);
            	dbNew=(DynaBean)iteratorTable.next();
            }
            dataChanges.add(new AddRowChange(table, dbNew));
        	//System.out.println("Row will be added: "+dbNew);
        }
        
        

	}
	
	private int comparePKs(Database model, DynaBean db1, DynaBean db2)
	{
        SqlDynaClass      dynaClass   = model.getDynaClassFor(db1);
        SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();
        for(int i=0;i<primaryKeys.length;i++)
        {
        	try
        	{
        	int pk1=Integer.parseInt(db1.get(primaryKeys[i].getName()).toString());
        	int pk2=Integer.parseInt(db2.get(primaryKeys[i].getName()).toString());
        	if(pk1<pk2)
        		return -1;
        	else if(pk1>pk2)
        		return 1;
        	}
        	catch(Exception e)
        	{
        		return -2;
        	}
        }
        return 0;
		
	}
	
	private void compareRows(Database model, DynaBean db1, DynaBean db2)
	{

        SqlDynaClass      dynaClass   = model.getDynaClassFor(db1);
        SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();
        SqlDynaProperty[] nonprimaryKeys = dynaClass.getNonPrimaryKeyProperties();
        String pk="[";
        for(int i=0;i<primaryKeys.length;i++)
        {
        	pk+=primaryKeys[i].getName()+"="+db1.get(primaryKeys[i].getName())+";";
        }
        pk+="]";
        Vector<String> tablesModel=new Vector<String>();
        for(int i=0;i<model.getTableCount();i++)
        	tablesModel.add(model.getTable(i).getName());
        for(int i=0;i<nonprimaryKeys.length;i++)
        {
        	if(tablesModel.contains(nonprimaryKeys[i].getName()))
        	{
	        	//System.out.println(nonprimaryKeys[i].getName());
	        	Object v1=db1.get(nonprimaryKeys[i].getName());
	        	Object v2=db2.get(nonprimaryKeys[i].getName());
	        	
	        	if((v1==null && v2!=null) ||
	        	   (v1!=null && v2==null) ||
	        	   (v1!=null && v2!=null && !v1.equals(v2)))
	        	{
	        		dataChanges.add(new ColumnDataChange(dynaClass.getTable(),nonprimaryKeys[i].getColumn(),v1,v2));
	        		//System.out.println("Column change: "+pk+"["+nonprimaryKeys[i].getName()+"]:"+v1+","+v2);
	        	}
        	}
        }
	}
	
	public Vector<DataChange> getChanges()
	{
		return dataChanges;
	}
}
