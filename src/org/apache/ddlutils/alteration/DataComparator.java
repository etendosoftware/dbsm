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
	
	public DataComparator(PlatformInfo platformInfo, boolean caseSensitive)
    {
        _platformInfo  = platformInfo;
        _caseSensitive = caseSensitive;
    }

	public void compare(Database originaldb, Database currentdb, Platform platform, HashMap<String, Vector<DynaBean>> originalData)
	{
		ModelComparator modelComparator=new ModelComparator(_platformInfo, _caseSensitive);
		List modelChanges=modelComparator.compare(originaldb, currentdb);

		//First, we will find the common tables
		Vector<Table> commonTables=new Vector<Table>();
		Vector<Table> newTables=new Vector<Table>();
		
		Table[] tables=currentdb.getTables();

		for(int i=0;i<tables.length;i++)
		{
			commonTables.add(tables[i]);
		}
		for(int i=0;i<modelChanges.size();i++)
		{
			if(modelChanges.get(i) instanceof AddTableChange)
			{
				Table table=((AddTableChange)modelChanges.get(i)).getNewTable();
				System.out.println("New table"+table.getName());
				commonTables.remove(table);
				newTables.add(table);
			}
		}
		
		//Now we should create AddRowChanges for the new tables
		
		//Now we will compare tables. If tables are equivalent in both models, we will compare rows.
		for(int i=0;i<commonTables.size();i++)
		{
			Table table=commonTables.get(i);
			_log.debug("Comparing table: "+table.getName());
			int indC=0;
			Vector changesToTable=new Vector();
			while(indC<modelChanges.size())
			{
				Object change=modelChanges.get(indC);
				if(change instanceof AddColumnChange)
				{
					if(((AddColumnChange)change).getChangedTable().equals(table))
						changesToTable.add(change);
				}
				else if(change instanceof RemoveColumnChange)
				{
					if(((RemoveColumnChange)change).getChangedTable().equals(table))
						changesToTable.add(change);
				}
				indC++;
			}
			if(changesToTable.isEmpty())
			{
				//Table is identical in both models. We will now compare rows
				Table[] atables={table};
				Connection connection=platform.borrowConnection();
				Statement  statement  = null;
		        ResultSet  resultSet  = null;
		        Iterator   answer     = null;

		        try
		        {
		            statement = connection.createStatement();

					Vector<DynaBean> rowsOriginalData=originalData.get(table.getName());
		            String sqlstatement="SELECT * FROM "+table.getName();
		            if(rowsOriginalData!=null && rowsOriginalData.size()>0)
		            {
		            	DynaBean firstBean=rowsOriginalData.get(0);
		                SqlDynaClass      dynaClass   = currentdb.getDynaClassFor(firstBean);
		                SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();
		                sqlstatement+=" ORDER BY ";
		                for(int j=0;j<primaryKeys.length;j++)
		                {
		                	if(j>0)
		                		sqlstatement+=",";
		                	sqlstatement+=primaryKeys[j].getName();
		                }
		            	
		            }
		            resultSet = statement.executeQuery(sqlstatement);
		            answer    = platform.createResultSetIterator(currentdb, resultSet, atables);
					

					//We now have the rows of the table in the database (answer)
					//and the rows in the XML files (HashMap originalData)
					compareTables(originaldb, table, rowsOriginalData, answer);
					if(!connection.isClosed())
						connection.close();
		        }
		        catch (SQLException ex)
		        {
		            _log.error(ex.getLocalizedMessage());
//		            throw new DatabaseOperationException("Error while performing a query", ex);
		        }
				//Iterator it=platform.query(currentdb, "SELECT * FROM "+table.getName(), atables);

			}
			else
			{
				//Table is different. We'll think what to do.
			}
		}
		
	}
	
	
	private void compareTables(Database model, Table table, Vector<DynaBean> originalData, Iterator iteratorTable)
	{
		if(originalData==null || originalData.size()==0)
		{
			//_log.error("Original data for table "+table.getName()+" is empty");
			return;
		}
		DynaBean firstDB=originalData.get(0);

        SqlDynaClass      dynaClass   = model.getDynaClassFor(firstDB);
        SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();
        SqlDynaProperty[] nonprimaryKeys = dynaClass.getNonPrimaryKeyProperties();

        if (primaryKeys.length == 0)
        {
            _log.error("Cannot compare table "+table.getName()+" because it doesn't have a primary key");
            return;
        }
        
        if (nonprimaryKeys.length == 0) {
            _log.info("Cannot compare table "+table.getName()+" because it doesn't have data columns");
            return;
        }
        if(!iteratorTable.hasNext())
        {
        	//There is no data in the table. Everything must be transformed into RemoveRowChanges
        	//TODO: RemoveRowChanges
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
        		_log.error("Error: non numeric primary key in table"+table.getName()+".");
        		return;
        	}
        }
        if(!iteratorTable.hasNext() && indOrg>=originalData.size())
        {
        	//No rows remaining in database and no rows remaining in XML. We are finished.
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
        for(int i=0;i<nonprimaryKeys.length;i++)
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
	
	public Vector<DataChange> getChanges()
	{
		return dataChanges;
	}
}
