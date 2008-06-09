package org.apache.ddlutils.io;

import java.util.Collections;
import java.util.Comparator;
import java.util.Vector;

import org.apache.commons.beanutils.BeanComparator;
import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.collections.comparators.ComparatorChain;
import org.apache.commons.collections.comparators.NullComparator;
import org.apache.ddlutils.dynabean.SqlDynaClass;
import org.apache.ddlutils.dynabean.SqlDynaProperty;
import org.apache.ddlutils.model.Database;

public class DataToArraySink implements DataSink {
	Vector<DynaBean> beanArray;

	public void addBean(DynaBean bean) throws DataSinkException {
		beanArray.add(bean);
		//System.out.println(bean);

	}

	public void end() throws DataSinkException {
		
	}

	public void start() throws DataSinkException {
		beanArray=new Vector<DynaBean>();

	}

	public Vector<DynaBean> getVector()
	{
		return beanArray;
	}
	public static void sortArray(Database database, Vector<DynaBean> beanVector)
	{
		if(beanVector==null || beanVector.size()==0)
			return;

    	DynaBean firstBean=beanVector.get(0);
        SqlDynaClass      dynaClass   = database.getDynaClassFor(firstBean);
        SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();
		ComparatorChain chain = new ComparatorChain();
        for(int i=0;i<primaryKeys.length;i++)
        {
    		Comparator<DynaBean> comp = new BeanComparator(primaryKeys[i].getName(), new NullComparator(true));
    		chain.addComparator(comp,false);
    		
        }

		Collections.sort(beanVector,chain);

        
        
	}
}
