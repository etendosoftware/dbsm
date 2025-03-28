package org.apache.ddlutils.io;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Comparator;
import java.util.Vector;

import org.apache.commons.beanutils.BeanComparator;
import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.collections4.comparators.ComparatorChain;
import org.apache.ddlutils.dynabean.SqlDynaClass;
import org.apache.ddlutils.dynabean.SqlDynaProperty;
import org.apache.ddlutils.model.Database;

public class DataToArraySink implements DataSink {
  Vector<DynaBean> beanArray;

  @Override
  public void addBean(DynaBean bean) throws DataSinkException {
    beanArray.add(bean);
    // System.out.println(bean);

  }

  @Override
  public void end() throws DataSinkException {

  }

  @Override
  public void start() throws DataSinkException {
    beanArray = new Vector<DynaBean>();

  }

  public Vector<DynaBean> getVector() {
    return beanArray;
  }

  public static void sortArray(Database database, Vector<DynaBean> beanVector) {
    if (beanVector == null || beanVector.size() == 0) {
      return;
    }

    DynaBean firstBean = beanVector.get(0);
    SqlDynaClass dynaClass = database.getDynaClassFor(firstBean);
    SqlDynaProperty[] primaryKeys = dynaClass.getPrimaryKeyProperties();
    ComparatorChain chain = new ComparatorChain();
    for (int i = 0; i < primaryKeys.length; i++) {
      Comparator<DynaBean> comp = new BeanComparator(primaryKeys[i].getName(),
          new BaseOBIDHexComparator());
      chain.addComparator(comp, false);

    }

    Collections.sort(beanVector, chain);

  }

  private static class BaseOBIDHexComparator implements Comparator<Object> {

    @Override
    public int compare(Object o1, Object o2) {
      final String bob1 = o1.toString();
      final String bob2 = o2.toString();
      try {
        BigInteger bd1 = new BigInteger(bob1, 32);
        BigInteger bd2 = new BigInteger(bob2, 32);
        return bd1.compareTo(bd2);
      } catch (NumberFormatException n) {
        System.out.println("problem: " + n.getMessage());
        return 0;
      }
    }
  }
}
