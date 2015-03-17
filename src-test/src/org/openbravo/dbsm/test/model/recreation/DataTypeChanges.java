package org.openbravo.dbsm.test.model.recreation;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;

public class DataTypeChanges extends TableRecreationBaseTest {

  public DataTypeChanges(String rdbms, String driver, String url, String sid, String user,
      String password, String name, Type type) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, type);
  }

  @Test
  public void changeDecimalTypeSize() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE1.xml");
  }

  @Test
  public void changeVarcharTypeSize() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE2.xml");
  }

  @Test
  public void changeCharTypeSize() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE3.xml");
  }
}
