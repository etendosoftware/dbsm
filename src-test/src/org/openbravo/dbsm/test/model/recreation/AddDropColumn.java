package org.openbravo.dbsm.test.model.recreation;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

public class AddDropColumn extends TableRecreationBaseTest {

  public AddDropColumn(String rdbms, String driver, String url, String sid, String user,
      String password, String name, Type type) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, type);
  }

  @Test
  public void nonMandatoryColumn_COL1() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "COL1.xml");
  }

  @Test
  public void mandatoryColumnWithDefault_COL2() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "COL2.xml");
  }

  @Test
  public void mandatoryColumnWithOnCreateDefault_COL3() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "COL3.xml");
  }

  @Test
  public void nonMandatoryColumnWithDefault_COL4() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "COL4.xml");
  }

  @Test
  public void nonMandatoryColumnWithOnCreateDefault_COL5() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "COL5.xml");
  }

  @Test
  public void twoNonMandatoryColumns_COL6() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "COL6.xml");
  }
}
