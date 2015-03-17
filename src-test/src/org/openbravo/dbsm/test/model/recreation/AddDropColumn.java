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
  public void lastNonMandatoryColumn() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "COL1.xml");
  }

  @Test
  public void lastMandatoryColumnWithDefault() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "COL2.xml");
  }

  @Test
  public void lastMandatoryColumnWithOnCreateDefault() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "COL3.xml");
  }

  @Test
  public void lastNonMandatoryColumnWithDefault() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "COL4.xml");
  }

  @Test
  public void lastNonMandatoryColumnWithOnCreateDefault() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "COL5.xml");
  }

  @Test
  public void lastTwoNonMandatoryColumn() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "COL6.xml");
  }

  @Test
  public void index() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "IDX.xml");
  }

  @Test
  public void unique() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "UNIQUE.xml");
  }

  @Test
  public void fk() {
    assertTablesAreNotRecreated("FK_BASE.xml", "FK.xml");
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
