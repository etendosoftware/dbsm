package org.openbravo.dbsm.test.model.recreation;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

public class AddDropConstraints extends TableRecreationBaseTest {

  public AddDropConstraints(String rdbms, String driver, String url, String sid, String user,
      String password, String name, Type type) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, type);
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
    assertTablesAreNotRecreated("FK_BASE.xml", "FK.xml", false);
  }
}
