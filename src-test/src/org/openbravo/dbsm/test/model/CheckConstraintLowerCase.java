package org.openbravo.dbsm.test.model;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;

public class CheckConstraintLowerCase extends DbsmTest {

  public CheckConstraintLowerCase(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  public void lowerCaseIsPreservedInCheckConstraints() throws IOException {
    createDatabase("constraints/CHK_LOWER_CASE.xml");
    assertExportIsConsistent("constraints/CHK_LOWER_CASE.xml");
  }
}
