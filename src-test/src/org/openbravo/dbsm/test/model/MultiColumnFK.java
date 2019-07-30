package org.openbravo.dbsm.test.model;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;

public class MultiColumnFK extends DbsmTest {

  public MultiColumnFK(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  public void multiColumnFKsToPKShouldBeCreated() throws IOException {
    String model = "multiColumnFK/PK";
    createDatabase(model);
    assertExportIsConsistent(model);
  }

  @Test
  public void multiColumnFKsToUniqueShouldBeCreated() throws IOException {
    String model = "multiColumnFK/unique";
    createDatabase(model);
    assertExportIsConsistent(model);
  }
}
