package org.openbravo.dbsm.test.base;

import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Before;

/**
 * Base class to implement dbsm test cases to be executed only in PostgreSQL
 * 
 * @author alostale
 *
 */
public class PGOnlyDbsmTest extends DbsmTest {

  public PGOnlyDbsmTest(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Before
  public void executeOnlyInPG() {
    assumeThat("not executing in Oracle", getRdbms(), is(Rdbms.PG));
  }

}
