package org.openbravo.dbsm.test.model.recreation;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;

public class SQLCommands extends DbsmTest {

  public SQLCommands(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  public void severalTableAlterationsShouldBeInASingleStmt() {
    resetDB();
    updateDatabase("recreation/BASE_MODEL.xml");
    List<String> l = sqlStatmentsForUpdate("recreation/COL1.xml");
    String statements = "";
    int numberOfAlterTable = 0;
    for (String st : l) {
      statements += st;
      if (st.contains("ALTER TABLE TEST")) {
        numberOfAlterTable += 1;
      }
    }
    assertThat("Number of ALTER statements\n" + statements, numberOfAlterTable, is(1));
  }

}
