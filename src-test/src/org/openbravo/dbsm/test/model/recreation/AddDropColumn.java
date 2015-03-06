package org.openbravo.dbsm.test.model.recreation;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;
import org.openbravo.dbsm.test.base.PGOnlyDbsmTest;

public class AddDropColumn extends PGOnlyDbsmTest {
  private final static String TEST_TABLE_NAME = "TEST";

  public AddDropColumn(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  public void appendColumnRecreatesTable() throws SQLException {
    resetDB();
    updateDatabase("recreation/COL1.xml");
    String oldTableInternalId = getTableDBOId(TEST_TABLE_NAME);
    updateDatabase("recreation/COL2.xml");
    String newTableInternalId = getTableDBOId(TEST_TABLE_NAME);
    assertThat(oldTableInternalId, not(equalTo(newTableInternalId)));
  }

  @Test
  public void dropColumnRecreatesTable() throws SQLException {
    resetDB();
    updateDatabase("recreation/COL2.xml");
    String oldTableInternalId = getTableDBOId(TEST_TABLE_NAME);
    updateDatabase("recreation/COL1.xml");
    String newTableInternalId = getTableDBOId(TEST_TABLE_NAME);
    assertThat(oldTableInternalId, not(equalTo(newTableInternalId)));
  }

  private String getTableDBOId(String testTableName) throws SQLException {
    Connection cn = null;
    try {
      cn = getDataSource().getConnection();

      PreparedStatement st;
      if (getRdbms() == Rdbms.PG) {
        st = cn.prepareStatement("select oid from pg_class where relname = lower(?)");
      } else {
        st = cn.prepareStatement("select object_id from user_objects where object_name = upper(?)");
      }
      st.setString(1, testTableName);

      ResultSet rs = st.executeQuery();

      rs.next();
      return rs.getString(1);
    } finally {
      if (cn != null) {
        cn.close();
      }
    }
  }
}
