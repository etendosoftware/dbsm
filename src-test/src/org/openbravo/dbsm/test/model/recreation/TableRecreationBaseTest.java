package org.openbravo.dbsm.test.model.recreation;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.codehaus.jettison.json.JSONException;
import org.junit.runners.Parameterized.Parameters;
import org.openbravo.dbsm.test.base.DbsmTest;

public class TableRecreationBaseTest extends DbsmTest {
  private static final String MODEL_DIRECTORY = "recreation/";

  protected enum Type {
    add, drop
  }

  private Type type;;

  public TableRecreationBaseTest(String rdbms, String driver, String url, String sid, String user,
      String password, String name, Type type) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
    this.type = type;
  }

  @Parameters(name = "DB: {6} - {7}")
  public static Collection<Object[]> parameters() throws IOException, JSONException {
    List<Object[]> configs = new ArrayList<Object[]>();

    for (String[] param : DbsmTest.params()) {
      List<Object> p = new ArrayList<Object>(Arrays.asList(param));
      p.add(Type.add);
      configs.add(p.toArray());
      p = new ArrayList<Object>(Arrays.asList(param));
      p.add(Type.drop);
      configs.add(p.toArray());
    }
    return configs;
  }

  protected void assertTablesAreNotRecreated(String fromModel, String toModel) {
    resetDB();
    try {
      Database originalModel = updateDatabase(MODEL_DIRECTORY
          + (type == Type.add ? fromModel : toModel));

      generateData(originalModel);

      List<String> oldTableInternalId = getOIds(originalModel);

      Database newModel = updateDatabase(MODEL_DIRECTORY + (type == Type.add ? toModel : fromModel));
      List<String> newTableInternalId = getOIds(newModel);
      assertThat("Table OID changed", newTableInternalId, contains(oldTableInternalId.toArray()));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception " + e.getMessage());
    }
  }

  private void generateData(Database model) throws SQLException {

    for (Table table : model.getTables()) {
      for (int i = 0; i < 10; i++) {
        String sql = "insert into " + table.getName() + " (";
        boolean first = true;
        String values = "";
        for (Column col : table.getColumns()) {
          col.getName();
          if (!first) {
            sql += ", ";
            values += ", ";
          }
          System.out.println(col.getType() + " - " + col.getTypeCode() + " - " + col.getSize());
          first = false;
          sql += col.getName();
          if ("VARCHAR".equals(col.getType()) || "NVARCHAR".equals(col.getType())) {
            values += "'" + RandomStringUtils.randomAlphanumeric(col.getSizeAsInt()) + "'";
          } else if ("DECIMAL".equals(col.getType())) {
            values += RandomStringUtils.randomNumeric(col.getSizeAsInt());
          }
        }
        sql += ") values (" + values + ")";
        System.out.println(sql);
        Connection cn = null;
        try {
          cn = getDataSource().getConnection();

          PreparedStatement st = cn.prepareStatement(sql);
          st.execute();
        } finally {
          if (cn != null) {
            cn.close();
          }
        }
      }
    }

  }

  private List<String> getOIds(Database originalModel) throws SQLException {
    List<String> oids = new ArrayList<String>();
    for (Table table : originalModel.getTables()) {
      oids.add(table.getName() + ":" + getTableDBOId(table.getName()));
    }
    return oids;
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
