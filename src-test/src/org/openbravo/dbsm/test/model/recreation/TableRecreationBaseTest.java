/*
 ************************************************************************************
 * Copyright (C) 2015 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */
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

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.codehaus.jettison.json.JSONException;
import org.junit.runners.Parameterized.Parameters;
import org.openbravo.dbsm.test.base.DbsmTest;

public class TableRecreationBaseTest extends DbsmTest {
  protected static final String MODEL_DIRECTORY = "recreation/";

  protected enum ActionType {
    append, prepend, drop
  }

  protected static List<ActionType> availableTypes = new ArrayList<ActionType>();
  private ActionType type;

  public TableRecreationBaseTest(String rdbms, String driver, String url, String sid, String user,
      String password, String name, ActionType type, DbsmTest.RecreationMode recMode)
      throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
    this.type = type;
    this.recreationMode = recMode;
  }

  @Parameters(name = "DB: {6} - {7} - recreation {8}")
  public static Collection<Object[]> parameters() throws IOException, JSONException {
    List<Object[]> configs = new ArrayList<Object[]>();

    for (String[] param : DbsmTest.params()) {
      for (ActionType type : availableTypes) {
        for (DbsmTest.RecreationMode recMode : DbsmTest.RecreationMode.values()) {
          List<Object> p = new ArrayList<Object>(Arrays.asList(param));
          p.add(type);
          p.add(recMode);
          configs.add(p.toArray());
        }
      }
    }
    return configs;
  }

  protected void assertTablesAreNotRecreated(String toModel) {
    String fromModel = type == ActionType.prepend ? "BASE_MODEL_PREPEND.xml" : "BASE_MODEL.xml";
    assertTablesAreNotRecreated(fromModel, toModel, true);
  }

  protected void assertTablesAreNotRecreated(String fromModel, String toModel) {
    assertTablesAreNotRecreated(fromModel, toModel, true);
  }

  protected void assertTablesAreNotRecreated(String fromModel, String toModel,
      boolean generateDummyData) {
    resetDB();
    try {
      String initialModel = MODEL_DIRECTORY
          + (type == ActionType.append || type == ActionType.prepend ? fromModel : toModel);
      log.info("Updating to " + initialModel);
      Database originalModel = updateDatabase(initialModel);

      if (generateDummyData) {
        generateData(originalModel, 10);
      }

      List<String> oldTableInternalId = getOIds(originalModel);

      String targetModel = MODEL_DIRECTORY
          + (type == ActionType.append || type == ActionType.prepend ? toModel : fromModel);
      Database newModel = updateDatabase(targetModel);

      log.info("Updating to " + newModel);
      if (recreationMode == RecreationMode.standard) {
        List<String> newTableInternalId = getOIds(newModel);
        assertThat("Table OID changed", newTableInternalId, contains(oldTableInternalId.toArray()));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception " + e.getMessage());
    }
  }

  protected List<String> getOIds(Database originalModel) throws SQLException {
    List<String> oids = new ArrayList<String>();
    for (Table table : originalModel.getTables()) {
      oids.add(table.getName() + ":" + getTableDBOId(table.getName()));
    }
    return oids;
  }

  protected String getTableDBOId(String testTableName) throws SQLException {
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
