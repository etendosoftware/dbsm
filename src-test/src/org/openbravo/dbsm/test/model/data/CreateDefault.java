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

package org.openbravo.dbsm.test.model.data;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

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
import org.codehaus.jettison.json.JSONException;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;
import org.openbravo.dbsm.test.base.DbsmTest;

/**
 * Test cases covering onCreateDefault behavior
 * 
 * @author alostale
 *
 */
public class CreateDefault extends DbsmTest {

  private static final String TEST_TABLE_NAME = "TEST";

  protected enum AdditionMode {
    append, addInTheMiddle
  }

  private AdditionMode mode;

  public CreateDefault(String rdbms, String driver, String url, String sid, String user,
      String password, String name, AdditionMode mode) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
    this.mode = mode;
  }

  /**
   * Adds new parameter to default ones to decide whether new column in following test cases is
   * appended or added between existent columns
   */
  @Parameters(name = "DB: {6} - {7}")
  public static Collection<Object[]> parameters() throws IOException, JSONException {
    List<Object[]> configs = new ArrayList<Object[]>();

    for (String[] param : DbsmTest.params()) {
      List<Object> p = new ArrayList<Object>(Arrays.asList(param));
      p.add(AdditionMode.append);
      configs.add(p.toArray());
      p = new ArrayList<Object>(Arrays.asList(param));
      p.add(AdditionMode.addInTheMiddle);
      configs.add(p.toArray());
    }
    return configs;
  }

  // ---- Non Mandatory columns ----

  @Test
  public void nonMandatoryDefault() throws SQLException {
    assertDefaults("NM1", "A");
  }

  @Test
  public void nonMandatoryOnCreateDefault() throws SQLException {
    assertDefaults("NM2", "A");
  }

  @Test
  public void nonMandatorySameDefaultAndOnCreateDefault() throws SQLException {
    assertDefaults("NM3", "A");
  }

  @Test
  public void nonMandatoryDifferentDefaultAndOnCreateDefault() throws SQLException {
    assertDefaults("NM4", "B");
  }

  // ---- Mandatory columns ----

  @Test
  public void mandatoryDefault() throws SQLException {
    assertDefaults("M1", "A");
  }

  @Test
  public void mandatoryOnCreateDefault() throws SQLException {
    assertDefaults("M2", "A");
  }

  @Test
  public void mandatorySameDefaultAndOnCreateDefault() throws SQLException {
    assertDefaults("M3", "A");
  }

  @Test
  public void mandatoryDifferentDefaultAndOnCreateDefault() throws SQLException {
    assertDefaults("M4", "B");
  }

  private void assertDefaults(String columnName, String value) throws SQLException {
    resetDB();
    Database originalModel = updateDatabase(mode == AdditionMode.append ? "createDefault/BASE_MODEL.xml"
        : "createDefault/BASE_MODEL2.xml");
    generateData(originalModel, 1);
    updateDatabase("createDefault/" + columnName + ".xml");
    assertThat("Value for column " + columnName, getActualValue(columnName), equalTo(value));
  }

  /** gets first value in DB for given column */
  private String getActualValue(String columnName) throws SQLException {
    Connection cn = null;
    try {
      cn = getDataSource().getConnection();

      PreparedStatement st;

      st = cn.prepareStatement("select " + columnName + " from " + TEST_TABLE_NAME);

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
