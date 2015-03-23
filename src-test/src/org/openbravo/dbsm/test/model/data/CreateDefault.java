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

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

import java.io.FileNotFoundException;
import java.io.IOException;
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
import org.openbravo.dbsm.test.base.matchers.DBSMMatchers;

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

  protected enum DataMode {
    instance, ADupdateExisting, ADAddInUpdate, ADAfterUpdate
  }

  private AdditionMode mode;
  private DataMode dataMode;

  public CreateDefault(String rdbms, String driver, String url, String sid, String user,
      String password, String name, AdditionMode mode, DataMode datamode)
      throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
    this.mode = mode;
    this.dataMode = datamode;
  }

  /**
   * Adds new parameter to default ones to decide whether new column in following test cases is
   * appended or added between existent columns
   */
  @Parameters(name = "DB: {6} - {7} - {8}")
  public static Collection<Object[]> parameters() throws IOException, JSONException {
    List<Object[]> configs = new ArrayList<Object[]>();

    for (String[] param : DbsmTest.params()) {
      for (AdditionMode addMode : AdditionMode.values()) {
        if (false && addMode == AdditionMode.addInTheMiddle) {
          continue;
        }
        for (DataMode dataMode : DataMode.values()) {
          List<Object> p = new ArrayList<Object>(Arrays.asList(param));
          p.add(addMode);
          p.add(dataMode);
          configs.add(p.toArray());
        }
      }
    }
    return configs;
  }

  // ---- Non Mandatory columns ----

  @Test
  public void nonMandatoryDefault_NM1() throws SQLException {
    assumeThat(dataMode, not(anyOf(is(DataMode.ADAfterUpdate), is(DataMode.ADAddInUpdate))));
    assertDefaults("NM1", "A");
  }

  @Test
  public void nonMandatoryOnCreateDefault_NM2() throws SQLException {
    assumeThat(dataMode, not(DataMode.ADAfterUpdate));
    assertDefaults("NM2", "A");
  }

  @Test
  public void nonMandatorySameDefaultAndOnCreateDefault_NM3() throws SQLException {
    assumeThat(dataMode, not(DataMode.ADAfterUpdate));
    assertDefaults("NM3", "A");
  }

  @Test
  public void nonMandatoryDifferentDefaultAndOnCreateDefault_NM4() throws SQLException {
    assumeThat(dataMode, not(DataMode.ADAfterUpdate));
    assertDefaults("NM4", "B");
  }

  // ---- Mandatory columns ----

  @Test
  public void mandatoryDefault_M1() throws SQLException {
    assumeThat(dataMode, not(anyOf(is(DataMode.ADAfterUpdate), is(DataMode.ADAddInUpdate))));

    assertDefaults("M1", "A");
  }

  @Test
  public void mandatoryOnCreateDefault_M2() throws SQLException {
    assertDefaults("M2", "A");
  }

  @Test
  public void mandatorySameDefaultAndOnCreateDefault_M3() throws SQLException {
    assertDefaults("M3", "A");
  }

  @Test
  public void mandatoryDifferentDefaultAndOnCreateDefault_M4() throws SQLException {
    assertDefaults("M4", "B");
  }

  private void assertDefaults(String columnName, String value) throws SQLException {
    resetDB();

    String originalModelName = mode == AdditionMode.append ? "createDefault/BASE_MODEL.xml"
        : "createDefault/BASE_MODEL2.xml";

    Database originalModel = null;
    Database newModel = null;

    Row oldRow = null;
    String pk = "";
    switch (dataMode) {
    case instance:
      originalModel = updateDatabase(originalModelName);
      break;
    case ADAfterUpdate:
    case ADAddInUpdate:
      originalModel = updateDatabase(originalModelName);
      break;
    case ADupdateExisting:
      originalModel = updateDatabase(originalModelName, "data/createDefault", Arrays.asList("TEST"));
    }

    switch (dataMode) {
    case instance:
      generateRow(originalModel, TEST_TABLE_NAME);
    case ADAfterUpdate:
      newModel = updateDatabase("createDefault/" + columnName + ".xml");
      break;
    case ADAddInUpdate:
    case ADupdateExisting:
      newModel = updateDatabase("createDefault/" + columnName + ".xml", "data/createDefault",
          Arrays.asList("TEST"));
      break;
    }

    if (dataMode == DataMode.ADAfterUpdate) {
      // same update than before including obsolete AD data
      updateDatabase("createDefault/" + columnName + ".xml", "data/createDefault",
          Arrays.asList("TEST"));
    }

    assertThat("Value for column " + columnName, getActualValue(TEST_TABLE_NAME, columnName),
        equalTo(value));

    if (dataMode == DataMode.instance) {
      pk = generateRow(newModel, TEST_TABLE_NAME);
      oldRow = getRowValues(TEST_TABLE_NAME, pk);

      updateDatabase("createDefault/" + columnName + ".xml");
      Row newRow = getRowValues(TEST_TABLE_NAME, pk);

      assertThat(newRow, DBSMMatchers.rowEquals(oldRow));
    }

  }

}
