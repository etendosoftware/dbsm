/*
 ************************************************************************************
 * Copyright (C) 2017 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.dbsm.test.model;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.ddlutils.model.Database;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openbravo.dbsm.test.base.DbsmTest;

/**
 * Tests that user triggers that are not part of the model are disabled and enabled when invoking
 * the Platform.disableAllTriggers and Platform.enableAllTriggers methods respectively
 *
 */
@RunWith(Parameterized.class)
public class CheckTriggerDisablement extends DbsmTest {

  // sequence number to be used as record id
  private static int recordSeq = 0;

  public CheckTriggerDisablement(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  public void allUserTriggersAreDisabled() throws IOException {
    try {
      resetDB();
      // Create a table with a trigger that modifies the TRIGGER_WAS_INVOKED and sets it to 0
      Database model = updateDatabase("excludeFilter/BASE_MODEL_WITH_COUNT_AND_TRIGGER.xml");
      // Remove the trigger for the model, it should be disabled when invoking the
      // disableAllTriggers anyway
      // This is equivalent to updating the database with a model without the trigger, creating the
      // trigger manually and using an exclude filter to exclude it.
      model.removeTrigger(0);

      Connection con = getPlatform().borrowConnection();
      getPlatform().disableAllTriggers(con, model, false);

      assertTriggerIsEnabled(false);

      getPlatform().enableAllTriggers(con, model, false);

      assertTriggerIsEnabled(true);

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * Asserts that the trigger is enabled/disabled, depending on the expectedValue parameter
   * 
   * A row is inserted in the target table, filling in the id, but leaving empty the
   * trigger_was_invoked was invoked column (its default value is 0). If the trigger is enabled, the
   * value of that column after inserting the new row will be 1
   * 
   * @param expectedValue
   *          true if the trigger should have been enabled, false otherwise
   */
  private void assertTriggerIsEnabled(boolean expectedValue) throws SQLException {
    String recordId = Integer.toString(recordSeq++);
    insertRowManually(recordId);
    Row newRow = getRowValues("TEST", recordId);
    String columnName = getRdbms() == Rdbms.ORA ? "trigger_was_invoked".toUpperCase()
        : "trigger_was_invoked";
    boolean triggerWasEnabled = (1 == Integer.parseInt(newRow.getValue(columnName)));
    assertThat(triggerWasEnabled, equalTo(expectedValue));
  }

  private void insertRowManually(String id) throws SQLException {
    Connection cn = null;
    PreparedStatement st = null;
    try {
      cn = getDataSource().getConnection();
      st = cn.prepareStatement("INSERT INTO test(test_id, trigger_was_invoked) values ('" + id
          + "', 0)");
      st.execute();
    } finally {
      if (st != null) {
        try {
          st.close();
        } catch (SQLException e) {
          log.error("Error closing statement", e);
        }
      }
      if (cn != null) {
        try {
          cn.close();
        } catch (SQLException e) {
          log.error("Error closing connection", e);
        }
      }
    }
  }
}
