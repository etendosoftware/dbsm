/*
 ************************************************************************************
 * Copyright (C) 2016 Openbravo S.L.U.
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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Trigger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openbravo.dbsm.test.base.DbsmTest;

/**
 * Test cases to test it is to create in Oracle a trigger with a FOLLOWS clause
 * 
 * @author AugustoMauch
 *
 */
@RunWith(Parameterized.class)
public class CheckFollowsClauseCanBeDefinedInOracleTriggers extends DbsmTest {

  public CheckFollowsClauseCanBeDefinedInOracleTriggers(String rdbms, String driver, String url,
      String sid, String user, String password, String name) throws FileNotFoundException,
      IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  public void triggerWithFollowsClauseCanBeDefinedInOracle() throws IOException {
    assumeThat("Should be executed", isOracle(), is(true));
    resetDB();
    Database db = updateDatabase("triggers/TABLE_WITH_TWO_TRIGGERS.xml");
    Trigger t = db.findTrigger("TEST_TRIGGER_SECOND");
    Connection con = getPlatform().borrowConnection();
    boolean expected = false;
    assertFirstTriggerFollowsSecondTrigger(expected);
    // TEST_TRIGGER_SECOND is going to be recreated, now it is going to FOLLOW TEST_TRIGGER_FIRST
    getPlatform().dropTrigger(con, db, t);
    List<String> triggersToFollow = new ArrayList<String>();
    triggersToFollow.add("TEST_TRIGGER_FIRST");
    getPlatform().createTrigger(con, db, t, triggersToFollow);
    expected = true;
    assertFirstTriggerFollowsSecondTrigger(expected);
  }

  public void assertFirstTriggerFollowsSecondTrigger(boolean expectedValue) {
    boolean firstTriggerFollowsSecondTrigger = checkFirstTriggerFollowsSecondTrigger();
    assertThat(firstTriggerFollowsSecondTrigger, equalTo(expectedValue));
  }

  public boolean checkFirstTriggerFollowsSecondTrigger() {
    // in oracle if a trigger follows another one, its description will contain the substring
    // 'FOLLOWS TRIGGER_NAME"
    Connection cn = null;
    try {
      cn = getDataSource().getConnection();
      PreparedStatement st = null;
      st = cn
          .prepareStatement("SELECT count(*) FROM user_triggers WHERE upper(trigger_name) = 'TEST_TRIGGER_SECOND' and UPPER(description) like '%FOLLOWS  TEST_TRIGGER_FIRST%'");
      ResultSet rs = st.executeQuery();
      if (rs.next()) {
        int nRecords = rs.getInt(1);
        return (nRecords == 1);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (cn != null) {
        try {
          cn.close();
        } catch (SQLException e) {
        }
      }
    }
    return false;
  }

  private boolean isOracle() {
    return getRdbms() == Rdbms.ORA;
  }
}
