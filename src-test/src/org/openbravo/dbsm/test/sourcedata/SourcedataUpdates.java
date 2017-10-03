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

package org.openbravo.dbsm.test.sourcedata;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;

/** Test cases covering updates on source data */
public class SourcedataUpdates extends DbsmTest {

  public SourcedataUpdates(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  /**
   * If there are more than one change for the same row, all of them should be executed in a single
   * statement. See issue #35653
   */
  @Test
  // TODO: recover this tests case once #36938 gets fixed
  @Ignore("Current test plaform does not support src updates, see issue #36938")
  public void allChangesInARowAreExecutedAltogether() throws SQLException {
    resetDB();
    String model = "constraints/TWO_COLS_CHECK.xml";
    List<String> adTables = Arrays.asList("TEST");

    updateDatabase(model, "data/datachanges/v1", adTables);

    // v2 changes values in col1 and col2, if they were executed in 2 steps, constraint would fail
    updateDatabase(model, "data/datachanges/v2", adTables);

    assertThat("updated value in test.col1", getActualValue("test", "col1"), is("v2"));
  }

  /**
   * Regression test for issue #36984
   * 
   * Ensures changes in different AD rows are applied in proper order.
   */
  @Test
  // TODO: recover this tests case once #36938 gets fixed
  @Ignore("Current test plaform does not support src updates, see issue #36938")
  public void updatesAreAppliedInProperOrder() throws SQLException {
    resetDB();
    String model = "constraints/SIMPLE_UNIQUE.xml";
    List<String> adTables = Arrays.asList("TEST");

    updateDatabase(model, "data/datachanges1/v1", adTables);

    updateDatabase(model, "data/datachanges1/v2", adTables);
  }
}
