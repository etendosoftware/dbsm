/*
 ************************************************************************************
 * Copyright (C) 2019 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */
package org.openbravo.dbsm.test.model.recreation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Ignore;
import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;

public class ColumnOrderOnExport extends DbsmTest {

  public ColumnOrderOnExport(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  @Ignore("To be completed once the test infrastructure is ready for it. "
      + "See issue https://issues.openbravo.com/view.php?id=40210")
  public void sourceDataExportedUsingModelColumnOrder() {
    resetDB();
    // create TEST table with columns: TEST_ID, COL1, COL2, DUMMY
    // they are presented in that order both in the XML model and in the database
    createDatabase("recreation/COL1.xml");

    // add a new column between the existing ones in the XML definition of the table
    // after this the order of the columns in the TEST table is different:
    // XML model: TEST_ID, COL1, COL2, COL3, DUMMY
    // Database: TEST_ID, COL1, COL2, DUMMY, COL3
    updateDatabase("recreation/COL6.xml", "data/columnOrder", Arrays.asList("TEST"));

    // assert here the exported source data: the columns should be ordered as defined in the XML
    // model
    // exportDatabase("/tmp/sourcedata");
  }

}
