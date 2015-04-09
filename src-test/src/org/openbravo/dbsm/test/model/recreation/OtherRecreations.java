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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.codehaus.jettison.json.JSONException;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

public class OtherRecreations extends TableRecreationBaseTest {

  static {
    availableTypes.add(ActionType.append);
  }

  public OtherRecreations(String rdbms, String driver, String url, String sid, String user,
      String password, String name, ActionType type) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, type);
  }

  @Parameters(name = "DB: {6} - {7}")
  public static Collection<Object[]> parameters() throws IOException, JSONException {
    return TableRecreationBaseTest.parameters();
  }

  @Test
  public void realADTableMP17to15Q2() {
    assertTablesAreNotRecreated("AD_TABLE-MP17.xml", "AD_TABLE-PR15Q2.xml", false);
  }

  @Test
  public void createDefaultRemoval() {
    assertTablesAreNotRecreated("OCD1.xml", "OCD2.xml", false);
  }

  @Test
  public void mandatorynessRemoval() {
    assertTablesAreNotRecreated("M1.xml", "M2.xml", false);
  }

  @Test
  public void mandatoryNoDefaultNorOCDModelIsOk() throws SQLException {
    resetDB();
    assertTablesAreNotRecreated("RM1.xml", "RM2.xml", false);
  }

  @Test(expected = AssertionError.class)
  public void mandatoryNoDefaultNorOCDDataNeedsFix() throws SQLException {

    // resetDB();
    // assertTablesAreNotRecreated("RM1.xml", "RM2.xml", true);

    resetDB();
    Database db = updateDatabase("recreation/RM1.xml");
    Table testTable = db.getTable(0);
    generateRow(testTable, true);

    // we've generated a row with null for the new mandatory column, expected exception. This case
    // should be null deferred and can be fixed by moduleScripts.
    // BatchExector logs error but it does not throws exception, expecting assertion failure
    // after update database

    updateDatabase("recreation/RM2.xml");
  }

  @Test
  public void mandatoryChanged() throws SQLException {
    resetDB();
    assertTablesAreNotRecreated("COL4.xml", "COL41.xml", false);
  }
}
