/*
 ************************************************************************************
 * Copyright (C) 2016-2017 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */
package org.openbravo.dbsm.test.configscript;

import static org.junit.Assert.assertNull;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.ddlutils.model.Check;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ConfigScriptRemoveCheckChange extends ConfigScriptBaseTest {

  private static final String BASE_MODEL = MODEL_DIRECTORY + "BASE_MODEL.xml";
  private static final String BASE_MODEL_INSTALL = MODEL_DIRECTORY
      + "removeCheckChange/BASE_MODEL_CHECK_CONSTRAINT.xml";
  private static final String CONFIG_SCRIPT_INSTALL = "model/configScripts/removeCheckChange/configScript.xml";

  private static final String TEST_TABLE = "TEST";
  private static final String CHECK_TEST = "TEST_CONSTRAINT";

  public ConfigScriptRemoveCheckChange(String rdbms, String driver, String url, String sid,
      String user, String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Override
  protected void doModelChanges(Database database) {
    Table table = database.findTable(TEST_TABLE);
    Check check = table.findCheck(CHECK_TEST);
    table.removeCheck(check);
  }

  @Test
  public void isCheckConstraintRemovedOnUpdate() {
    Database database = exportModelChangesAndUpdateDatabase(BASE_MODEL);
    assertIsCheckConstraintRemoved(database, TEST_TABLE, CHECK_TEST);
  }

  /**
   * Test cases to test it is possible to remove a check constraint during the install source using
   * a removeCheckChange in a configScript.
   * 
   * See issue https://issues.openbravo.com/view.php?id=36137
   *
   */
  @Test
  public void isCheckConstraintRemovedOnInstall() {
    Database originalDB = createDatabaseAndApplyConfigurationScript(BASE_MODEL_INSTALL,
        Arrays.asList(CONFIG_SCRIPT_INSTALL));
    assertIsCheckConstraintRemoved(originalDB, TEST_TABLE, CHECK_TEST);
  }

  /**
   * Check if constraint is removed from the database
   */
  private void assertIsCheckConstraintRemoved(Database db, String tableName, String checkName) {
    Table table = db.findTable(tableName);
    Check check = table.findCheck(checkName);
    assertNull("Check " + checkName + " removed by the configuration script", check);
  }
}
