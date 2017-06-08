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
package org.openbravo.dbsm.test.configscript;

import static org.junit.Assert.assertNull;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.ddlutils.model.Check;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test cases to test it is possible to remove a check constraint during the install source using a
 * removeCheckChange in a configScript.
 * 
 * See issue https://issues.openbravo.com/view.php?id=36137
 * 
 * @author inigo.sanchez
 *
 */
@RunWith(Parameterized.class)
public class ConfigScriptRemoveCheckChangeConstraint extends ConfigScriptBaseTest {

  private static final String BASE_MODEL = "removeCheckChange/BASE_MODEL_CHECK_CONSTRAINT.xml";
  private static final String CONFIG_SCRIPT = "model/removeCheckChange/configScript.xml";
  private static final String TEST_TABLE = "TEST";
  private static final String CHECK_TEST = "TEST_CONSTRAINT";

  public ConfigScriptRemoveCheckChangeConstraint(String rdbms, String driver, String url,
      String sid, String user, String password, String name) throws FileNotFoundException,
      IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  public void isConfigurationScriptAppliedOnInstallSource() {
    // Install source and applyConfigurationScript
    Database originalDB = applyConfigurationScript(BASE_MODEL, CONFIG_SCRIPT);
    // Check if constraint is removed
    Table table = originalDB.findTable(TEST_TABLE);
    Check check = table.findCheck(CHECK_TEST);
    assertNull("Check " + CHECK_TEST + " removed by the configuration script", check);
  }

  @Override
  protected void doModelChanges(Database database) {
    // Not needed as this test case does not perform model but data changes
  }
}
