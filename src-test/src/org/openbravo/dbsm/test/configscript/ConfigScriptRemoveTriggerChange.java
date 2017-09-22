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

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Trigger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ConfigScriptRemoveTriggerChange extends ConfigScriptBaseTest {

  private static final String BASE_MODEL = MODEL_DIRECTORY + "BASE_MODEL.xml";
  private static final String CONFIG_SCRIPT_INSTALL = "model/configScripts/removeTriggerChange/configScript.xml";

  private static final String TRIGGER_TEST = "TEST_TRIGGER";

  public ConfigScriptRemoveTriggerChange(String rdbms, String driver, String url, String sid,
      String user, String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  public void isTriggerRemovedOnUpdate() {
    Database database = exportModelChangesAndUpdateDatabase(BASE_MODEL,
        Arrays.asList(CONFIG_SCRIPT_INSTALL));
    assertIsTriggerRemoved(database, TRIGGER_TEST);
  }

  @Test
  public void isTriggerRemovedOnInstall() {
    Database originalDB = createDatabaseAndApplyConfigurationScript(BASE_MODEL,
        Arrays.asList(CONFIG_SCRIPT_INSTALL));
    assertIsTriggerRemoved(originalDB, TRIGGER_TEST);
  }

  private void assertIsTriggerRemoved(Database database, String triggerName) {
    Trigger trg = database.findTrigger(triggerName);
    assertNull("Trigger " + triggerName + " removed by the configuration script", trg);
  }
}
