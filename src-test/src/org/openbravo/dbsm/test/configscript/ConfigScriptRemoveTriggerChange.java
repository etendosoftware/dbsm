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
package org.openbravo.dbsm.test.configscript;

import static org.junit.Assert.assertNull;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Trigger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ConfigScriptRemoveTriggerChange extends ConfigScriptBaseTest {

  private static final String BASE_MODEL = MODEL_DIRECTORY + "BASE_MODEL.xml";
  private static final String TRIGGER_TEST = "TEST_TRIGGER";

  public ConfigScriptRemoveTriggerChange(String rdbms, String driver, String url, String sid,
      String user, String password, String name, UpdateModelTask task)
      throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, task);
  }

  @Override
  protected void doModelChanges(Database database) {
    Trigger trigger = database.findTrigger(TRIGGER_TEST);
    database.removeTrigger(trigger);
  }

  @Test
  public void isTriggerRemoved() {
    Database database = exportModelChangesAndUpdateDatabase(BASE_MODEL);
    Trigger trigger = database.findTrigger(TRIGGER_TEST);
    assertNull("Trigger " + TRIGGER_TEST + " removed by the configuration script", trigger);
  }
}
