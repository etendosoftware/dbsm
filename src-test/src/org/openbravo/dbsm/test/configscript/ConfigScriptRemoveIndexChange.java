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
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ConfigScriptRemoveIndexChange extends ConfigScriptBaseTest {

  private static final String BASE_MODEL = MODEL_DIRECTORY + "BASE_MODEL.xml";
  private static final String CONFIG_SCRIPT_INSTALL = "model/configScripts/removeIndexChange/configScript.xml";

  private static final String TEST_TABLE = "TEST";
  private static final String TEST_INDEX = "TEST_INDEX";

  public ConfigScriptRemoveIndexChange(String rdbms, String driver, String url, String sid,
      String user, String password, String systemUser, String systemPassword, String name)
      throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, systemUser, systemPassword, name);
  }

  @Test
  public void isIndexRemovedOnUpdate() {
    Database database = exportModelChangesAndUpdateDatabase(BASE_MODEL,
        Arrays.asList(CONFIG_SCRIPT_INSTALL));
    Table table = database.findTable(TEST_TABLE);
    Index index = table.findIndex(TEST_INDEX);
    assertNull("Index " + TEST_INDEX + " removed by the configuration script", index);
  }

  @Test
  public void isIndexRemovedOnInstall() {
    Database originalDB = createDatabaseAndApplyConfigurationScript(BASE_MODEL,
        Arrays.asList(CONFIG_SCRIPT_INSTALL));
    assertIsIndexRemoved(originalDB, TEST_TABLE, TEST_INDEX);
  }

  /**
   * Check if index is removed from the database
   */
  private void assertIsIndexRemoved(Database db, String tableName, String indexName) {
    Table table = db.findTable(tableName);
    Index index = table.findIndex(indexName);
    assertNull("Index " + indexName + " removed by the configuration script", index);
  }
}
