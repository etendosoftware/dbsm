/*
 ************************************************************************************
 * Copyright (C) 2016-2020 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */
package org.openbravo.dbsm.test.configscript;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openbravo.test.base.Issue;

@RunWith(Parameterized.class)
public class ConfigScriptColumnSizeChange extends ConfigScriptBaseTest {

  private static final String BASE_MODEL = MODEL_DIRECTORY + "BASE_MODEL.xml";
  private static final String CONFIG_SCRIPT_INSTALL = "model/configScripts/columnSizeChange/configScript.xml";
  private static final String DATA_DIRECTORY_SIZE = "data/configScriptsChangeSizeColumn";

  private static final String TEST_TABLE = "TEST";
  private static final String TEST_COLUMN = "COL1";
  private static final String TEST_ROW_ID = "1";

  private static final Map<String, String> columnDataChanges;

  static {
    columnDataChanges = new LinkedHashMap<>();
    columnDataChanges.put("TEST_ID", "1");
    columnDataChanges.put("COL1",
        "This is the first part of the regression test.This is the second.");
    columnDataChanges.put("COL2", null);
  }

  public ConfigScriptColumnSizeChange(String rdbms, String driver, String url, String sid,
      String user, String password, String name) throws IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  public void isColumnSizeChangeAppliedOnUpdate() {
    Database database = exportModelChangesAndUpdateDatabase(BASE_MODEL,
        Arrays.asList(CONFIG_SCRIPT_INSTALL));
    Table table = database.findTable(TEST_TABLE);
    Column column = table.findColumn(TEST_COLUMN);
    assertEquals("Size of column " + TEST_COLUMN + " increased by the configuration script", 70,
        Integer.parseInt(column.getSize()));
  }

  @Test
  public void isColumnSizeChangeAppliedOnInstall() {
    Database originalDB = createDatabaseAndApplyConfigurationScript(BASE_MODEL,
        Arrays.asList(CONFIG_SCRIPT_INSTALL));

    assertIsColumnSizeChangeApplied(originalDB, TEST_TABLE, TEST_COLUMN);
  }

  /**
   * Test case to test if it is possible to update the database with a columnSizeChange without any
   * problem in the insert of the data in the recreated table.
   */
  @Test
  @Issue("36902")
  public void isColumnSizeAppliedProperly() {
    // this method creates a new database with the configScripts applied and insert the data
    exportModelChangesAndUpdateDatabase(BASE_MODEL, Arrays.asList(TEST_TABLE),
        Arrays.asList(CONFIG_SCRIPT_INSTALL), DATA_DIRECTORY_SIZE);
    // update the database and take into account the configScript
    updateDatabase(BASE_MODEL, DATA_DIRECTORY_SIZE, Arrays.asList(TEST_TABLE), true,
        Arrays.asList(CONFIG_SCRIPT_INSTALL));

    assertEquals("Data changes applied by Configuration Script", getColumnDataChangesColumnValues(),
        getRowValues(TEST_ROW_ID, TEST_TABLE, getColumnDataChangesColumnNames()));
  }

  /**
   * Check if column size is changed in the database
   */
  private void assertIsColumnSizeChangeApplied(Database db, String tableName, String columnName) {
    Table table = db.findTable(tableName);
    Column column = table.findColumn(columnName);
    assertEquals(
        "Size of the column " + columnName + " was 60 and now is " + column.getSize() + ".", 70,
        column.getSizeAsInt());
  }

  private static Set<String> getColumnDataChangesColumnNames() {
    return columnDataChanges.keySet();
  }

  private static List<String> getColumnDataChangesColumnValues() {
    return new ArrayList<>(columnDataChanges.values());
  }
}
