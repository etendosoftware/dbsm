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

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ConfigScriptColumnSizeChange extends ConfigScriptBaseTest {

  private static final String BASE_MODEL = MODEL_DIRECTORY + "BASE_MODEL.xml";
  private static final String TEST_TABLE = "TEST";
  private static final String TEST_COLUMN = "COL1";
  private int newColumnSize;

  public ConfigScriptColumnSizeChange(String rdbms, String driver, String url, String sid,
      String user, String password, String name, UpdateModelTask task)
      throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, task);
  }

  @Override
  protected void doModelChanges(Database database) {
    Table table = database.findTable(TEST_TABLE);
    Column column = table.findColumn(TEST_COLUMN);
    newColumnSize = Integer.parseInt(column.getSize()) + 10;
    column.setSize(newColumnSize + "");
  }

  @Test
  public void isColumnSizeChangeApplied() {
    Database database = exportModelChangesAndUpdateDatabase(BASE_MODEL);
    Table table = database.findTable(TEST_TABLE);
    Column column = table.findColumn(TEST_COLUMN);
    assertEquals("Size of column " + TEST_COLUMN + " increased by the configuration script",
        newColumnSize, Integer.parseInt(column.getSize()));
  }
}
