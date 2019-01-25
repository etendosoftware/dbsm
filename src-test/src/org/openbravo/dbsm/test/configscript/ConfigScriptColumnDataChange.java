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

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ConfigScriptColumnDataChange extends ConfigScriptBaseTest {

  private static final String BASE_MODEL = MODEL_DIRECTORY + "BASE_MODEL.xml";
  private static final String CONFIG_SCRIPT = "model/configScripts/configScript.xml";

  private static final String TEST_TABLE = "TEST";
  private static final String TEST_ROW_ID = "1";

  private static final Map<String, String> columnDataChanges;

  static {
    columnDataChanges = new LinkedHashMap<String, String>();
    columnDataChanges.put("TEST_ID", "1");
    // check for single quote
    columnDataChanges.put("COL1", "active='Y'");
    // check for breakline
    columnDataChanges.put("COL2", "This is the first part \nThis is the second part");
  }

  public ConfigScriptColumnDataChange(String rdbms, String driver, String url, String sid,
      String user, String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  public void isColumnDataChangeApplied() {
    List<String> adTableNames = Arrays.asList(TEST_TABLE);
    List<String> configScripts = Arrays.asList(CONFIG_SCRIPT);
    applyConfigurationScripts(BASE_MODEL, adTableNames, configScripts);
    assertEquals("Data changes applied by Configuration Script", getColumnDataChangesColumnValues(),
        getRowValues(TEST_ROW_ID, TEST_TABLE, getColumnDataChangesColumnNames()));
  }

  /**
   * This test checks that data changes present in a Configuration Script are applied properly.
   * Eventually, this test makes use of {@link org.apache.ddlutils.Platform#applyConfigScript}
   * method.
   */
  @Test
  public void isConfigurationScriptApplied() {
    List<String> adTableNames = Arrays.asList(TEST_TABLE);
    applyConfigurationScript(BASE_MODEL, adTableNames, CONFIG_SCRIPT);
    assertEquals("Data changes applied by Configuration Script", getColumnDataChangesColumnValues(),
        getRowValues(TEST_ROW_ID, TEST_TABLE, getColumnDataChangesColumnNames()));
  }

  private static List<String> getColumnDataChangesColumnValues() {
    return new ArrayList<String>(columnDataChanges.values());
  }

  private static Set<String> getColumnDataChangesColumnNames() {
    return columnDataChanges.keySet();
  }

}
