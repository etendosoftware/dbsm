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
import java.util.Collection;

import org.codehaus.jettison.json.JSONException;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;
import org.openbravo.dbsm.test.base.DbsmTest;

public class AddDropConstraints extends TableRecreationBaseTest {

  static {
    availableTypes.clear();
    availableTypes.add(ActionType.append);
    availableTypes.add(ActionType.drop);
  }

  public AddDropConstraints(String rdbms, String driver, String url, String sid, String user,
      String password, String name, ActionType type, DbsmTest.RecreationMode recMode)
      throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, type, recMode);
  }

  @Parameters(name = "DB: {6} - {7} - recreation {8}")
  public static Collection<Object[]> parameters() throws IOException, JSONException {
    return TableRecreationBaseTest.parameters();
  }

  @Test
  public void unique() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "UNIQUE.xml");
  }

  @Test
  public void fk() {
    assertTablesAreNotRecreated("FK_BASE.xml", "FK.xml", false);
  }

  @Test
  public void index() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "IDX.xml");
  }

  @Test
  public void functionBasedIndexFromScratch() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "FUNCTION_IDX.xml");
  }

  @Test
  public void addFunctionToExistingIndex() {
    assertTablesAreNotRecreated("IDX.xml", "FUNCTION_IDX.xml");
  }
}
