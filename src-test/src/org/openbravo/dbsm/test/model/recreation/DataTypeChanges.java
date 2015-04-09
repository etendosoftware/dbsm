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

public class DataTypeChanges extends TableRecreationBaseTest {

  static {
    availableTypes.add(ActionType.append);
  }

  public DataTypeChanges(String rdbms, String driver, String url, String sid, String user,
      String password, String name, ActionType type) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, type);
  }

  @Parameters(name = "DB: {6}")
  public static Collection<Object[]> parameters() throws IOException, JSONException {
    return TableRecreationBaseTest.parameters();
  }

  @Test
  public void changeDecimalTypeSize() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE1.xml");
  }

  @Test
  public void changeVarcharTypeSize() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE2.xml");
  }

  @Test
  public void changeCharTypeSize() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE3.xml");
  }
}
