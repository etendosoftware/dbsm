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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assume.assumeThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;

import org.codehaus.jettison.json.JSONException;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;
import org.openbravo.dbsm.test.base.DbsmTest;

public class DataTypeChanges extends TableRecreationBaseTest {

  static {
    availableTypes.clear();
    availableTypes.add(ActionType.append);
  }

  public DataTypeChanges(String rdbms, String driver, String url, String sid, String user,
      String password, String name, ActionType type, DbsmTest.RecreationMode recMode)
      throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, type, recMode);
  }

  @Parameters(name = "DB: {6} - recreation {8}")
  public static Collection<Object[]> parameters() throws IOException, JSONException {
    return TableRecreationBaseTest.parameters();
  }

  // ========================== Size changes ========================================
  @Test
  public void changeDecimalTypeSize() {
    notWorkingYet();
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE1.xml");
  }

  @Test
  public void increaseVarcharSize() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE2.xml");
  }

  @Test
  public void increaseCharSize() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE3.xml");
  }

  // ========================== Type changes ========================================
  @Test
  public void changeVarcharToNVarchar() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE4.xml");
  }

  @Test
  public void changeNVarcharToVarchar() {
    worksOnlyIn(Rdbms.PG);
    assertTablesAreNotRecreated("DATA_TYPE4.xml", "DATA_TYPE_BASE.xml");
  }

  @Test
  public void changeVarcharToText() {
    worksOnlyIn(Rdbms.PG);
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE5.xml");
  }

  @Test
  public void changeCharToText() {
    worksOnlyIn(Rdbms.PG);
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE6.xml");
  }

  private void worksOnlyIn(Rdbms dbSpecific) {
    if (recreationMode == RecreationMode.standard) {
      assumeThat("Feature supported only for " + dbSpecific, getRdbms(), is(dbSpecific));
    }
  }

  private void notWorkingYet() {
    assumeThat("Feature not implemented yet", recreationMode, is(DbsmTest.RecreationMode.forced));
  }

}
