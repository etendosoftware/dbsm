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

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.codehaus.jettison.json.JSONException;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

public class OtherRecreations extends TableRecreationBaseTest {

  static {
    availableTypes.add(ActionType.append);
  }

  public OtherRecreations(String rdbms, String driver, String url, String sid, String user,
      String password, String name, ActionType type) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, type);
  }

  @Parameters(name = "DB: {6} - {7}")
  public static Collection<Object[]> parameters() throws IOException, JSONException {
    return TableRecreationBaseTest.parameters();
  }

  @Test
  public void realADTableMP17to15Q2() {
    assertTablesAreNotRecreated("AD_TABLE-MP17.xml", "AD_TABLE-PR15Q2.xml", false);
  }

  @Test
  public void createDefaultRemoval() {
    assertTablesAreNotRecreated("OCD1.xml", "OCD2.xml", false);
  }

  @Test
  public void mandatorynessRemoval() {
    assertTablesAreNotRecreated("M1.xml", "M2.xml", false);
  }

  @Test
  public void mandatoryNoDefaultNorOCDModelIsOk() throws SQLException {
    resetDB();
    assertTablesAreNotRecreated("RM1.xml", "RM2.xml", false);
  }

  @Test(expected = AssertionError.class)
  public void mandatoryNoDefaultNorOCDDataNeedsFix() throws SQLException {

    // resetDB();
    // assertTablesAreNotRecreated("RM1.xml", "RM2.xml", true);

    resetDB();
    Database db = updateDatabase("recreation/RM1.xml");
    Table testTable = db.getTable(0);
    generateRow(testTable, true);

    // we've generated a row with null for the new mandatory column, expected exception. This case
    // should be null deferred and can be fixed by moduleScripts.
    // BatchExector logs error but it does not throws exception, expecting assertion failure
    // after update database

    updateDatabase("recreation/RM2.xml");
  }

  @Test
  public void mandatoryChanged() throws SQLException {
    resetDB();
    assertTablesAreNotRecreated("COL4.xml", "COL41.xml", false);
  }

  @Test
  public void differentDefaultAndOCDinCreateDatabase() throws SQLException {
    resetDB();
    createDatabase("createDefault/M4.xml");
  }

  @Test
  public void fksShouldnBeRecreated() throws SQLException {
    resetDB();
    updateDatabase("recreation/FK.xml");
    List<String> l = sqlStatmentsForUpdate("recreation/FK2.xml");
    for (String st : l) {
      assertThat(st,
          not(anyOf(containsString("DROP CONSTRAINT"), containsString("ADD CONSTRAINT"))));
    }
  }

  @Test
  public void fksShoulBeRecreatedWhenRefTableIsRecreated() throws SQLException {
    resetDB();
    updateDatabase("recreation/FK.xml");
    List<String> l = sqlStatmentsForUpdate("recreation/FK3.xml");
    StringBuilder allSts = new StringBuilder();
    for (String st : l) {
      allSts.append(st);
    }

    System.out.println("\n\n*****************************************************************");
    System.out.println(allSts);
    System.out.println("*****************************************************************\n\n");

    // checking SQL commands
    assertThat(allSts.toString(),
        allOf(containsString("DROP CONSTRAINT"), containsString("ADD CONSTRAINT")));

    // checking real update
    updateDatabase("recreation/FK3.xml");
  }

  @Test
  public void fksShoulBeRecreatedWhenRefTableIsRecreatedAD() throws SQLException {
    resetDB();
    updateDatabase("recreation/FK.xml", "data/createDefault", Arrays.asList("TEST", "TEST2"));
    List<String> l = sqlStatmentsForUpdate("recreation/FK3.xml", "data/createDefault",
        Arrays.asList("TEST", "TEST2"));
    StringBuilder allSts = new StringBuilder();
    int numOfDrops = 0;
    int numOfAdds = 0;
    for (String st : l) {
      allSts.append(st);
      if (st.contains("DROP CONSTRAINT") && st.contains("TEST2_TEST_FK")) {
        numOfDrops++;
      }
      if (st.contains("ADD CONSTRAINT") && st.contains("TEST2_TEST_FK")) {
        numOfAdds++;
      }
    }

    System.out.println("\n\n*****************************************************************");
    System.out.println(allSts);
    System.out.println("*****************************************************************\n\n");

    // checking SQL commands
    if (true) {
      assertThat(allSts.toString(),
          allOf(containsString("DROP CONSTRAINT"), containsString("ADD CONSTRAINT")));
      assertThat("Number of DROP statements", numOfDrops, is(1));
      assertThat("Number of ADD statements", numOfAdds, is(1));
    }
    // checking real update
    updateDatabase("recreation/FK3.xml", "data/createDefault", Arrays.asList("TEST", "TEST2"));
  }
}
