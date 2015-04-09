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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.openbravo.dbsm.test.base.DbsmTest;

public class SQLCommands extends DbsmTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  public SQLCommands(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  public void severalTableAlterationsShouldBeInASingleStmt() {
    resetDB();
    updateDatabase("recreation/BASE_MODEL.xml");
    List<String> l = sqlStatmentsForUpdate("recreation/COL1.xml");
    String statements = "";
    int numberOfAlterTable = 0;
    for (String st : l) {
      System.out.println(st);
      statements += st;
      if (st.contains("ALTER TABLE TEST")) {
        numberOfAlterTable += 1;
      }
    }
    assertThat("Number of ALTER statements\n" + statements, numberOfAlterTable, is(1));
  }

  @Test
  public void sameDefaultAndOCDShouldntExecuteOCD() {
    resetDB();
    updateDatabase("recreation/BASE_MODEL.xml");
    List<String> l = sqlStatmentsForUpdate("createDefault/M3.xml");
    String statements = "";
    int executionsOfOnCrateDefault = 0;
    for (String st : l) {
      System.out.println(st);
      statements += st;
      if (st.startsWith("UPDATE TEST SET M3")) {
        executionsOfOnCrateDefault += 1;
      }
    }
    assertThat("OnCreateDefault shouldn't be executed\n" + statements, executionsOfOnCrateDefault,
        is(0));
  }

  @Test
  public void differentDefaultAndOCDShouldntExecuteOCD() {
    resetDB();
    updateDatabase("recreation/BASE_MODEL.xml");
    List<String> l = sqlStatmentsForUpdate("createDefault/M4.xml");
    int numberOfAlterTable = 0;
    for (String st : l) {
      System.out.println("    " + st);
      if (st.startsWith("UPDATE TEST SET M4")) {
        numberOfAlterTable += 1;
      }
    }
    assertThat("OnCreateDefault shouldn't be executed", numberOfAlterTable, is(0));
  }
}
