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

package org.openbravo.dbsm.test.model;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Function;
import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;

public class Functions extends DbsmTest {

  public Functions(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  /**
   * Checks functions are parameters are properly read from db.
   * 
   * See this jdbc thread: <code>
   * http://www.postgresql.org/message-id/CABtr+CJC54wue94UweBocnDnL0CfM3s6kC4Ckt48NmztwQMdKQ@mail.gmail.com
   * <code>
   */
  @Test
  public void functionParameters() {
    resetDB();
    updateDatabase("functions/F.xml", false);
    Database db = getPlatform().loadModelFromDatabase(getExcludeFilter());

    assertThat("Number of functions in db", db.getFunctionCount(), is(6));
    for (int f = 0; f < db.getFunctionCount(); f++) {
      Function function = db.getFunction(f);
      assertThat("Number of parameters in function " + function.getName(),
          function.getParameterCount(), is(2));
      assertThat("1st param in function " + function.getName(), function.getParameter(0).getName(),
          equalTo("p1"));
      assertThat("2nd param in function " + function.getName(), function.getParameter(1).getName(),
          equalTo("p2"));
    }
  }
}
