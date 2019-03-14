/*
 ************************************************************************************
 * Copyright (C) 2019 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */
package org.openbravo.dbsm.test.model;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Function;
import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;

/** Test cases covering volatility in functions */
public class FunctionVolatility extends DbsmTest {

  public FunctionVolatility(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  public void defaultFunctionIsVolatile() {
    Database db = createDatabase("functions/SIMPLE_FUNCTION.xml");
    assertThat("Default volatility", db.getFunction(0).getVolatility(),
        is(Function.Volatility.VOLATILE));
  }
}
