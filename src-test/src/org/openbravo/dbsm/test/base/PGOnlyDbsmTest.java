/*
 ************************************************************************************
 * Copyright (C) 2015-2017 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.dbsm.test.base;

import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Before;

/**
 * Base class to implement dbsm test cases to be executed only in PostgreSQL
 * 
 * @author alostale
 *
 */
public class PGOnlyDbsmTest extends DbsmTest {

  public PGOnlyDbsmTest(String rdbms, String driver, String url, String sid, String user,
      String password, String systemUser, String systemPassword, String name)
      throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, systemUser, systemPassword, name);
  }

  @Before
  public void executeOnlyInPG() {
    assumeThat("not executing in Oracle", getRdbms(), is(Rdbms.PG));
  }

}
