/*
 ************************************************************************************
 * Copyright (C) 2017 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */
package org.openbravo.dbsm.test.model.recreation;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;

public class PLCode extends DbsmTest {

  public PLCode(String rdbms, String driver, String url, String sid, String user, String password,
      String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  public void thereShouldNoBePLRecreationIfNotModified() {
    resetDB();
    String dbModelPath = "triggers/TABLE_WITH_TWO_TRIGGERS.xml";
    updateDatabase(dbModelPath);

    List<String> updateStatements = sqlStatmentsForUpdate(dbModelPath);

    assertThat("changes between updated db and target db", updateStatements,
        not(hasItem(containsString("DROP TRIGGER"))));
  }
}
