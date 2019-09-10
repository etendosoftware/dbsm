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
package org.openbravo.dbsm.test.model;

import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

/**
 * Test intended to check the correct behavior when parallelizing the creation of indexes on
 * existing tables.
 */
public class IndexParallelization extends IndexBaseTest {

  private static int MAX_ACTIVE_CONNECTIONS = 2;

  public IndexParallelization(String rdbms, String driver, String url, String sid, String user,
      String password, String name, TestType testType) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, testType);
  }

  @Before
  public void configureNumberOfThreadsAndMaxActiveConnections() {
    setNumberOfThreads(MAX_ACTIVE_CONNECTIONS);
    getDataSource().setMaxActive(MAX_ACTIVE_CONNECTIONS);
  }

  // Tests that it is possible to create several indexes even if the initial maximum number of
  // active connections configured for the pool is lower than the number of indexes about to be
  // created
  @Test
  public void addNewIndexesOnExistingTable() throws IOException {
    assumeThat(getTestType(), is(TestType.onCreate));
    resetDB();
    createDatabase("indexes/BASE_MODEL_FOUR_COLUMNS.xml");
    updateDatabase("indexes/THREE_INDEXES.xml");
    assertExportIsConsistent("indexes/THREE_INDEXES.xml");
  }

}
