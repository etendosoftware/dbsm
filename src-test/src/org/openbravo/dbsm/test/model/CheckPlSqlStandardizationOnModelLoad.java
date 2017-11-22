/*
 ************************************************************************************
 * Copyright (C) 2016-2017 Openbravo S.L.U.
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openbravo.dbsm.test.base.DbsmTest;

/**
 * Test cases to test it is possible to disable the PLSQL standardization when the model is loaded
 * 
 * @author AugustoMauch
 *
 */
@RunWith(Parameterized.class)
public class CheckPlSqlStandardizationOnModelLoad extends DbsmTest {

  private static final String EXPORT_DIR = "/tmp/export-test";

  public CheckPlSqlStandardizationOnModelLoad(String rdbms, String driver, String url, String sid,
      String user, String password, String systemUser, String systemPassword, String name)
      throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, systemUser, systemPassword, name);
  }

  @Test
  // Checks that it is possible to standardize the PLSQL code.
  // This is done by checking that both in Oracle and PostgreSQL the exported XML is the same as the
  // imported XML
  public void plSqlIsStandardized() throws IOException {
    resetDB();
    updateDatabase("plSqlStandardization/BASE_MODEL_WITH_FUNCTION.xml");
    boolean doPlSqlStandardization = true;
    assertExport("plSqlStandardization/BASE_MODEL_WITH_FUNCTION.xml", doPlSqlStandardization);
  }

  @Test
  // Checks that it is possible load the model without standarizing the PLSQL code.
  // This is done by checking that in Oracle the exported XML is the same as the imported XML (dbsm
  // exports XML in the Oracle's standarized format) and that in PostgreSQL the exported content is
  // different, as it has not been standarized
  public void plSqlIsNotStandarized() throws IOException {
    resetDB();
    updateDatabase("plSqlStandardization/BASE_MODEL_WITH_FUNCTION.xml");
    boolean doPlSqlStandardization = false;
    if (getRdbms().equals(DbsmTest.Rdbms.ORA)) {
      assertExport("plSqlStandardization/BASE_MODEL_WITH_FUNCTION.xml", doPlSqlStandardization);
    } else {
      assertExport("plSqlStandardization/BASE_MODEL_WITH_FUNCTION_NOT_STANDARDIZED.xml",
          doPlSqlStandardization);
    }
  }

  private void assertExport(String modelFileToCompare, boolean doPlSqlStandarization)
      throws IOException {
    File exportTo = new File(EXPORT_DIR);
    if (exportTo.exists()) {
      exportTo.delete();
    }
    exportTo.mkdirs();
    exportDatabase(EXPORT_DIR, doPlSqlStandarization);

    File exportedTable = new File(EXPORT_DIR, "functions/TEST_FUNCTION.xml");
    assertThat("exported function exists", exportedTable.exists(), is(true));

    String exportedContents = FileUtils.readFileToString(exportedTable);
    String originalContents = FileUtils.readFileToString(new File("model", modelFileToCompare));
    assertThat("exported contents", exportedContents, equalTo(originalContents));
  }
}
