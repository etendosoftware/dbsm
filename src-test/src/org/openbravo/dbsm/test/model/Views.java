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

import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;

public class Views extends DbsmTest {

  protected static final String EXPORT_DIR = "/tmp/export-test";

  private static final String VIEW_XML_OPEN_TAG = "<view";
  private static final String VIEW_XML_CLOSE_TAG = "</view>";

  public Views(String rdbms, String driver, String url, String sid, String user, String password,
      String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  // Tests that a basic view can be created and exported properly
  public void exportBasicView() throws IOException {
    resetDB();
    updateDatabase("views/BASE_MODEL.xml", false);
    assertExportedView("views/BASE_MODEL.xml", "views/TEST_VIEW.xml");
  }

  @Test
  // Tests that a view which makes use of the SUBSTR function be created and exported properly
  // In PostgresSQL this view will have a ::text cast as part of its SQL code which must be removed
  // properly when exporting the model
  public void exportViewWithTextCast() throws IOException {
    resetDB();
    updateDatabase("views/TEXT_CAST_TEST_MODEL.xml", false);
    assertExportedView("views/TEXT_CAST_TEST_MODEL.xml", "views/TEST_VIEW.xml");
  }

  @Test
  // Test intended to check that in PostgresSQL views which have a ::double precision cast as part
  // of its SQL code can be imported and exported properly
  // This test is not executed in Oracle because the test view uses the DATE_PART function which is
  // not available in Oracle
  public void exportViewWithDoublePrecisionCast() throws IOException {
    assumeThat("not executing in Oracle", getRdbms(), is(Rdbms.PG));
    resetDB();
    updateDatabase("views/DOUBLE_PRECISION_CAST_TEST_MODEL.xml", false);
    assertExportedView("views/DOUBLE_PRECISION_CAST_TEST_MODEL.xml", "views/TEST_VIEW.xml");
  }

  @Test
  public void multipleInClauses() throws IOException {
    resetDB();
    updateDatabase("views/IN_VIEW.xml", false);
    assertExportedView("views/IN_VIEW.xml", "views/TEST_VIEW.xml");
  }

  @Test
  public void multipleNotInClauses() throws IOException {
    resetDB();
    updateDatabase("views/NOT_IN_VIEW.xml", false);
    assertExportedView("views/NOT_IN_VIEW.xml", "views/TEST_VIEW.xml");
  }

  private void assertExportedView(String modelFileToCompare, String exportedObjectPath)
      throws IOException {
    assertExportedObject(EXPORT_DIR, modelFileToCompare, exportedObjectPath, VIEW_XML_OPEN_TAG,
        VIEW_XML_CLOSE_TAG);
  }

}
