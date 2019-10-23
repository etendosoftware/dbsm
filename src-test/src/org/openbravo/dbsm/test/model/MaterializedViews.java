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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;

/**
 * Includes tests related with materialized views
 * 
 */
public class MaterializedViews extends DbsmTest {

  protected static final String EXPORT_DIR = "/tmp/export-test";

  private static final String MATERIALIZED_VIEW_XML_OPEN_TAG = "<materializedView";
  private static final String MATERIALIZED_VIEW_XML_CLOSE_TAG = "</materializedView>";

  public MaterializedViews(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  // Tests that a basic view can be created and exported properly
  public void exportBasicMaterializedView() throws IOException {
    resetDB();
    updateDatabase("materializedViews/BASE_MODEL.xml", false);
    assertExportedMaterializedView("materializedViews/BASE_MODEL.xml",
        "materializedViews/TEST_MATERIALIZEDVIEW.xml");
  }

  @Test
  // Tests that a view which makes use of the SUBSTR function be created and exported properly
  // In PostgresSQL this view will have a ::text cast as part of its SQL code which must be removed
  // properly when exporting the model
  public void exportViewWithTextCast() throws IOException {
    resetDB();
    updateDatabase("materializedViews/TEXT_CAST_TEST_MODEL.xml", false);
    assertExportedMaterializedView("materializedViews/TEXT_CAST_TEST_MODEL.xml",
        "materializedViews/TEST_MATERIALIZEDVIEW.xml");
  }

  @Test
  public void exportViewWithBasicIndex() throws IOException {
    resetDB();
    updateDatabase("materializedViews/BASIC_INDEX_MODEL.xml", false);
    assertExportedMaterializedView("materializedViews/BASIC_INDEX_MODEL.xml",
        "materializedViews/TEST_MATERIALIZEDVIEW.xml");
  }

  @Test
  public void exportViewWithOperatorClassIndex() throws IOException {
    resetDB();
    updateDatabase("materializedViews/OPERATOR_CLASS_INDEX_MODEL.xml", false);
    assertExportedMaterializedView("materializedViews/OPERATOR_CLASS_INDEX_MODEL.xml",
        "materializedViews/TEST_MATERIALIZEDVIEW.xml");
  }

  private void assertExportedMaterializedView(String modelFileToCompare, String exportedObjectPath)
      throws IOException {
    assertExportedObject(EXPORT_DIR, modelFileToCompare, exportedObjectPath,
        MATERIALIZED_VIEW_XML_OPEN_TAG, MATERIALIZED_VIEW_XML_CLOSE_TAG);
  }
}
