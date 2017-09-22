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
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;

/**
 * Checks that it is possible to change the length of a column (which will cause the recreation of
 * the table) even if there is a view that references that table, and other views that reference the
 * first view
 */
public class ColumnSizeChangesWithDependentViews extends DbsmTest {

  public ColumnSizeChangesWithDependentViews(String rdbms, String driver, String url, String sid,
      String user, String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  /** update.database properly applies column size changes */
  @Test
  public void updateDatabaseShouldApplyScaleChanges() throws CloneNotSupportedException {
    resetDB();
    // loads the initial model, 1 table with a column with size 20, and 3 views
    updateDatabase("columnSizeChangesWithDependentViews/TEST.xml");
    // updates the size of the column to 40
    updateDatabase("columnSizeChangesWithDependentViews/TEST2.xml");

    Database appliedDB = readModelFromDB();
    Table testTable = appliedDB.findTable("TEST");
    Column textColumn = testTable.findColumn("TEXT");
    assertThat(textColumn.getSize(), is("40"));
  }
}
