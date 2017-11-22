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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

public class ColumnTypeChange extends DataTypeChanges {

  public ColumnTypeChange(String rdbms, String driver, String url, String sid, String user,
      String password, String systemUser, String systemPassword, String name, ActionType type,
      RecreationMode recMode) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, systemUser, systemPassword, name, type, recMode);
  }

  @Test
  public void changeVarcharToNVarchar() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE4.xml");
  }

  @Test
  public void changeNVarcharToVarchar() {
    worksOnlyIn(Rdbms.PG);
    assertTablesAreNotRecreated("DATA_TYPE4.xml", "DATA_TYPE_BASE.xml");
  }

  @Test
  public void changeNVarcharToVarcharORA() {
    worksOnlyIn(Rdbms.ORA);
    assertTablesAreRecreated("DATA_TYPE4.xml", "DATA_TYPE_BASE.xml");
  }

  @Test
  public void changeVarcharToText() {
    worksOnlyIn(Rdbms.PG);
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE5.xml");
  }

  @Test
  public void changeVarcharToORA() {
    worksOnlyIn(Rdbms.ORA);
    assertTablesAreRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE5.xml");
  }

  @Test
  public void changeCharToText() {
    worksOnlyIn(Rdbms.PG);
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE6.xml");
  }

  @Test
  public void changeCharToTextORA() {
    worksOnlyIn(Rdbms.ORA);
    assertTablesAreRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE6.xml");
  }

  @Test
  public void changeTextToVarchar() {
    assertTablesAreRecreated("DATA_TYPE5.xml", "DATA_TYPE_BASE.xml");
  }
}
