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

public class ColumnSizeChange extends DataTypeChanges {

  public ColumnSizeChange(String rdbms, String driver, String url, String sid, String user,
      String password, String systemUser, String systemPassword, String name, ActionType type,
      RecreationMode recMode) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, systemUser, systemPassword, name, type, recMode);
  }

  @Test
  public void increaseVarcharSize() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE2.xml");
  }

  @Test
  public void increaseCharSize() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE3.xml");
  }

  @Test
  public void increaseNumberPrecisionNoScale() {
    assertTablesAreNotRecreated("DATA_TYPE_NUMBERS_BASE.xml", "DATA_TYPE_NUMBERS1.xml");
  }

  @Test
  public void increaseScaleKeepPrecision() {
    assertTablesAreRecreated("DATA_TYPE_NUMBERS_BASE.xml", "DATA_TYPE_NUMBERS2.xml", false);
  }

  @Test
  public void decreaseScaleKeepPrecision() {
    // ORA-01440: column to be modified must be empty to decrease precision or scale
    worksOnlyIn(Rdbms.PG);

    assertTablesAreNotRecreated("DATA_TYPE_NUMBERS2.xml", "DATA_TYPE_NUMBERS_BASE.xml");
  }

  @Test
  public void decreaseScaleKeepPrecisionORA() {
    // this case is not supported in ORA, so table should always be recreated
    worksOnlyIn(Rdbms.ORA);

    assertTablesAreRecreated("DATA_TYPE_NUMBERS2.xml", "DATA_TYPE_NUMBERS_BASE.xml");
  }

  @Test
  public void fromAnyPrecisionToFixedPrecision() {
    // ORA-01440: column to be modified must be empty to decrease precision or scale
    // it works in PG as far as data doesn't overflow new restriction
    worksOnlyIn(Rdbms.PG);

    assertTablesAreNotRecreated("DATA_TYPE_NUMBERS_BASE.xml", "DATA_TYPE_NUMBERS3.xml");
  }

  @Test
  public void fromAnyPrecisionToFixedPrecisionORA() {
    // this case is not supported in ORA, so table should always be recreated
    worksOnlyIn(Rdbms.ORA);

    assertTablesAreRecreated("DATA_TYPE_NUMBERS_BASE.xml", "DATA_TYPE_NUMBERS3.xml");
  }

}
