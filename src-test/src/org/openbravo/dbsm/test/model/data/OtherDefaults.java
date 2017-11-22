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

package org.openbravo.dbsm.test.model.data;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.ddlutils.model.Database;
import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;

public class OtherDefaults extends DbsmTest {

  public OtherDefaults(String rdbms, String driver, String url, String sid, String user,
      String password, String systemUser, String systemPassword, String name)
      throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, systemUser, systemPassword, name);
  }

  @Test
  public void valuesAreKept() throws SQLException {
    resetDB();
    Database db = updateDatabase("createDefault/M2.xml");
    generateData(db, 1);

    String oldValue = getActualValue("test", "m2");
    updateDatabase("createDefault/M2.xml");
    String newValue = getActualValue("test", "m2");
    assertThat("Value is unchanged", newValue, is(equalTo(oldValue)));
  }

  @Test
  public void onCreateDefaultIsNotExecutedInADIfDataPresentCreatingNewTable() throws SQLException {
    resetDB();
    updateDatabase("createDefault/M2.xml", "data/newCreateDefault", Arrays.asList("TEST"));
    assertThat("New AD column value should be kept", getActualValue("test", "m2"),
        is(equalTo("NEW")));
  }

  @Test
  public void onCreateDefaultIsNotExecutedInADIfDataPresent() throws SQLException {
    resetDB();
    updateDatabase("createDefault/BASE_MODEL.xml");
    updateDatabase("createDefault/M2.xml", "data/newCreateDefault", Arrays.asList("TEST"));
    assertThat("New AD column value should be kept", getActualValue("test", "m2"),
        is(equalTo("NEW")));
  }

  @Test
  public void setMandatoryInAD() throws SQLException {
    resetDB();
    updateDatabase("createDefault/NM3.xml", "data/newCreateDefault", Arrays.asList("TEST"));
    updateDatabase("createDefault/NM31.xml", "data/newCreateDefault", Arrays.asList("TEST"));
  }

  @Test
  public void setMandatoryInstance() throws SQLException {
    resetDB();
    updateDatabase("createDefault/NM3.xml");
    updateDatabase("createDefault/NM31.xml");
  }

  @Test
  public void unsetMandatoryInAD() throws SQLException {
    resetDB();
    updateDatabase("createDefault/NM31.xml", "data/newCreateDefault", Arrays.asList("TEST"));
    updateDatabase("createDefault/NM3.xml", "data/newCreateDefault", Arrays.asList("TEST"));
  }

  @Test
  public void unsetMandatoryInstance() throws SQLException {
    resetDB();
    updateDatabase("createDefault/NM31.xml");
    updateDatabase("createDefault/NM3.xml");
  }
}
