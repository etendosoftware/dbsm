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

package org.openbravo.dbsm.test.sqlscript;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openbravo.dbsm.test.base.PGOnlyDbsmTest;

public class PgSystemPreScriptTest extends PGOnlyDbsmTest {

  private final String MODEL = "sqlScripts";

  public PgSystemPreScriptTest(String rdbms, String driver, String url, String sid, String user,
      String password, String systemUser, String systemPassword, String name)
      throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, systemUser, systemPassword, name);
  }

  @Before
  @Override
  public void configureExcludeFilter() {
    super.configureExcludeFilter();
  }

  @After
  public void uninstallExtensions() {
    uninstallPgTrgmExtension();
    uninstallUUIDExtension();
  }

  @Test
  public void extensionsAreInstalled() {
    resetDB();
    updateDatabase(MODEL); // this model includes an XML file defining a table + a system pre-script
    List<String> extensions = getInstalledPgExtensions();
    assertThat("Expected extensions are installed", extensions, hasItems("pg_trgm", "uuid-ossp"));
  }

  private List<String> getInstalledPgExtensions() {
    List<String> extensions = new ArrayList<>();
    Connection cn = null;
    try {
      cn = getDataSource().getConnection();
      String query = "SELECT extname FROM pg_extension";
      PreparedStatement st = cn.prepareStatement(query);
      ResultSet rs = st.executeQuery();
      while (rs.next()) {
        String extension = rs.getString(1);
        log.info("Installed extension: " + extension);
        extensions.add(rs.getString(1));
      }
    } catch (SQLException e) {
      log.error("Error while retrieving the installed extensions", e);
    } finally {
      getPlatform().returnConnection(cn);
    }
    return extensions;
  }
}
