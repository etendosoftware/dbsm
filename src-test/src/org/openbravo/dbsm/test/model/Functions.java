/*
 ************************************************************************************
 * Copyright (C) 2015-2018 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.dbsm.test.model;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Function;
import org.hamcrest.collection.IsEmptyCollection;
import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;
import org.openbravo.dbsm.test.base.TestLogAppender;

public class Functions extends DbsmTest {

  public Functions(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  /**
   * Checks functions are parameters are properly read from db.
   * 
   * See this jdbc thread: <code>
   * http://www.postgresql.org/message-id/CABtr+CJC54wue94UweBocnDnL0CfM3s6kC4Ckt48NmztwQMdKQ@mail.gmail.com
   * <code>
   */
  @Test
  public void functionParameters() {
    resetDB();
    updateDatabase("functions/F.xml", false);
    Database db = getPlatform().loadModelFromDatabase(getExcludeFilter());

    assertThat("Number of functions in db", db.getFunctionCount(), is(6));
    for (int f = 0; f < db.getFunctionCount(); f++) {
      Function function = db.getFunction(f);
      assertThat("Number of parameters in function " + function.getName(),
          function.getParameterCount(), is(2));
      assertThat("1st param in function " + function.getName(), function.getParameter(0).getName(),
          equalTo("p1"));
      assertThat("2nd param in function " + function.getName(), function.getParameter(1).getName(),
          equalTo("p2"));
    }
  }

  /** see issue #38179 */
  @Test
  public void searchPathIsAddedToNewFunctionsInPG() throws SQLException {
    assumeThat("not executing in Oracle", getRdbms(), is(Rdbms.PG));

    resetDB();
    updateDatabase("functions/SIMPLE_FUNCTION.xml");
    assertThat(getSearchPath("test1"),
        allOf(containsString("search_path"), containsString("public")));
  }

  /** see issue #38179 */
  @Test
  public void searchPathIsAddedToOldFunctionsInPG() throws SQLException {
    assumeThat("not executing in Oracle", getRdbms(), is(Rdbms.PG));

    resetDB();
    createFunctionWithoutSearchPath();

    updateDatabase("functions/SIMPLE_FUNCTION.xml");
    assertThat(getSearchPath("test1"),
        allOf(containsString("search_path"), containsString("public")));
  }

  /** see issue #38179 */
  @Test
  public void invalidFunctionIsIgnored() throws SQLException {
    assumeThat("not executing in Oracle", getRdbms(), is(Rdbms.PG));
    try {
      allowLogErrorsForThisTest();
      resetDB();
      createInvalidFunction();
      updateDatabase("functions/SIMPLE_FUNCTION.xml");
      List<String> warnAndErrors = TestLogAppender.getWarnAndErrors();
      assertThat(warnAndErrors, not(IsEmptyCollection.empty()));
      assertThat(warnAndErrors.get(0),
          containsString("Skipping row: Function parameter without name is not supported"));
    } finally {
      removeInvalidFunction();
    }
  }

  private String getSearchPath(String functionName) throws SQLException {
    try (Connection cn = getDataSource().getConnection();
        PreparedStatement st = cn
            .prepareStatement("select proconfig from pg_proc where proname=?")) {
      st.setString(1, functionName);

      ResultSet rs = st.executeQuery();

      rs.next();
      Array config = rs.getArray(1);
      if (config == null) {
        return null;
      }
      return ((String[]) config.getArray())[0];
    }
  }

  private void createFunctionWithoutSearchPath() throws SQLException {
    String code = "CREATE OR REPLACE FUNCTION TEST1(p1 IN NUMERIC) RETURNS NUMERIC\n" + //
        "\n" + //
        "AS $BODY$ DECLARE \n" + //
        "BEGIN\n" + //
        "  RETURN NULL;\n" + //
        "END ; $BODY$ LANGUAGE plpgsql;";
    try (Connection cn = getDataSource().getConnection();
        PreparedStatement st = cn.prepareStatement(code)) {
      st.execute();
    }
  }

  private void createInvalidFunction() throws SQLException {
    String code = "CREATE FUNCTION bad_function(integer, integer) RETURNS integer " + //
        " AS 'select $1 + $2;' " + //
        " LANGUAGE SQL " + //
        " IMMUTABLE " + //
        " RETURNS NULL ON NULL INPUT;";
    try (Connection cn = getDataSource().getConnection();
        PreparedStatement st = cn.prepareStatement(code)) {
      st.execute();
    }
  }

  private void removeInvalidFunction() throws SQLException {
    String code = "DROP FUNCTION bad_function(integer, integer);";
    try (Connection cn = getDataSource().getConnection();
        PreparedStatement st = cn.prepareStatement(code)) {
      st.execute();
    }
  }
}
