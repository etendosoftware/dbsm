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
package org.openbravo.dbsm.test.model.data;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.codehaus.jettison.json.JSONException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openbravo.dbsm.test.base.DbsmTest;

@RunWith(Parameterized.class)
public class DefaultValuesTest extends DbsmTest {

  protected static final String MODEL_DIRECTORY = "defaults/";
  protected static final String TEST_TABLE = "TEST";
  protected static final String TEST_COLUMN = "COL1";
  protected static final String SYSDATE = "SYSDATE";
  protected static final String NO = "'N'";
  protected static final String NULL = "NULL";

  private String baseModel;
  private String updatedModel;
  private String defaultValue;

  public DefaultValuesTest(String rdbms, String driver, String url, String sid, String user,
      String password, String systemUser, String systemPassword, String name, String baseModel,
      String updatedModel, String defaultValue) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, systemUser, systemPassword, name);
    this.baseModel = baseModel;
    this.updatedModel = updatedModel;
    if (SYSDATE.equals(defaultValue)) {
      this.defaultValue = getNowDefaultValue();
    } else {
      this.defaultValue = defaultValue;
    }
  }

  private String getNowDefaultValue() {
    if (getRdbms() == Rdbms.PG) {
      return "NOW()";
    } else {
      return "SYSDATE";
    }
  }

  @Parameters(name = "DB: {6} - base model {7} - updated model {8} - default value {9}")
  public static Collection<Object[]> parameters() throws IOException, JSONException {
    List<Object[]> configs = new ArrayList<Object[]>();
    List<String> baseModels = new ArrayList<String>();
    List<String> updatedModels = new ArrayList<String>();
    List<String> expectedDefaultValues = new ArrayList<String>();

    baseModels.add(MODEL_DIRECTORY + "BASE_MODEL.xml");
    updatedModels.add(MODEL_DIRECTORY + "BASE_MODEL_WITH_DEFAULT.xml");
    expectedDefaultValues.add(SYSDATE);

    baseModels.add(MODEL_DIRECTORY + "BASE_MODEL2.xml");
    updatedModels.add(MODEL_DIRECTORY + "BASE_MODEL2_WITH_DEFAULT.xml");
    expectedDefaultValues.add(NO);

    for (String[] param : DbsmTest.params()) {
      for (int i = 0; i < baseModels.size(); i++) {
        List<Object> p = new ArrayList<Object>(Arrays.asList(param));
        p.add(baseModels.get(i));
        p.add(updatedModels.get(i));
        p.add(expectedDefaultValues.get(i));
        configs.add(p.toArray());
      }
    }
    return configs;
  }

  @Test
  public void defaultValueIsCreated() throws SQLException {
    doDatabaseChanges(baseModel, updatedModel);
    assertIsExpectedDefaultValue(defaultValue);
  }

  @Test
  public void defaultValueIsDropped() throws SQLException {
    doDatabaseChanges(updatedModel, baseModel);
    assertIsExpectedDefaultValue(NULL);
  }

  private void doDatabaseChanges(String originalModel, String finalModel) {
    resetDB();
    createDatabase(originalModel);
    updateDatabase(finalModel);
  }

  private void assertIsExpectedDefaultValue(String expectedDefaultValue) {
    String columnDefaultValue;
    if (getRdbms() == Rdbms.PG) {
      columnDefaultValue = getColumnDefaultValueInPostgres(TEST_TABLE, TEST_COLUMN);
    } else {
      columnDefaultValue = getColumnDefaultValueInOracle(TEST_TABLE, TEST_COLUMN);
    }
    assertThat("Expected Default Value Expression", formatColumnDefaultValue(columnDefaultValue),
        equalTo(expectedDefaultValue));
  }

  private String formatColumnDefaultValue(String originalValue) {
    String newValue = originalValue.replace("\n", "").replace("::character varying", "");
    return newValue.toUpperCase();
  }

  private String getColumnDefaultValueInPostgres(String tableName, String columnName) {
    String postgresSQLQuery = "SELECT d.adsrc AS default_value " //
        + "FROM pg_attribute a " //
        + "LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum " //
        + "JOIN pg_class t ON t.oid = a.attrelid " //
        + "WHERE UPPER(t.relname) = UPPER('" + tableName + "') " //
        + "AND UPPER(a.attname) = UPPER('" + columnName + "') ";
    return getColumnDefaultValue(postgresSQLQuery);
  }

  private String getColumnDefaultValueInOracle(String tableName, String columnName) {
    String oracleSQLQuery = "SELECT data_default AS default_value " //
        + "FROM user_tab_columns " //
        + "WHERE UPPER(table_name) = UPPER('" + tableName + "') " //
        + "AND UPPER(column_name) = UPPER('" + columnName + "') ";
    return getColumnDefaultValue(oracleSQLQuery);
  }

  private String getColumnDefaultValue(String sqlQuery) {
    Connection cn = null;
    try {
      cn = getDataSource().getConnection();
      Statement st = cn.createStatement();
      ResultSet rs = st.executeQuery(sqlQuery);
      if (rs.next()) {
        String result = rs.getString(1);
        if (result == null) {
          return NULL;
        }
        return result;
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (cn != null) {
        try {
          cn.close();
        } catch (SQLException e) {
        }
      }
    }
    return null;
  }

}
