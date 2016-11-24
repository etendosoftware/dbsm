/*
 ************************************************************************************
 * Copyright (C) 2016 Openbravo S.L.U.
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.codehaus.jettison.json.JSONException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openbravo.dbsm.test.base.DbsmTest;

@RunWith(Parameterized.class)
public class IndexBaseTest extends DbsmTest {

  protected static final String EXPORT_DIR = "/tmp/export-test";

  protected enum TestType {
    onCreate, onUpdate
  }

  private TestType testType;

  public IndexBaseTest(String rdbms, String driver, String url, String sid, String user,
      String password, String name, TestType testType) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
    this.testType = testType;
  }

  @Parameters(name = "DB: {6} - Test Type: {7}")
  public static Collection<Object[]> parameters() throws IOException, JSONException {
    List<Object[]> configs = new ArrayList<Object[]>();

    for (String[] param : DbsmTest.params()) {
      for (TestType type : TestType.values()) {
        List<Object> p = new ArrayList<Object>(Arrays.asList(param));
        p.add(type);
        configs.add(p.toArray());
      }
    }
    return configs;
  }

  public TestType getTestType() {
    return this.testType;
  }

  protected void createDatabaseIfNeeded() {
    boolean forceCreation = false;
    createDatabaseIfNeeded(forceCreation);
  }

  protected void createDatabaseIfNeeded(boolean forceCreation) {
    // Only start from the base model if the testType if onUpdate or if creation is forced
    if (forceCreation || testType == TestType.onUpdate) {
      updateDatabase("indexes/BASE_MODEL.xml");
    }
  }

  protected void assertExport(String modelFileToCompare, String exportedTablePath)
      throws IOException {
    File exportTo = new File(EXPORT_DIR);
    if (exportTo.exists()) {
      exportTo.delete();
    }
    exportTo.mkdirs();
    exportDatabase(EXPORT_DIR);

    File exportedTable = new File(EXPORT_DIR, exportedTablePath);
    assertThat("exported table exists", exportedTable.exists(), is(true));

    String exportedContents = FileUtils.readFileToString(exportedTable);
    log.debug("exported Contents " + exportedContents);
    String originalContents = FileUtils.readFileToString(new File("model", modelFileToCompare));
    log.debug("original Contents " + originalContents);
    assertThat("exported contents", exportedContents, equalTo(originalContents));
  }

  /**
   * Given the name of a column and its table name, returns the comment of the column
   * 
   * @param tableName
   *          the name of the table to which the column belongs
   * @param columnName
   *          the name of the column
   * @return the comment of the given column
   */
  protected String getCommentOfColumnInOracle(String tableName, String columnName) {
    String columnComment = null;
    Connection con = null;
    try {
      PreparedStatement st = null;
      con = getPlatform().getDataSource().getConnection();
      st = con
          .prepareStatement("SELECT comments FROM all_col_comments WHERE UPPER(table_name) = ? AND UPPER(column_name) = ?");
      st.setString(1, tableName.toUpperCase());
      st.setString(2, columnName.toUpperCase());
      ResultSet rs = st.executeQuery();
      if (rs.next()) {
        columnComment = rs.getString(1);
      }
    } catch (SQLException e) {
      log.error("Error while getting the comment of the column " + tableName + "." + columnName, e);
    } finally {
      getPlatform().returnConnection(con);
    }
    return columnComment;
  }

  /**
   * Given the name of an index, returns the operator class of its first column
   * 
   * @param indexName
   *          the name of the index
   * @return the operator class of the first column of the given index
   */
  protected String getOperatorClassNameForIndexFromDb(String indexName) {
    String operatorClassName = null;
    Connection cn = null;
    try {
      cn = getDataSource().getConnection();
      StringBuilder query = new StringBuilder();
      query.append("SELECT PG_OPCLASS.opcname ");
      query.append("FROM PG_INDEX, PG_CLASS, PG_OPCLASS ");
      query.append("WHERE PG_INDEX.indexrelid = PG_CLASS.OID ");
      query.append("AND PG_OPCLASS.OID = PG_INDEX.indclass[0] ");
      query.append("AND UPPER(PG_CLASS.relname) = ?");
      PreparedStatement st = cn.prepareStatement(query.toString());
      st.setString(1, indexName.toUpperCase());
      ResultSet rs = st.executeQuery();
      if (rs.next()) {
        operatorClassName = rs.getString(1);
      }
    } catch (SQLException e) {
      log.error("Error while getting the name of the operator class of the index " + indexName, e);
    } finally {
      getPlatform().returnConnection(cn);
    }
    return operatorClassName;
  }

  /**
   * Given the name of an index, returns its access method (BTREE, GIN,...)
   * 
   * @param indexName
   *          the name of the index
   * @return the access method of the index.
   */
  protected String getIndexAccessMethodFromDb(String indexName) {
    String indexWhereClause = null;
    Connection connection = null;
    try {
      connection = getDataSource().getConnection();
      StringBuilder query = new StringBuilder();
      query.append("SELECT PG_AM.amname ");
      query.append("FROM PG_INDEX, PG_CLASS, PG_AM ");
      query.append("WHERE PG_INDEX.indexrelid = PG_CLASS.oid ");
      query.append("AND PG_CLASS.relam = PG_AM.oid ");
      query.append("AND UPPER(PG_CLASS.relname) = UPPER(?)");
      PreparedStatement st = connection.prepareStatement(query.toString());
      st.setString(1, indexName.toUpperCase());
      ResultSet rs = st.executeQuery();
      if (rs.next()) {
        indexWhereClause = rs.getString(1);
      }
    } catch (SQLException e) {
      log.error("Error while getting the access method of the index " + indexName, e);
    } finally {
      getPlatform().returnConnection(connection);
    }
    return indexWhereClause;
  }

}
