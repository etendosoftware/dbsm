/*
 ************************************************************************************
 * Copyright (C) 2015 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.dbsm.test.model;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openbravo.dbsm.test.base.DbsmTest;

/**
 * Test cases to test the index operator class support
 * 
 * @author AugustoMauch
 *
 */
@RunWith(Parameterized.class)
public class OperatorClassIndexes extends DbsmTest {

  private static final String EXPORT_DIR = "/tmp/export-test";

  private enum TestType {
    onCreate, onUpdate
  }

  private TestType testType;

  public OperatorClassIndexes(String rdbms, String driver, String url, String sid, String user,
      String password, String name, TestType testType) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
    this.testType = testType;
  }

  @Parameters(name = "DB: {6} - {7}")
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

  private void createDatabaseIfNeeded() {
    boolean forceCreation = false;
    createDatabaseIfNeeded(forceCreation);
  }

  private void createDatabaseIfNeeded(boolean forceCreation) {
    // Only start from the base model if the testType if onUpdate or if creation is forced
    if (forceCreation || testType == TestType.onUpdate) {
      updateDatabase("indexes/BASE_MODEL.xml");
    }
  }

  @Test
  // Tests that function based indexes with a defined operator class are properly imported
  public void importBasicIndexWithOperatorClass() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_INDEX_WITH_OPERATOR_CLASS.xml");
    if (Rdbms.PG.equals(getRdbms())) {
      String operatorClassName = getOperatorClassNameForIndexFromDb("OP_CLASS_INDEX1");
      assertThat(operatorClassName, equalTo("varchar_pattern_ops"));
    } else if (Rdbms.ORA.equals(getRdbms())) {
      // In Oracle, the operator class should be stored in the comment of the table
      assertThat(getCommentOfTableInOracle("TEST"),
          equalTo("OP_CLASS_INDEX1.COL1.operatorClass=varchar_pattern_ops$"));
    }
  }

  @Test
  // Tests that function based indexes with a defined operator class are properly imported
  public void importFunctionBasedIndexWithOperatorClass() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/FUNCTION_INDEX_WITH_OPERATOR_CLASS.xml");
    if (Rdbms.PG.equals(getRdbms())) {
      String operatorClassName = getOperatorClassNameForIndexFromDb("OP_CLASS_INDEX1");
      assertThat(operatorClassName, equalTo("varchar_pattern_ops"));
    } else if (Rdbms.ORA.equals(getRdbms())) {
      // In Oracle, the operator class should be stored in the comment of the table
      assertThat(getCommentOfTableInOracle("TEST"),
          equalTo("OP_CLASS_INDEX1.functionBasedColumn.operatorClass=varchar_pattern_ops$"));
    }
  }

  @Test
  // Tests that if an operator class is added to an index, that index is recreated in postgres
  // but not in oracle
  public void recreationToAddOperatorClassToIndex() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/FUNCTION_INDEX_WITHOUT_OPERATOR_CLASS.xml");
    List<String> commands = sqlStatmentsForUpdate("indexes/FUNCTION_INDEX_WITH_OPERATOR_CLASS.xml");
    if (Rdbms.PG.equals(getRdbms())) {
      assertThat(commands, is(not(empty())));
    } else if (Rdbms.ORA.equals(getRdbms())) {
      assertThat(commands, empty());
    }
  }

  @Test
  // Tests that if an operator class is removed from an index, that index is recreated in postgres
  // but not in oracle
  public void recreationToRemoveOperatorClassToIndex() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/FUNCTION_INDEX_WITH_OPERATOR_CLASS.xml");
    List<String> commands = sqlStatmentsForUpdate("indexes/FUNCTION_INDEX_WITHOUT_OPERATOR_CLASS.xml");
    if (Rdbms.PG.equals(getRdbms())) {
      assertThat(commands, is(not(empty())));
    } else if (Rdbms.ORA.equals(getRdbms())) {
      assertThat(commands, empty());
    }
  }

  @Test
  // Tests that if an index with an operator class is removed in Oracle, the comment associated with
  // it is removed from its table
  public void removeIndexShouldRemoveComment() {
    assumeThat("not executing in Postgres", getRdbms(), is(Rdbms.ORA));
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/FUNCTION_INDEX_WITH_OPERATOR_CLASS.xml");
    updateDatabase("indexes/BASE_MODEL.xml");
    String tableComment = getCommentOfTableInOracle("TEST");
    assertThat(tableComment, anyOf(isEmptyString(), nullValue()));
  }

  @Test
  // Tests that function based indexes with operator class are properly exported
  public void exportFunctionBasedIndexesWithOperatorClass() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/FUNCTION_INDEX_WITH_OPERATOR_CLASS.xml");
    assertExport("indexes/FUNCTION_INDEX_WITH_OPERATOR_CLASS.xml");
  }

  @Test
  // Tests that basic indexes with operator class are properly exported
  public void exportBasicIndexWithOperatorClass() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_INDEX_WITH_OPERATOR_CLASS.xml");
    assertExport("indexes/BASIC_INDEX_WITH_OPERATOR_CLASS.xml");
  }

  @Test
  // Tests that function based indexes are properly exported
  public void exportFunctionBasedIndexesWithOperatorClass2() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/FUNCTION_INDEX_WITH_OPERATOR_CLASS.xml");
    updateDatabase("indexes/OTHER_FUNCTION_INDEX_WITH_OPERATOR_CLASS.xml");
    assertExport("indexes/OTHER_FUNCTION_INDEX_WITH_OPERATOR_CLASS.xml");
  }

  /**
   * Given a table, return its comment
   * 
   * @param tableName
   *          the name of table
   * @return the comment of the given table
   */
  private String getCommentOfTableInOracle(String tableName) {
    String tableComment = null;
    try {
      PreparedStatement st = null;
      Connection con = getPlatform().getDataSource().getConnection();
      st = con
          .prepareStatement("SELECT comments FROM all_tab_comments WHERE UPPER(table_name) = ?");
      st.setString(1, tableName.toUpperCase());
      ResultSet rs = st.executeQuery();
      if (rs.next()) {
        tableComment = rs.getString(1);
      }
    } catch (SQLException e) {
      log.error("Error while getting the comment of the table " + tableName, e);
    }
    return tableComment;
  }

  private void assertExport(String modelFileToCompare) throws IOException {
    File exportTo = new File(EXPORT_DIR);
    if (exportTo.exists()) {
      exportTo.delete();
    }
    exportTo.mkdirs();
    exportDatabase(EXPORT_DIR);

    File exportedTable = new File(EXPORT_DIR, "tables/TEST.xml");
    assertThat("exported table exists", exportedTable.exists(), is(true));

    String exportedContents = FileUtils.readFileToString(exportedTable);
    String originalContents = FileUtils.readFileToString(new File("model", modelFileToCompare));
    assertThat("exported contents", exportedContents, equalTo(originalContents));
  }

  /**
   * Given the name of an index, returns the operator class of its first column
   * 
   * @param indexName
   *          the name of the index
   * @return the operator class of the first column of the given index
   */
  private String getOperatorClassNameForIndexFromDb(String indexName) {
    String operatorClassName = null;
    try {
      Connection cn = getDataSource().getConnection();
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
    }
    return operatorClassName;
  }

}