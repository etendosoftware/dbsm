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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

/**
 * Test cases that covers the support for partial indexes
 * 
 * @author caristu
 *
 */
public class PartialIndexes extends IndexBaseTest {

  public PartialIndexes(String rdbms, String driver, String url, String sid, String user,
      String password, String name, TestType testType) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, testType);
  }

  @Test
  // Tests that partial indexes are properly imported
  public void importBasicPartialIndex() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_PARTIAL_INDEX.xml");
    if (Rdbms.PG.equals(getRdbms())) {
      String indexWhereClause = getWhereClauseForIndexFromDb("BASIC_INDEX");
      assertThat(indexWhereClause.toUpperCase(), equalTo("COL1 IS NOT NULL"));
    } else if (Rdbms.ORA.equals(getRdbms())) {
      // In Oracle, the partial index definition should be stored in the comment of the first column
      assertThat(getCommentOfColumnInOracle("TEST", "COL1"),
          equalTo("BASIC_INDEX.whereClause=COL1 IS NOT NULL$"));
    }
  }

  @Test
  // Tests that partial indexes which make use of functions within the where clause are properly
  // imported
  public void importFunctionPartialIndex() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/FUNCTION_PARTIAL_INDEX.xml");
    if (Rdbms.PG.equals(getRdbms())) {
      String indexWhereClause = getWhereClauseForIndexFromDb("FUNCTION_INDEX");
      assertThat(indexWhereClause.toUpperCase(), equalTo("UPPER(COL1::TEXT) = ''::TEXT"));
    } else if (Rdbms.ORA.equals(getRdbms())) {
      // In Oracle, the partial index definition should be stored in the comment of the first column
      assertThat(getCommentOfColumnInOracle("TEST", "COL1"),
          equalTo("FUNCTION_INDEX.whereClause=UPPER(COL1)=''$"));
    }
  }

  /**
   * Given the name of an index, returns its where clause definition.
   * 
   * @param indexName
   *          the name of the index
   * @return the where clause of the index (if defined). Otherwise, null is returned
   */
  private String getWhereClauseForIndexFromDb(String indexName) {
    String indexWhereClause = null;
    Connection connection = null;
    try {
      connection = getDataSource().getConnection();
      StringBuilder query = new StringBuilder();
      query.append("SELECT PG_GET_EXPR(PG_INDEX.indpred, PG_INDEX.indrelid, true) ");
      query.append("FROM PG_INDEX, PG_CLASS ");
      query.append("WHERE PG_INDEX.indexrelid = PG_CLASS.OID ");
      query.append("AND UPPER(PG_CLASS.relname) = UPPER(?)");
      PreparedStatement st = connection.prepareStatement(query.toString());
      st.setString(1, indexName.toUpperCase());
      ResultSet rs = st.executeQuery();
      if (rs.next()) {
        indexWhereClause = rs.getString(1);
      }
    } catch (SQLException e) {
      log.error("Error while getting the where clause of the index " + indexName, e);
    } finally {
      getPlatform().returnConnection(connection);
    }
    return indexWhereClause;
  }

  @Test
  // Tests that an existing basic index can be changed as partial
  public void changeIndexFromBasicToPartial() throws IOException {
    assumeThat(getTestType(), is(TestType.onCreate));
    resetDB();
    updateDatabase("indexes/BASIC_INDEX.xml");
    updateDatabase("indexes/BASIC_PARTIAL_INDEX.xml");
    assertExport("indexes/BASIC_PARTIAL_INDEX.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that an existing partial index can be changed as not partial
  public void changeIndexFromPartialToBasic() throws IOException {
    assumeThat(getTestType(), is(TestType.onCreate));
    resetDB();
    updateDatabase("indexes/BASIC_PARTIAL_INDEX.xml");
    updateDatabase("indexes/BASIC_INDEX.xml");
    assertExport("indexes/BASIC_INDEX.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that it is possible to replace the where clause of an existing partial index
  public void changeIndexPartialIndexWhereClause() throws IOException {
    assumeThat(getTestType(), is(TestType.onCreate));
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_PARTIAL_INDEX2.xml");
    updateDatabase("indexes/BASIC_PARTIAL_INDEX3.xml");
    assertExport("indexes/BASIC_PARTIAL_INDEX3.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that if an index is changed as partial, that index is recreated in postgres
  // but not in oracle
  public void recreationToChangeIndexAsPartial() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_INDEX.xml");
    List<String> commands = sqlStatmentsForUpdate("indexes/BASIC_PARTIAL_INDEX.xml");
    if (Rdbms.PG.equals(getRdbms())) {
      assertThat("Index is dropped", commands, hasItem(containsString("DROP INDEX BASIC_INDEX")));
      assertThat("Index is created", commands, hasItem(containsString("CREATE INDEX BASIC_INDEX")));
    } else if (Rdbms.ORA.equals(getRdbms())) {
      List<String> commentUpdateCommand = Arrays
          .asList("COMMENT ON COLUMN TEST.COL1 IS 'BASIC_INDEX.whereClause=COL1 IS NOT NULL$'\n");
      assertEquals("Not recreating index", commentUpdateCommand, commands);
    }
  }

  @Test
  // Tests that if an index is changed to be not partial, that index is recreated in postgres
  // but not in oracle
  public void recreationToChangeIndexAsNotPartial() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_PARTIAL_INDEX.xml");
    List<String> commands = sqlStatmentsForUpdate("indexes/BASIC_INDEX.xml");
    if (Rdbms.PG.equals(getRdbms())) {
      assertThat("Index is dropped", commands, hasItem(containsString("DROP INDEX BASIC_INDEX")));
      assertThat("Index is created", commands, hasItem(containsString("CREATE INDEX BASIC_INDEX")));
    } else if (Rdbms.ORA.equals(getRdbms())) {
      List<String> commentUpdateCommand = Arrays.asList("COMMENT ON COLUMN TEST.COL1 IS ''\n");
      assertEquals("Not recreating index", commentUpdateCommand, commands);
    }
  }

  @Test
  // Tests that if a partial index is removed in Oracle, the comment associated with
  // it is removed from its column
  public void removeIndexShouldRemoveComment() {
    assumeThat("not executing in Postgres", getRdbms(), is(Rdbms.ORA));
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_PARTIAL_INDEX.xml");
    updateDatabase("indexes/BASE_MODEL.xml");
    String tableComment = getCommentOfColumnInOracle("TEST", "COL1");
    assertThat(tableComment, anyOf(isEmptyString(), nullValue()));
  }

  @Test
  // Tests that the comment associated to the partial index in Oracle is always placed within the
  // first column of the index
  public void indexCommentShouldBeInFirstColumn() {
    assumeThat("not executing in Postgres", getRdbms(), is(Rdbms.ORA));
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/MULTIPLE_COLUMN_PARTIAL_INDEX.xml");
    // In Oracle, the partial index definition should be stored in the comment of the first column
    assertThat(getCommentOfColumnInOracle("TEST", "COL1"),
        equalTo("MULTIPLE_INDEX.whereClause=COL1 IS NOT NULL AND COL2 IS NOT NULL$"));
    assertNull(getCommentOfColumnInOracle("TEST", "COL2"));
  }

  @Test
  // Tests if basic partial indexes are properly exported
  public void exportBasicPartialIndex() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_PARTIAL_INDEX.xml");
    assertExport("indexes/BASIC_PARTIAL_INDEX.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that it is possible to define a partial index whose where clause is casted (::text) in
  // PostgresSQL
  public void exportBasicPartialIndexCasted() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_PARTIAL_INDEX2.xml");
    assertExport("indexes/BASIC_PARTIAL_INDEX2.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that it is possible to define a partial index whose where clause contains a ';' character
  public void exportBasicPartialIndexWithSemicolon() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_PARTIAL_INDEX3.xml");
    assertExport("indexes/BASIC_PARTIAL_INDEX3.xml", "tables/TEST.xml");
  }

  @Test
  // Tests if partial indexes which apply to multiple columns are properly exported
  public void exportMultiplePartialIndex() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/MULTIPLE_COLUMN_PARTIAL_INDEX.xml");
    assertExport("indexes/MULTIPLE_COLUMN_PARTIAL_INDEX.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that partial indexes expressions which apply to multiple columns preserve the white
  // spaces between the DB operators
  public void exportMultiplePartialIndex2() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/MULTIPLE_COLUMN_PARTIAL_INDEX2.xml");
    assertExport("indexes/MULTIPLE_COLUMN_PARTIAL_INDEX2.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that it is possible to define partial indexes which make use of functions
  public void exportFunctionPartialIndex() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/FUNCTION_PARTIAL_INDEX.xml");
    assertExport("indexes/FUNCTION_PARTIAL_INDEX.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that it is possible to define partial indexes that make use of nested functions
  public void exportNestedFunctionPartialIndex() throws IOException {
    resetDB();
    boolean forceCreation = true;
    createDatabaseIfNeeded(forceCreation);
    updateDatabase("indexes/NESTED_FUNCTION_PARTIAL_INDEX.xml");
    assertExport("indexes/NESTED_FUNCTION_PARTIAL_INDEX.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that if a partial index makes use of an expression with whitespaces inside quotes, those
  // whitespaces are not removed
  public void exportQuotedBlankSpacesInPartialIndex() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/FUNCTION_PARTIAL_INDEX_WITH_QUOTED_BLANKSPACES.xml");
    assertExport("indexes/FUNCTION_PARTIAL_INDEX_WITH_QUOTED_BLANKSPACES.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that it is possible to add a column with a partial index and an on create default
  // statement within the same update
  public void exportPartialIndexAndOnCreateDefault() throws IOException {
    assumeThat(getTestType(), is(TestType.onCreate));
    resetDB();
    updateDatabase("indexes/BASIC_PARTIAL_INDEX_AND_ON_CREATE_DEFAULT.xml");
    assertExport("indexes/BASIC_PARTIAL_INDEX_AND_ON_CREATE_DEFAULT.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that it is possible to add a partial index on a column which has an on create default
  // statement
  public void exportAddPartialIndexOnColumnWithOnCreateDefault() throws IOException {
    assumeThat(getTestType(), is(TestType.onCreate));
    resetDB();
    updateDatabase("indexes/BASE_MODEL_ON_CREATE_DEFAULT.xml");
    updateDatabase("indexes/BASIC_PARTIAL_INDEX_AND_ON_CREATE_DEFAULT.xml");
    assertExport("indexes/BASIC_PARTIAL_INDEX_AND_ON_CREATE_DEFAULT.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that it is possible to drop a partial index on a column which has an on create default
  // statement
  public void exportDropPartialIndexOnColumnWithOnCreateDefault() throws IOException {
    assumeThat(getTestType(), is(TestType.onCreate));
    resetDB();
    updateDatabase("indexes/BASIC_PARTIAL_INDEX_AND_ON_CREATE_DEFAULT.xml");
    updateDatabase("indexes/BASE_MODEL_ON_CREATE_DEFAULT.xml");
    assertExport("indexes/BASE_MODEL_ON_CREATE_DEFAULT.xml", "tables/TEST.xml");
  }
}
