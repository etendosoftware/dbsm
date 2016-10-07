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

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
      assertThat(indexWhereClause.toUpperCase(), equalTo("(COL1 IS NOT NULL)"));
    } else if (Rdbms.ORA.equals(getRdbms())) {
      // In Oracle, the partial index definition should be stored in the comment of the first column
      assertThat(getCommentOfColumnInOracle("TEST", "COL1"),
          equalTo("BASIC_INDEX.whereClause=(COL1 IS NOT NULL)$"));
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
    try {
      Connection cn = getDataSource().getConnection();
      StringBuilder query = new StringBuilder();
      query.append("SELECT PG_GET_EXPR(PG_INDEX.indpred, PG_INDEX.indrelid) ");
      query.append("FROM PG_INDEX, PG_CLASS ");
      query.append("WHERE PG_INDEX.indexrelid = PG_CLASS.OID ");
      query.append("AND UPPER(PG_CLASS.relname) = UPPER(?)");
      PreparedStatement st = cn.prepareStatement(query.toString());
      st.setString(1, indexName.toUpperCase());
      ResultSet rs = st.executeQuery();
      if (rs.next()) {
        indexWhereClause = rs.getString(1);
      }
    } catch (SQLException e) {
      log.error("Error while getting the where clause of the index " + indexName, e);
    }
    return indexWhereClause;
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
      assertThat(commands, is(not(empty())));
    } else if (Rdbms.ORA.equals(getRdbms())) {
      assertThat(commands, empty());
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
      assertThat(commands, is(not(empty())));
    } else if (Rdbms.ORA.equals(getRdbms())) {
      assertThat(commands, empty());
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
        equalTo("MULTIPLE_INDEX.whereClause=(COL1 IS NOT NULL AND COL2 IS NOT NULL)$"));
    assertNull(getCommentOfColumnInOracle("TEST", "COL2"));
  }

  @Test
  // Tests if partial indexes are properly exported
  public void exportBasicPartialIndex() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_PARTIAL_INDEX.xml");
    assertExport("indexes/BASIC_PARTIAL_INDEX.xml", "tables/TEST.xml");
  }
}
