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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.ddlutils.platform.ExcludeFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases to test the support of indexes intended for fast searching of similar strings.
 * 
 * @author caristu
 *
 */
public class SimilarityIndexes extends IndexBaseTest {

  public SimilarityIndexes(String rdbms, String driver, String url, String sid, String user,
      String password, String name, TestType testType) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, testType);
  }

  @Before
  public void installPgTrgmExtension() {
    if (getRdbms() != Rdbms.PG) {
      return;
    }
    Connection connection = null;
    try {
      connection = getDataSource().getConnection();
      StringBuilder query = new StringBuilder();
      query.append("CREATE EXTENSION IF NOT EXISTS \"pg_trgm\"");
      PreparedStatement st = connection.prepareStatement(query.toString());
      st.execute();
    } catch (SQLException e) {
      log.error("Error while creating pg_trgm extension");
    } finally {
      getPlatform().returnConnection(connection);
    }
    // Configure the exclude filter
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter.fillFromFile(new File("model/excludeFilter/excludePgTrgmFunctions.xml"));
    setExcludeFilter(excludeFilter);
  }

  @After
  public void uninstallPgTrgmExtension() {
    if (getRdbms() != Rdbms.PG) {
      return;
    }
    Connection connection = null;
    try {
      connection = getDataSource().getConnection();
      StringBuilder query = new StringBuilder();
      query.append("DROP EXTENSION \"pg_trgm\" CASCADE");
      PreparedStatement st = connection.prepareStatement(query.toString());
      st.execute();
    } catch (SQLException e) {
      log.error("Error while deleting pg_trgm extension");
    } finally {
      getPlatform().returnConnection(connection);
    }
  }

  @Test
  // Tests that similarity indexes are properly imported
  public void importBasicSimilarityIndex() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_SIMILARITY_INDEX.xml");
    if (Rdbms.PG.equals(getRdbms())) {
      assertIsSimilarityIndex("BASIC_INDEX");
    }
  }

  @Test
  // Tests that an index used for ILIKE comparisons can be imported properly
  // This index is a function based index which makes use of the similarity feature
  public void importIlikeSimilarityIndex() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/ILIKE_INDEX.xml");
    if (Rdbms.PG.equals(getRdbms())) {
      assertIsSimilarityIndex("BASIC_INDEX");
    }
  }

  private void assertIsSimilarityIndex(String indexName) {
    final List<String> expectedConfiguration = Arrays.asList("gin", "gin_trgm_ops");
    String accessMethod = getIndexAccessMethodFromDb(indexName);
    String operatorClassName = getOperatorClassNameForIndexFromDb(indexName);
    assertEquals(expectedConfiguration, Arrays.asList(accessMethod, operatorClassName));
  }

  @Test
  // Tests that similarity indexes are properly exported
  public void exportBasicSimilarityIndex() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_SIMILARITY_INDEX.xml");
    assertExport("indexes/BASIC_SIMILARITY_INDEX.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that an index used for ILIKE comparisons can be exported properly
  public void exportIlikeSimilarityIndex() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/ILIKE_INDEX.xml");
    assertExport("indexes/ILIKE_INDEX.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that an existing basic index can be changed as a similarity one
  public void changeIndexFromBasicToSimilarity() throws IOException {
    assumeThat(getTestType(), is(TestType.onCreate));
    resetDB();
    updateDatabase("indexes/BASIC_INDEX.xml");
    updateDatabase("indexes/BASIC_SIMILARITY_INDEX.xml");
    assertExport("indexes/BASIC_SIMILARITY_INDEX.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that an existing similarity index can be changed as basic
  public void changeIndexFromSimilarityToBasic() throws IOException {
    assumeThat(getTestType(), is(TestType.onCreate));
    resetDB();
    updateDatabase("indexes/BASIC_SIMILARITY_INDEX.xml");
    updateDatabase("indexes/BASIC_INDEX.xml");
    assertExport("indexes/BASIC_INDEX.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that if an index is changed as a similarity one, that index is recreated in postgres
  public void recreationToChangeIndexAsSimilarity() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_INDEX.xml");
    List<String> commands = sqlStatmentsForUpdate("indexes/BASIC_SIMILARITY_INDEX.xml");
    if (Rdbms.PG.equals(getRdbms())) {
      assertThat("Index is dropped", commands, hasItem(containsString("DROP INDEX BASIC_INDEX")));
      assertThat("Index is created", commands, hasItem(containsString("CREATE INDEX BASIC_INDEX")));
    }
  }

  @Test
  // Tests that if a similarity index is changed to be basic, that index is recreated in postgres
  public void recreationToChangeIndexAsBasic() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_SIMILARITY_INDEX.xml");
    List<String> commands = sqlStatmentsForUpdate("indexes/BASIC_INDEX.xml");
    if (Rdbms.PG.equals(getRdbms())) {
      assertThat("Index is dropped", commands, hasItem(containsString("DROP INDEX BASIC_INDEX")));
      assertThat("Index is created", commands, hasItem(containsString("CREATE INDEX BASIC_INDEX")));
    }
  }

}
