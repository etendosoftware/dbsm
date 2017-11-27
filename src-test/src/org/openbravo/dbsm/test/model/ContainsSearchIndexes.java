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
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases to test the support of indexes intended to speed up searching using 'contains'
 * operators.
 * 
 * @author caristu
 *
 */
public class ContainsSearchIndexes extends IndexBaseTest {

  public ContainsSearchIndexes(String rdbms, String driver, String url, String sid, String user,
      String password, String name, TestType testType) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, testType);
  }

  @Before
  @Override
  public void installPgTrgmExtension() {
    super.installPgTrgmExtension();
  }

  @After
  @Override
  public void uninstallPgTrgmExtension() {
    super.uninstallPgTrgmExtension();
  }

  @Test
  // Tests that indexes for contains search are properly imported
  public void importContainsSearchIndex() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/CONTAINS_SEARCH_INDEX.xml");
    if (Rdbms.PG.equals(getRdbms())) {
      assertIsContainsSearchIndex("BASIC_INDEX");
    } else if (Rdbms.ORA.equals(getRdbms())) {
      // In Oracle, the contains search index definition should be stored in the comment of the
      // table
      assertThat(getCommentOfTableInOracle("TEST"), equalTo("BASIC_INDEX.containsSearch$"));
    }
  }

  @Test
  // Tests that indexes for icontains search are properly imported
  // This index is a function based index which makes use of the icontains search feature
  public void importIcontainsSearchIndex() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/ICONTAINS_SEARCH_INDEX.xml");
    if (Rdbms.PG.equals(getRdbms())) {
      assertIsContainsSearchIndex("BASIC_INDEX");
    } else if (Rdbms.ORA.equals(getRdbms())) {
      // In Oracle, the contains search index definition should be stored in the comment of the
      // table
      assertThat(getCommentOfTableInOracle("TEST"), equalTo("BASIC_INDEX.containsSearch$"));
    }
  }

  private void assertIsContainsSearchIndex(String indexName) {
    final List<String> expectedConfiguration = Arrays.asList("gin", "gin_trgm_ops");
    String accessMethod = getIndexAccessMethodFromDb(indexName);
    String operatorClassName = getOperatorClassNameForIndexFromDb(indexName);
    assertEquals(expectedConfiguration, Arrays.asList(accessMethod, operatorClassName));
  }

  @Test
  // Tests that indexes for contains search are properly exported
  public void exportContainsSearchIndex() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/CONTAINS_SEARCH_INDEX.xml");
    assertExport("indexes/CONTAINS_SEARCH_INDEX.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that indexes for icontains search are properly exported
  public void exportIcontainsSearchIndex() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/ICONTAINS_SEARCH_INDEX.xml");
    assertExport("indexes/ICONTAINS_SEARCH_INDEX.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that an existing basic index can be changed as a contains search index
  public void changeIndexFromBasicToContainsSearch() throws IOException {
    assumeThat(getTestType(), is(TestType.onCreate));
    resetDB();
    updateDatabase("indexes/BASIC_INDEX.xml");
    updateDatabase("indexes/CONTAINS_SEARCH_INDEX.xml");
    assertExport("indexes/CONTAINS_SEARCH_INDEX.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that an existing contains search index can be changed as basic
  public void changeIndexFromContainsSearchToBasic() throws IOException {
    assumeThat(getTestType(), is(TestType.onCreate));
    resetDB();
    updateDatabase("indexes/CONTAINS_SEARCH_INDEX.xml");
    updateDatabase("indexes/BASIC_INDEX.xml");
    assertExport("indexes/BASIC_INDEX.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that if an index is changed as a contains search one, that index is recreated in postgres
  // but not in oracle
  public void recreationToChangeIndexAsContainsSearch() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_INDEX.xml");
    List<String> commands = sqlStatmentsForUpdate("indexes/CONTAINS_SEARCH_INDEX.xml");
    if (Rdbms.PG.equals(getRdbms())) {
      assertThat("Index is dropped", commands, hasItem(containsString("DROP INDEX BASIC_INDEX")));
      assertThat("Index is created", commands, hasItem(containsString("CREATE INDEX BASIC_INDEX")));
    } else if (Rdbms.ORA.equals(getRdbms())) {
      List<String> commentUpdateCommand = Arrays
          .asList("COMMENT ON TABLE TEST IS 'BASIC_INDEX.containsSearch$'\n");
      assertEquals("Not recreating index", commentUpdateCommand, commands);
    }
  }

  @Test
  // Tests that if a contains search index is changed to be basic, that index is recreated in
  // postgres but not in oracle
  public void recreationToChangeIndexAsBasic() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/CONTAINS_SEARCH_INDEX.xml");
    List<String> commands = sqlStatmentsForUpdate("indexes/BASIC_INDEX.xml");
    if (Rdbms.PG.equals(getRdbms())) {
      assertThat("Index is dropped", commands, hasItem(containsString("DROP INDEX BASIC_INDEX")));
      assertThat("Index is created", commands, hasItem(containsString("CREATE INDEX BASIC_INDEX")));
    } else if (Rdbms.ORA.equals(getRdbms())) {
      List<String> commentUpdateCommand = Arrays.asList("COMMENT ON TABLE TEST IS ''\n");
      assertEquals("Not recreating index", commentUpdateCommand, commands);
    }
  }

  @Test
  // Tests that if a contains search index is removed in Oracle, the comment associated with
  // it is removed from its table
  public void removeIndexShouldRemoveComment() {
    assumeThat("not executing in Postgres", getRdbms(), is(Rdbms.ORA));
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/CONTAINS_SEARCH_INDEX.xml");
    updateDatabase("indexes/BASE_MODEL.xml");
    String tableComment = getCommentOfTableInOracle("TEST");
    assertThat(tableComment, anyOf(isEmptyString(), nullValue()));
  }

  @Test
  // Tests that it is possible to define contains search indexes with multiple
  // columns
  public void exportMultipleContainsSearchIndex() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/MULTIPLE_CONTAINS_SEARCH_INDEX.xml");
    assertExport("indexes/MULTIPLE_CONTAINS_SEARCH_INDEX.xml", "tables/TEST.xml");
  }

  @Test
  // Tests that it is possible to define a partial index to be used for contains search
  public void exportPartialContainsSearchIndex() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/PARTIAL_CONTAINS_SEARCH_INDEX.xml");
    assertExport("indexes/PARTIAL_CONTAINS_SEARCH_INDEX.xml", "tables/TEST.xml");
  }
}
