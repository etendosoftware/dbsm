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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.codehaus.jettison.json.JSONException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openbravo.dbsm.test.base.DbsmTest;

/**
 * Test cases to test the function based indexes
 * 
 * @author AugustoMauch
 *
 */
@RunWith(Parameterized.class)
public class FunctionBasedIndexes extends DbsmTest {

  private static final String EXPORT_DIR = "/tmp/export-test";
  private static final String VIRTUAL_COLUMN_PREFIX = "SYS_NC";

  private enum TestType {
    onCreate, onUpdate
  }

  private TestType testType;

  private static final Map<String, String> functionBasedIndexColumnMapPostgreSql;
  static {
    functionBasedIndexColumnMapPostgreSql = new HashMap<String, String>();
    functionBasedIndexColumnMapPostgreSql.put("test_col1_upper", "{upper(col1::text)}");
    functionBasedIndexColumnMapPostgreSql.put("test_mixed_indexes1", "{col1,lower(col2::text)}");
    functionBasedIndexColumnMapPostgreSql.put("test_mixed_indexes2", "{lower(col1::text),col2}");
  }

  private static final Map<String, String> functionBasedIndexColumnMapOracle;
  static {
    functionBasedIndexColumnMapOracle = new HashMap<String, String>();
    functionBasedIndexColumnMapOracle.put("test_col1_upper", "{upper(\"col1\")}");
    functionBasedIndexColumnMapOracle.put("test_mixed_indexes1", "{col1,lower(\"col2\")}");
    functionBasedIndexColumnMapOracle.put("test_mixed_indexes2", "{lower(\"col1\"),col2}");
  }

  private static final Map<String, String> simpleIndexColumnMap;
  static {
    simpleIndexColumnMap = new HashMap<String, String>();
    simpleIndexColumnMap.put("basic_index", "{col1}");
  }

  public FunctionBasedIndexes(String rdbms, String driver, String url, String sid, String user,
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
  // Tests that indexes not based on functions are properly imported
  public void importBasicIndex() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_INDEX.xml");
    for (String indexName : simpleIndexColumnMap.keySet()) {
      assertThat(getColumnsFromIndex(indexName), equalTo(simpleIndexColumnMap.get(indexName)));
    }
  }

  @Test
  // Tests that indexes not based on functions are properly exported
  public void exportBasicIndex() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/BASIC_INDEX.xml");

    assertExport("indexes/BASIC_INDEX.xml");
  }

  @Test
  // Tests that function based indexes are properly imported
  public void importFunctionBasedIndexes() {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/FUNCTION_BASED_INDEXES.xml");
    Map<String, String> indexParametersMap = null;
    if (getRdbms() == Rdbms.PG) {
      indexParametersMap = functionBasedIndexColumnMapPostgreSql;
    } else {
      indexParametersMap = functionBasedIndexColumnMapOracle;
    }
    for (String indexName : indexParametersMap.keySet()) {
      assertThat(getColumnsFromIndex(indexName), equalTo(indexParametersMap.get(indexName)));
    }
  }

  @Test
  // Tests that function based indexes are properly exported
  public void exportFunctionBasedIndexes() throws IOException {
    resetDB();
    createDatabaseIfNeeded();
    updateDatabase("indexes/FUNCTION_BASED_INDEXES.xml");

    assertExport("indexes/FUNCTION_BASED_INDEXES.xml");
  }

  @Test
  // Tests that it is possible to define indexes that use non monadic functions
  public void testNonMonadicFunctionBasedIndex() throws IOException {
    resetDB();
    boolean forceCreation = true;
    createDatabaseIfNeeded(forceCreation);
    updateDatabase("indexes/NON_MONADIC_FUNCTION_INDEX.xml");
    assertExport("indexes/NON_MONADIC_FUNCTION_INDEX.xml");
  }

  @Test
  // Tests that it is possible to define indexes ethat use nested functions
  public void testNestedFunctionBasedIndex() throws IOException {
    resetDB();
    boolean forceCreation = true;
    createDatabaseIfNeeded(forceCreation);
    updateDatabase("indexes/NESTED_FUNCTION_INDEX.xml");
    assertExport("indexes/NESTED_FUNCTION_INDEX.xml");
  }

  @Test
  public void recreationFromBasicToFunction() throws IOException {
    assumeThat(testType, is(TestType.onCreate));
    resetDB();
    updateDatabase("indexes/BASIC_INDEX.xml");

    // 2nd update should perform model check, but it doesn't check correctly index type...
    updateDatabase("indexes/FUNCTION_INDEX.xml");

    // ...that's why we compare models now
    assertExport("indexes/FUNCTION_INDEX.xml");
  }

  @Test
  public void recreationFromFunctionToBasic() throws IOException {
    assumeThat(testType, is(TestType.onCreate));
    resetDB();
    updateDatabase("indexes/FUNCTION_INDEX.xml");
    assertExport("indexes/FUNCTION_INDEX.xml");

    // 2nd update should perform model check, but it doesn't check correctly index type...
    updateDatabase("indexes/BASIC_INDEX.xml");

    // ...that's why we compare models now
    assertExport("indexes/BASIC_INDEX.xml");
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

  // Given the name of an index, return a string representation of its column, along with the
  // function applied to them
  private String getColumnsFromIndex(String indexName) {
    String columnsFromIndex = null;
    if (getRdbms() == Rdbms.PG) {
      columnsFromIndex = getColumnsFromIndexPostgreSql(indexName);
    } else {
      columnsFromIndex = getColumnsFromIndexOracle(indexName);
    }
    return columnsFromIndex;
  }

  private String getColumnsFromIndexOracle(String indexName) {
    StringBuilder indexColumns = new StringBuilder();
    indexColumns.append("{");
    Connection cn = null;
    try {
      cn = getDataSource().getConnection();
      PreparedStatement st = null;
      st = cn.prepareStatement(getColumnsFromIndexOracleQuery());
      st.setString(1, indexName.toUpperCase());
      ResultSet rs = st.executeQuery();
      while (rs.next()) {
        String columnName = rs.getString(1);
        if (columnName.startsWith(VIRTUAL_COLUMN_PREFIX)) {
          String indexExpression = rs.getString(2);
          columnName = indexExpression;
        }
        indexColumns.append(columnName.toLowerCase() + ",");
      }
      // remove the last comma
      indexColumns.deleteCharAt(indexColumns.length() - 1);
      indexColumns.append("}");
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
    return indexColumns.toString();
  }

  private String getColumnsFromIndexOracleQuery() {
    StringBuilder query = new StringBuilder();
    query.append("SELECT IC.COLUMN_NAME, IE.COLUMN_EXPRESSION ");
    query.append("FROM USER_IND_COLUMNS IC ");
    query.append("LEFT JOIN USER_IND_EXPRESSIONS IE ON IC.INDEX_NAME = IE.INDEX_NAME ");
    query.append("WHERE IC.INDEX_NAME = ? ");
    query.append("ORDER BY IC.COLUMN_POSITION");
    return query.toString();
  }

  private String getColumnsFromIndexPostgreSql(String indexName) {
    Connection cn = null;
    try {
      cn = getDataSource().getConnection();
      PreparedStatement st = null;
      st = cn.prepareStatement(getColumnsFromIndexPostgreSqlQuery());
      st.setString(1, indexName);
      ResultSet rs = st.executeQuery();
      if (rs.next()) {
        Array array = rs.getArray(1);
        return array.toString();
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
    return "";
  }

  private String getColumnsFromIndexPostgreSqlQuery() {
    StringBuilder query = new StringBuilder();
    query.append("SELECT ARRAY(");
    query.append("       SELECT pg_get_indexdef(PG_INDEX.indexrelid, k + 1, true) ");
    query.append("        FROM generate_subscripts(PG_INDEX.indkey, 1) as k ");
    query.append("       ORDER BY k ");
    query.append("       ) as indkey_names ");
    query.append("FROM PG_INDEX, PG_CLASS, PG_NAMESPACE ");
    query.append("WHERE PG_INDEX.indexrelid = PG_CLASS.OID ");
    query.append("AND PG_CLASS.RELNAMESPACE = PG_NAMESPACE.OID ");
    query.append("AND PG_NAMESPACE.NSPNAME = CURRENT_SCHEMA() ");
    query.append("AND PG_INDEX.INDISPRIMARY ='f' ");
    query.append("AND PG_CLASS.RELNAME = ? ");
    query.append("AND PG_CLASS.RELNAME NOT IN (SELECT pg_constraint.conname::text  ");
    query.append("FROM pg_constraint JOIN pg_class ON pg_class.oid = pg_constraint.conrelid ");
    query.append("WHERE pg_constraint.contype = 'u') ");
    query.append("order by PG_CLASS.RELNAME");
    return query.toString();
  }
}
