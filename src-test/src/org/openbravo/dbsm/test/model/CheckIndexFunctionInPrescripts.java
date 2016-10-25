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
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openbravo.dbsm.test.base.DbsmTest;

/**
 * Test case to ensure if a new function defined in a prescript and used in a function based index
 * can be exported properly without changes.
 *
 * In case of Oracle, the export database was modifying the function index definition by adding the
 * database owner at the beginning. This change should not be exported in order to avoid a db
 * consistency error. See issue #33659
 * 
 * @author inigo.sanchez
 *
 */

@RunWith(Parameterized.class)
public class CheckIndexFunctionInPrescripts extends DbsmTest {

  private static final String EXPORT_DIR = "/tmp/export-test";

  public CheckIndexFunctionInPrescripts(String rdbms, String driver, String url, String sid,
      String user, String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Before
  public void resetDatabaseAndCreateFunction() {
    resetDB();

    String sql = null;
    if (getRdbms() == Rdbms.PG) {
      sql = createFunctionPostgresql();
    } else {
      sql = createFunctionOracle();
    }
    Connection cn = null;
    try {
      cn = getDataSource().getConnection();
      PreparedStatement st = null;
      st = cn.prepareStatement(sql);
      st.executeQuery();
    } catch (SQLException e) {
      log.error("Testing database function could not be created", e);
    } finally {
      if (cn != null) {
        try {
          cn.close();
        } catch (SQLException e) {
        }
      }
    }
  }

  @Test
  public void isFunctionIndexExportedProperly() throws IOException {
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter.fillFromFile(new File("model/excludeFilter/excludeIndexFunction.xml"));
    setExcludeFilter(excludeFilter);

    updateDatabase("indexes/BASE_FUNCTION_INDEX_PRESCRIPT.xml");
    assertExport("indexes/BASE_FUNCTION_INDEX_PRESCRIPT.xml");
  }

  @After
  public void dropFunction() {
    StringBuilder query = new StringBuilder();
    query.append("DROP function OBEQUALS");
    String sql = query.toString();
    Connection cn = null;
    try {
      cn = getDataSource().getConnection();
      PreparedStatement st = null;
      st = cn.prepareStatement(sql);
      st.executeQuery(sql);
    } catch (SQLException e) {
      log.error("Testing database function could not  be dropped", e);
    } finally {
      if (cn != null) {
        try {
          cn.close();
        } catch (SQLException e) {
        }
      }
    }
  }

  private String createFunctionPostgresql() {
    StringBuilder query = new StringBuilder();
    query.append("CREATE OR REPLACE FUNCTION obequals(");
    query.append("p_number_a numeric) ");
    query.append("RETURNS char AS ");
    query.append("$BODY$ DECLARE ");
    query.append("v_dif NUMERIC;");
    query.append("BEGIN ");
    query.append("v_dif := coalesce(p_number_a, 0) - coalesce(p_number_a, 0);");
    query.append("IF (v_dif = 0) THEN ");
    query.append("return 'Y';");
    query.append("ELSE ");
    query.append("return 'N';");
    query.append("END IF;");
    query.append("END; $BODY$ ");
    query.append("LANGUAGE plpgsql IMMUTABLE");
    return query.toString();
  }

  private String createFunctionOracle() {
    StringBuilder query = new StringBuilder();
    query.append("CREATE OR REPLACE FUNCTION OBEQUALS(p_number_a IN NUMBER) ");
    query.append("RETURN CHAR DETERMINISTIC AS v_dif NUMBER;");
    query.append("BEGIN v_dif := coalesce(p_number_a, 0) - coalesce(p_number_a, 0);");
    query.append("IF (v_dif = 0) THEN ");
    query.append("return 'Y';");
    query.append("ELSE ");
    query.append("return 'N';");
    query.append("END IF;");
    query.append("end OBEQUALS;");
    return query.toString();
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
}
