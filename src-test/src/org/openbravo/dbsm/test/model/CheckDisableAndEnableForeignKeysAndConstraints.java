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
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openbravo.dbsm.test.base.DbsmTest;

/**
 * Test cases to test it is possible to disable and enable constraints and foreign keys
 * 
 * @author AugustoMauch
 *
 */
@RunWith(Parameterized.class)
public class CheckDisableAndEnableForeignKeysAndConstraints extends DbsmTest {

  public CheckDisableAndEnableForeignKeysAndConstraints(String rdbms, String driver, String url,
      String sid, String user, String password, String name) throws FileNotFoundException,
      IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  public void allConstraintsCanBeDisabled() throws IOException {
    resetDB();
    Database db = updateDatabase("constraints/TWO_TABLES_WITH_CONSTRAINTS.xml");
    Connection con = getPlatform().borrowConnection();
    getPlatform().disableCheckConstraints(con, db, null);
    getPlatform().returnConnection(con);
    // both constraints should be disabled
    assertIsConstraintEnabled("test1_constraint", false);
    assertIsConstraintEnabled("test2_constraint", false);
  }

  @Test
  public void constraintsCanBeDisabledPerTable() throws IOException {
    resetDB();
    Database db = updateDatabase("constraints/TWO_TABLES_WITH_CONSTRAINTS.xml");
    Table t = db.findTable("TEST2");
    Connection con = getPlatform().borrowConnection();
    getPlatform().disableCheckConstraintsForTable(con, t);
    getPlatform().returnConnection(con);
    // only table test2´s constraint should be disabled
    assertIsConstraintEnabled("test1_constraint", true);
    assertIsConstraintEnabled("test2_constraint", false);
  }

  @Test
  public void allForeignKeysCanBeDisabled() throws IOException {
    resetDB();
    Database db = updateDatabase("foreignKeys/TWO_TABLES_WITH_FOREIGN_KEYS.xml");
    Connection con = getPlatform().borrowConnection();
    getPlatform().disableAllFK(con, db, false);
    getPlatform().returnConnection(con);

    // both foreign keys should be disabled
    assertIsConstraintEnabled("test1_fk", false);
    assertIsConstraintEnabled("test2_fk", false);
  }

  @Test
  public void foreignKeysCanBeDisabledPerTable() throws IOException {
    resetDB();
    Database db = updateDatabase("foreignKeys/TWO_TABLES_WITH_FOREIGN_KEYS.xml");
    Table t = db.findTable("TEST2");
    Connection con = getPlatform().borrowConnection();
    getPlatform().disableAllFkForTable(con, t, false);
    getPlatform().returnConnection(con);
    // only table test2´s foreign keys should be disabled (test1_fk is a foreign key defined on
    // test2 that points to the primary key of test1)
    assertIsConstraintEnabled("test1_fk", false);
    assertIsConstraintEnabled("test2_fk", true);
  }

  private void assertIsConstraintEnabled(String constraintName, boolean expectedValue) {
    boolean constraintIsEnabled;
    if (getRdbms() == Rdbms.PG) {
      constraintIsEnabled = isConstraintEnabledInPostgres(constraintName);
    } else {
      constraintIsEnabled = isConstraintEnabledInOracle(constraintName);
    }
    assertThat(constraintIsEnabled, equalTo(expectedValue));
  }

  public boolean isConstraintEnabledInOracle(String constraintName) {
    // in oracle if a constraint is enabled it will be present in the user_constraints table with
    // the status ENABLED
    Connection cn = null;
    try {
      cn = getDataSource().getConnection();
      PreparedStatement st = null;
      st = cn
          .prepareStatement("SELECT count(*) FROM user_constraints WHERE upper(constraint_name) = upper(?) and status = 'ENABLED'");
      st.setString(1, constraintName);
      ResultSet rs = st.executeQuery();
      if (rs.next()) {
        int nRecords = rs.getInt(1);
        return (nRecords == 1);
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
    return false;
  }

  public boolean isConstraintEnabledInPostgres(String constraintName) {
    // in postgres if a constraint is enabled it will be present in the pg_constraint table
    Connection cn = null;
    try {
      cn = getDataSource().getConnection();
      PreparedStatement st = null;
      st = cn
          .prepareStatement("SELECT count(*) FROM pg_constraint WHERE upper(conname) = upper(?)");
      st.setString(1, constraintName);
      ResultSet rs = st.executeQuery();
      if (rs.next()) {
        int nRecords = rs.getInt(1);
        return (nRecords == 1);
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
    return false;
  }

}
