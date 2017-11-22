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

package org.openbravo.dbsm.test.model;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;

public class PreventConstraintDeletion extends DbsmTest {

  private static final String MODEL_NAME = "foreignKeys/TABLES_WITH_FK_CONSTRAINTS.xml";
  private static final String ADTABLE1_NAME = "TABLE1";
  private static final String ADTABLE2_NAME = "TABLE2";
  private static final String ADTABLE3_NAME = "TABLE3";
  private static final String ADTABLE4_NAME = "TABLE4";
  private static final String TABLE1_FK_NAME = "TABLE1_FK";
  private static final String TABLE2_FK_NAME = "TABLE2_FK";
  private static final String TABLE4_FK_NAME = "TABLE4_FK";

  public PreventConstraintDeletion(String rdbms, String driver, String url, String sid,
      String user, String password, String systemUser, String systemPassword, String name)
      throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, systemUser, systemPassword, name);
  }

  // When a record of a non AD table is deleted, the constraints don't have to be recreated.
  @Test
  public void constraintsOfNonADTablesAreNotRecreatedWhenDeletingARecord() {
    resetDB();
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithTwoRecordsConstraint",
        Arrays.asList(ADTABLE1_NAME, ADTABLE2_NAME));
    String fkOidBeforeUpdate = obtainFkData(TABLE4_FK_NAME);
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithOneRecordConstraint",
        Arrays.asList(ADTABLE1_NAME, ADTABLE2_NAME));
    String fkOidAfterUpdate = obtainFkData(TABLE4_FK_NAME);
    assertThat(fkOidBeforeUpdate, equalTo(fkOidAfterUpdate));
  }

  // When a Record of a non AD table is deleted, the constraints of AD tables don't have to be
  // recreated.
  @Test
  public void constraintsOfADTableAreNotRecreatedWhenDeletingARecordOfANonADTable() {
    resetDB();
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithTwoRecordsConstraint",
        Arrays.asList(ADTABLE2_NAME));
    String fkOidBeforeUpdate = obtainFkData(TABLE2_FK_NAME);
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithOneRecordConstraint",
        Arrays.asList(ADTABLE2_NAME));
    String fkOidAfterUpdate = obtainFkData(TABLE2_FK_NAME);
    assertThat(fkOidBeforeUpdate, equalTo(fkOidAfterUpdate));
  }

  // When a record of an AD table is deleted, the constraint that references that table must be
  // recreated.
  @Test
  public void constraintsOfADTablesHaveToBeRecreatedWhenDeletingARecordOfTheADTable() {
    resetDB();
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithTwoRecordsConstraint",
        Arrays.asList(ADTABLE1_NAME, ADTABLE2_NAME));
    String adTable1FkOidBeforeUpdate = obtainFkData(TABLE1_FK_NAME);
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithOneRecordConstraint",
        Arrays.asList(ADTABLE1_NAME, ADTABLE2_NAME));
    String adTable1FkOidAfterUpdate = obtainFkData(TABLE1_FK_NAME);
    assertThat(adTable1FkOidBeforeUpdate, not(equalTo(adTable1FkOidAfterUpdate)));
  }

  // When a record of an AD table is deleted, the constraint which references that table must be
  // recreated, but not other AD constraints.
  @Test
  public void constraintsOfADTablesDontHaveToBeRecreatedWhenDeletingARecordOfAnotherADTable() {
    resetDB();
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithTwoRecordsConstraint",
        Arrays.asList(ADTABLE1_NAME, ADTABLE2_NAME));
    String adTable2FkOidBeforeUpdate = obtainFkData(TABLE2_FK_NAME);
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithOneRecordConstraint",
        Arrays.asList(ADTABLE1_NAME, ADTABLE2_NAME));
    String adTable2FkOidAfterUpdate = obtainFkData(TABLE2_FK_NAME);
    assertThat(adTable2FkOidBeforeUpdate, equalTo(adTable2FkOidAfterUpdate));
  }

  // When a record of AD table is updated, the constraints which reference that table don't have to
  // be recreated.
  @Test
  public void constraintsOfADTablesAreNotRecreatedWhenUpdatingATable() {
    resetDB();
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithOneRecordConstraint",
        Arrays.asList(ADTABLE1_NAME, ADTABLE2_NAME));
    String fkOidBeforeUpdate = obtainFkData(TABLE1_FK_NAME);
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithUpdatedRecordConstraint",
        Arrays.asList(ADTABLE1_NAME, ADTABLE2_NAME));
    String fkOidAfterUpdate = obtainFkData(TABLE1_FK_NAME);
    assertThat(fkOidBeforeUpdate, equalTo(fkOidAfterUpdate));
  }

  // When a new record is inserted in an AD table, the constraints which reference that table
  // have to be recreated.
  @Test
  public void constraintsOfADTablesAreRecreatedWhenInsertingARecordInATable() {
    resetDB();
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithOneRecordConstraint",
        Arrays.asList(ADTABLE1_NAME, ADTABLE2_NAME));
    String fkOidBeforeUpdate = obtainFkData(TABLE1_FK_NAME);
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithTwoRecordsConstraint",
        Arrays.asList(ADTABLE1_NAME, ADTABLE2_NAME));
    String fkOidAfterUpdate = obtainFkData(TABLE1_FK_NAME);
    assertThat(fkOidBeforeUpdate, not(equalTo(fkOidAfterUpdate)));
  }

  // When there are no changes, the AD table to AD table constraints don't have to be recreated.
  @Test
  public void constraintsOfADTablesAreNotRecreatedWhenThereAreNoChanges() {
    resetDB();
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithOneRecordConstraint",
        Arrays.asList(ADTABLE3_NAME, ADTABLE4_NAME));
    String fkOidBeforeUpdate = obtainFkData(TABLE4_FK_NAME);
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithOneRecordConstraint",
        Arrays.asList(ADTABLE3_NAME, ADTABLE4_NAME));
    String fkOidAfterUpdate = obtainFkData(TABLE4_FK_NAME);
    assertThat(fkOidBeforeUpdate, equalTo(fkOidAfterUpdate));
  }

  // When there is an insertion change into an AD table, the constraints have to be recreated.
  @Test
  public void constraintsOfADTablesAreRecreatedWhenThereIsAnInsertion() {
    resetDB();
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithOneRecordConstraint",
        Arrays.asList(ADTABLE1_NAME, ADTABLE3_NAME));
    String fkOidBeforeUpdate = obtainFkData(TABLE1_FK_NAME);
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithTwoRecordsConstraint",
        Arrays.asList(ADTABLE1_NAME, ADTABLE3_NAME));
    String fkOidAfterUpdate = obtainFkData(TABLE1_FK_NAME);
    assertThat(fkOidBeforeUpdate, not(equalTo(fkOidAfterUpdate)));
  }

  // When there is a deletion change into an AD table, the constraints have to be recreated.
  @Test
  public void constraintsOfADTablesAreRecreatedWhenThereIsADeletion() {
    resetDB();
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithTwoRecordsConstraint",
        Arrays.asList(ADTABLE1_NAME, ADTABLE3_NAME));
    String fkOidBeforeUpdate = obtainFkData(TABLE1_FK_NAME);
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithOneRecordConstraint",
        Arrays.asList(ADTABLE1_NAME, ADTABLE3_NAME));
    String fkOidAfterUpdate = obtainFkData(TABLE1_FK_NAME);
    assertThat(fkOidBeforeUpdate, not(equalTo(fkOidAfterUpdate)));
  }

  private String obtainFkData(String fkName) {
    String fkData;
    if (getRdbms() == Rdbms.PG) {
      fkData = getForeignKeyIDPostgres(fkName);
    } else {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      fkData = getUpdatedDateOfFkracle(fkName);
    }
    return fkData;
  }

  private String getForeignKeyIDPostgres(String fk) {
    String oid = "";
    try (Connection cn = getDataSource().getConnection()) {
      PreparedStatement st = null;
      st = cn.prepareStatement("SELECT oid FROM pg_constraint WHERE lower(conname) = lower(?)");
      st.setString(1, fk);
      ResultSet rs = st.executeQuery();
      rs.next();
      oid = rs.getString(1);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return oid;
  }

  private String getUpdatedDateOfFkracle(String fk) {
    String oid = "";
    try (Connection cn = getDataSource().getConnection()) {
      PreparedStatement st = null;
      st = cn
          .prepareStatement("SELECT to_char(last_change, 'DD:MM:YYYY HH24:MI:SS') FROM USER_CONSTRAINTS WHERE upper(constraint_name) =  upper(?)");
      st.setString(1, fk);
      ResultSet rs = st.executeQuery();
      rs.next();
      oid = rs.getString(1);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return oid;
  }
}
