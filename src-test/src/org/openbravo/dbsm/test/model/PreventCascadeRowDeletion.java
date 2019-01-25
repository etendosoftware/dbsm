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
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;

public class PreventCascadeRowDeletion extends DbsmTest {

  private static final String MODEL_NAME = "foreignKeys/TABLES_WITH_FK_CONSTRAINTS_FOR_ROW_DELETION_TEST.xml";
  private static final String TABLE2_NAME = "TABLE2";
  private static final String TABLE3_NAME = "TABLE3";
  private static final String TABLE1_NAME = "TABLE1";
  private static final String TABLE5_NAME = "TABLE5";
  private static final String TABLE6_NAME = "TABLE6";

  public PreventCascadeRowDeletion(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  // When a record of an AD table is deleted, if another AD table references that table, the records
  // which were referenced by the foreign key must be deleted.
  @Test
  public void adTableHasADeletedRowAndADTableReferencesIt() {
    resetDB();
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithTwoRecordsCascadeRowDeletion",
        Arrays.asList(TABLE2_NAME, TABLE3_NAME, TABLE1_NAME));
    List<String> list = sqlStatmentsForUpdate(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithOneRecordCascadeRowDeletion",
        Arrays.asList(TABLE2_NAME, TABLE3_NAME, TABLE1_NAME));
    StringBuilder allSts = new StringBuilder();
    for (String st : list) {
      allSts.append(st);
    }
    assertThat(allSts.toString(), containsString("DELETE FROM "));
  }

  // When a record of an AD table is deleted, if a non AD table references that table, the records
  // which were referenced by the foreign key must be deleted.
  @Test
  public void adTableHasDeletedRowAndNonADTableReferencesIt() {
    resetDB();
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithTwoRecordsCascadeRowDeletion",
        Arrays.asList(TABLE3_NAME));
    List<String> list = sqlStatmentsForUpdate(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithOneRecordCascadeRowDeletion",
        Arrays.asList(TABLE3_NAME));
    StringBuilder allSts = new StringBuilder();
    for (String st : list) {
      allSts.append(st);
    }
    assertThat(allSts.toString(), containsString("DELETE FROM "));
  }

  // When an AD table has no changes of remove type, the records which were referenced by the
  // foreign key mustn't be deleted.
  @Test
  public void adTableHasNoChangesOnUpdateDatabase() {
    resetDB();
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithTwoRecordsCascadeRowDeletion",
        Arrays.asList(TABLE3_NAME));
    List<String> list = sqlStatmentsForUpdate(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithTwoRecordsCascadeRowDeletion",
        Arrays.asList(TABLE3_NAME));
    StringBuilder allSts = new StringBuilder();
    for (String st : list) {
      allSts.append(st);
    }
    assertThat(allSts.toString(), not(containsString("DELETE FROM ")));
  }

  // When a record of an AD table is deleted, if a non AD table references that table, the records
  // which were referenced by the foreign key must be deleted. But if that non AD table references
  // also another AD table which has not deleted records, the records that where referenced by that
  // FK will not be deleted.
  @Test
  public void adTableHasDeletedRowAndNonADTableReferencesItAndAnotherADTable() {
    resetDB();
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithTwoRecordsCascadeRowDeletion",
        Arrays.asList(TABLE5_NAME, TABLE6_NAME));
    List<String> list = sqlStatmentsForUpdate(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithOneRecordCascadeRowDeletion",
        Arrays.asList(TABLE5_NAME, TABLE6_NAME));
    StringBuilder allSts = new StringBuilder();
    for (String st : list) {
      allSts.append(st);
    }
    assertThat(allSts.toString(), (containsString(
        "DELETE FROM TABLE4 t  WHERE NOT EXISTS (SELECT 1 FROM TABLE5 WHERE t.TABLE5_ID=TABLE5.TABLE5_ID) AND t.TABLE5_ID IS NOT NULL")));
    assertThat(allSts.toString(), not(containsString(
        "DELETE FROM TABLE4 t  WHERE NOT EXISTS (SELECT 1 FROM TABLE6 WHERE t.TABLE6_ID=TABLE6.TABLE6_ID) AND t.TABLE6_ID IS NOT NULL")));
  }

  // When there are no changes in one AD table which references another AD table, the foreign key
  // mustn't be deleted .
  @Test
  public void adTableToAdTableHasNoChangesOnUpdateDatabase() {
    resetDB();
    updateDatabase(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithOneRecordCascadeRowDeletion",
        Arrays.asList(TABLE2_NAME, TABLE3_NAME));
    List<String> list = sqlStatmentsForUpdate(MODEL_NAME,
        "data/preventCascadeConstraintDeletion/TablesWithOneRecordCascadeRowDeletion",
        Arrays.asList(TABLE2_NAME, TABLE3_NAME));
    StringBuilder allSts = new StringBuilder();
    for (String st : list) {
      allSts.append(st);
    }
    assertThat(allSts.toString(), not(containsString("DELETE FROM ")));
  }
}
