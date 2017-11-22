/*
 ************************************************************************************
 * Copyright (C) 2015-2017 Openbravo S.L.U.
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
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.openbravo.dbsm.test.base.DbsmTest;

/**
 * Test cases for DB sequence management
 * 
 * @author alostale
 *
 */
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class Sequences extends DbsmTest {
  private static final String SEQ_NAME = "testseq1";
  private static final String EXPORT_DIR = "/tmp/export-test";

  public Sequences(String rdbms, String driver, String url, String sid, String user,
      String password, String systemUser, String systemPassword, String name)
      throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, systemUser, systemPassword, name);
    modelPath = "sequences/TESTSEQ.xml";
  }

  /** Removes everything in DB to start with a fresh DB */
  @Test
  public void seq000CleanUpDB() {
    resetDB();
  }

  /** Creates a sequence from xml and checks it is present */
  @Test
  public void seq010shouldBeCreated() throws SQLException {
    updateDatabase();
    int val = getNextVal(SEQ_NAME);
    assertThat(val, is(10));
  }

  /** Updates db having a sequence, and checks it has not been recreated */
  @Test
  public void seq020shouldNotBeRecreated() throws SQLException {
    updateDatabase();
    int val = getNextVal(SEQ_NAME);
    assertThat(val, is(11));
  }

  /**
   * Modifies sequence definition (changes increment from 1 to 10) it checks the change has been
   * applied but the sequence has not been recreated
   */
  @Test
  public void seq030shouldBeUpdated() throws SQLException {
    updateDatabase("sequences/TESTSEQ-update.xml");
    int val = getNextVal(SEQ_NAME);
    assertThat(val, is(21));
  }

  /** Exports database and checks sequences are properly exported */
  @Test
  public void seq040shouldBeExported() throws IOException {
    File exportTo = new File(EXPORT_DIR);
    if (exportTo.exists()) {
      exportTo.delete();
    }
    exportTo.mkdirs();

    exportDatabase(EXPORT_DIR);
    File exportedSequence = new File(EXPORT_DIR, "sequences/TESTSEQ1.xml");
    assertThat(exportedSequence.exists(), is(true));

    String exportedContents = FileUtils.readFileToString(exportedSequence);
    String originalContents = FileUtils.readFileToString(new File("model",
        "sequences/TESTSEQ-update.xml"));

    assertThat(exportedContents, equalTo(originalContents));
  }

  private int getNextVal(String seqName) throws SQLException {
    Connection cn = null;
    try {
      cn = getDataSource().getConnection();

      PreparedStatement st;
      if (getRdbms() == Rdbms.PG) {
        st = cn.prepareStatement("select nextval(?)");
        st.setString(1, seqName);
      } else {
        st = cn.prepareStatement("select " + seqName + ".nextVal from dual");
      }

      ResultSet rs = st.executeQuery();

      rs.next();
      return rs.getInt(1);
    } finally {
      if (cn != null) {
        cn.close();
      }
    }
  }
}
