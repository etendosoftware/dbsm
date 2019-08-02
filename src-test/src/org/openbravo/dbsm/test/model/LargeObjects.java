package org.openbravo.dbsm.test.model;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;

public class LargeObjects extends DbsmTest {

  public LargeObjects(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  public void largeObjectsShouldBeSupported() {
    Database database;
    Column blobField;
    Column clobField;
    createDatabase("largeObjects/BLOB_TABLE_REQ.xml");
    database = readModelFromDB();
    blobField = database.findTable("BLOB_TABLE").findColumn("BLOB_FIELD");
    assertThat(blobField.isRequired(), is(true));
    clobField = database.findTable("BLOB_TABLE").findColumn("CLOB_FIELD");
    assertThat(clobField.isRequired(), is(true));

    updateDatabase("largeObjects/BLOB_TABLE_NOREQ.xml");
    database = readModelFromDB();
    blobField = database.findTable("BLOB_TABLE").findColumn("BLOB_FIELD");
    assertThat(blobField.isRequired(), is(false));
    clobField = database.findTable("BLOB_TABLE").findColumn("CLOB_FIELD");
    assertThat(clobField.isRequired(), is(false));
  }

}
