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
  public void canCreateDBWithLargeRequiredObjects() {
    createDatabase("largeObjects/BLOB_TABLE_REQ.xml");
    assertColumnsAreRequired(true);
  }

  @Test
  public void canCreateDBWithLargeOptionalObjects() {
    createDatabase("largeObjects/BLOB_TABLE_NOREQ.xml");
    assertColumnsAreRequired(false);
  }

  @Test
  public void canUpdateDBWithLargeRequiredObjects() {
    resetDB();
    updateDatabase("largeObjects/BLOB_TABLE_REQ.xml");
    assertColumnsAreRequired(true);
  }

  @Test
  public void canUpdateDBWithLargeOptionalObjects() {
    resetDB();
    updateDatabase("largeObjects/BLOB_TABLE_NOREQ.xml");
    assertColumnsAreRequired(false);
  }

  @Test
  public void canUpdateLargeObjetsFromMandatoryToNullable() {
    createDatabase("largeObjects/BLOB_TABLE_REQ.xml");
    updateDatabase("largeObjects/BLOB_TABLE_NOREQ.xml");
    assertColumnsAreRequired(false);
  }

  @Test
  public void canUpdateLargeObjetsFromNullableToMandatory() {
    createDatabase("largeObjects/BLOB_TABLE_NOREQ.xml");
    updateDatabase("largeObjects/BLOB_TABLE_REQ.xml");
    assertColumnsAreRequired(true);
  }

  private void assertColumnsAreRequired(boolean required) {
    Database database = readModelFromDB();
    Column blobColumn = database.findTable("BLOB_TABLE").findColumn("BLOB_FIELD");
    assertThat(blobColumn.getName() + " is required", blobColumn.isRequired(), is(required));

    Column clobColumn = database.findTable("BLOB_TABLE").findColumn("CLOB_FIELD");
    assertThat(clobColumn.getName() + " is required", clobColumn.isRequired(), is(required));
  }
}
