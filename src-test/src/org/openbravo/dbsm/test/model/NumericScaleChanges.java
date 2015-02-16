package org.openbravo.dbsm.test.model;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Vector;

import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.alteration.ColumnSizeChange;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;

/**
 * Test cases covering correct management of scale changes in numeric columns
 * 
 * @author alostale
 *
 */
public class NumericScaleChanges extends DbsmTest {

  public NumericScaleChanges(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  /**
   * Configuration script manages scale change and they are properly applied.
   * 
   * See issue #28913
   */
  @Test
  public void configScriptShouldApplyScaleChanges() throws CloneNotSupportedException {
    resetDB();
    Database db = updateDatabase("scaleChanges/TEST.xml");
    Database origDB = (Database) db.clone();

    DatabaseIO dbIO = new DatabaseIO();
    Vector<Change> changes = dbIO.readChanges(new File("model", "scaleChanges/configScript.xml"));

    boolean changeFound = false;
    for (Change change : changes) {
      if (change instanceof ColumnSizeChange) {
        ColumnSizeChange sizeChange = (ColumnSizeChange) change;
        assertThat("old scale", sizeChange.getOldScale(), is(0));
        assertThat("new scale", sizeChange.getNewScale(), is(2));
        sizeChange.apply(db, getPlatform().isDelimitedIdentifierModeOn());
        changeFound = true;
      }
    }
    assertThat("change should be in config script", changeFound, is(true));

    // apply real changes in DB
    getPlatform().alterTables(origDB, db, false);

    Database appliedDB = readModelFromDB();
    Column lineColumn = appliedDB.findTable("TEST").findColumn("LINE");
    assertThat(lineColumn.getSize(), is("10,2"));
    assertThat(lineColumn.getScale(), is(2));

  }

  /** update.database properly applies scale changes */
  @Test
  public void updateDatabaseShouldApplyScaleChanges() throws CloneNotSupportedException {
    resetDB();
    updateDatabase("scaleChanges/TEST.xml");
    updateDatabase("scaleChanges/TEST2.xml");

    Database appliedDB = readModelFromDB();
    Column lineColumn = appliedDB.findTable("TEST").findColumn("LINE");
    assertThat(lineColumn.getSize(), is("10,3"));
    assertThat(lineColumn.getScale(), is(3));
  }
}
