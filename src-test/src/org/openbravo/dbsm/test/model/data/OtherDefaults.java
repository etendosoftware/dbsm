package org.openbravo.dbsm.test.model.data;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.ddlutils.model.Database;
import org.junit.Test;
import org.openbravo.dbsm.test.base.DbsmTest;

public class OtherDefaults extends DbsmTest {

  public OtherDefaults(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  public void valuesAreKept() throws SQLException {
    resetDB();
    Database db = updateDatabase("createDefault/M2.xml");
    generateData(db, 1);

    String oldValue = getActualValue("test", "m2");
    updateDatabase("createDefault/M2.xml");
    String newValue = getActualValue("test", "m2");
    assertThat("Value is unchanged", newValue, is(equalTo(oldValue)));
  }

  @Test
  public void onCreateDefaultIsNotExecutedInADIfDataPresentCreatingNewTable() throws SQLException {
    resetDB();
    updateDatabase("createDefault/M2.xml", "data/newCreateDefault", Arrays.asList("TEST"));
    assertThat("New AD column value should be kept", getActualValue("test", "m2"),
        is(equalTo("NEW")));
  }

  @Test
  public void onCreateDefaultIsNotExecutedInADIfDataPresent() throws SQLException {
    resetDB();
    updateDatabase("createDefault/BASE_MODEL.xml");
    updateDatabase("createDefault/M2.xml", "data/newCreateDefault", Arrays.asList("TEST"));
    assertThat("New AD column value should be kept", getActualValue("test", "m2"),
        is(equalTo("NEW")));
  }
}
