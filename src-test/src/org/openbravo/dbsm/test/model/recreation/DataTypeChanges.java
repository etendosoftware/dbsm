package org.openbravo.dbsm.test.model.recreation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;

import org.codehaus.jettison.json.JSONException;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

public class DataTypeChanges extends TableRecreationBaseTest {

  static {
    availableTypes.add(ActionType.append);
  }

  public DataTypeChanges(String rdbms, String driver, String url, String sid, String user,
      String password, String name, ActionType type) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, type);
  }

  @Parameters(name = "DB: {6}")
  public static Collection<Object[]> parameters() throws IOException, JSONException {
    return TableRecreationBaseTest.parameters();
  }

  @Test
  public void changeDecimalTypeSize() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE1.xml");
  }

  @Test
  public void changeVarcharTypeSize() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE2.xml");
  }

  @Test
  public void changeCharTypeSize() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE3.xml");
  }
}
