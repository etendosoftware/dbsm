package org.openbravo.dbsm.test.model.recreation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;

import org.codehaus.jettison.json.JSONException;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

public class AddDropConstraints extends TableRecreationBaseTest {

  static {
    availableTypes.add(ActionType.append);
    availableTypes.add(ActionType.drop);
  }

  public AddDropConstraints(String rdbms, String driver, String url, String sid, String user,
      String password, String name, ActionType type) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, type);
  }

  @Parameters(name = "DB: {6} - {7}")
  public static Collection<Object[]> parameters() throws IOException, JSONException {
    return TableRecreationBaseTest.parameters();
  }

  @Test
  public void index() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "IDX.xml");
  }

  @Test
  public void unique() {
    assertTablesAreNotRecreated("BASE_MODEL.xml", "UNIQUE.xml");
  }

  @Test
  public void fk() {
    assertTablesAreNotRecreated("FK_BASE.xml", "FK.xml", false);
  }
}
