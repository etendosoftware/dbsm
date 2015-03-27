package org.openbravo.dbsm.test.model.recreation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;

import org.codehaus.jettison.json.JSONException;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

public class OtherRecreations extends TableRecreationBaseTest {

  static {
    availableTypes.add(ActionType.append);
  }

  public OtherRecreations(String rdbms, String driver, String url, String sid, String user,
      String password, String name, ActionType type) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, type);
  }

  @Parameters(name = "DB: {6} - {7}")
  public static Collection<Object[]> parameters() throws IOException, JSONException {
    return TableRecreationBaseTest.parameters();
  }

  @Test
  public void realADTableMP17to15Q2() {
    assertTablesAreNotRecreated("AD_TABLE-MP17.xml", "AD_TABLE-PR15Q2.xml", false);
  }

  @Test
  public void createDefaultRemoval() {
    assertTablesAreNotRecreated("OCD1.xml", "OCD2.xml", false);
  }
}
