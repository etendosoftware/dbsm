package org.openbravo.dbsm.test.model.recreation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;

import org.codehaus.jettison.json.JSONException;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

public class AddDropColumn extends TableRecreationBaseTest {

  static {
    availableTypes.add(ActionType.append);
    availableTypes.add(ActionType.prepend);
    availableTypes.add(ActionType.drop);
  }

  public AddDropColumn(String rdbms, String driver, String url, String sid, String user,
      String password, String name, ActionType type) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, type);
  }

  @Parameters(name = "DB: {6} - {7}")
  public static Collection<Object[]> parameters() throws IOException, JSONException {
    return TableRecreationBaseTest.parameters();
  }

  @Test
  public void nonMandatoryColumn_COL1() {
    assertTablesAreNotRecreated("COL1.xml");
  }

  @Test
  public void mandatoryColumnWithDefault_COL2() {
    assertTablesAreNotRecreated("COL2.xml");
  }

  @Test
  public void mandatoryColumnWithOnCreateDefault_COL3() {
    assertTablesAreNotRecreated("COL3.xml");
  }

  @Test
  public void nonMandatoryColumnWithDefault_COL4() {
    assertTablesAreNotRecreated("COL4.xml");
  }

  @Test
  public void nonMandatoryColumnWithOnCreateDefault_COL5() {
    assertTablesAreNotRecreated("COL5.xml");
  }

  @Test
  public void twoNonMandatoryColumns_COL6() {
    assertTablesAreNotRecreated("COL6.xml");
  }
}
