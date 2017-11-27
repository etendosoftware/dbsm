package org.openbravo.dbsm.test.model.recreation;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

public class CombinedTypeChanges extends DataTypeChanges {

  public CombinedTypeChanges(String rdbms, String driver, String url, String sid, String user,
      String password, String name, ActionType type, RecreationMode recMode)
      throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, type, recMode);
  }

  @Test
  public void fromVarcharToNVarcharAndSize() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE7.xml");
  }

  @Test
  public void fromVarcharToNVarcharAndOnCreateDefault() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE8.xml");
  }
}
