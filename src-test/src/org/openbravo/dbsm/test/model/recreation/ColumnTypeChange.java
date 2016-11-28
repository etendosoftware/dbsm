package org.openbravo.dbsm.test.model.recreation;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

public class ColumnTypeChange extends DataTypeChanges {

  public ColumnTypeChange(String rdbms, String driver, String url, String sid, String user,
      String password, String name, ActionType type, RecreationMode recMode)
      throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, type, recMode);
  }

  @Test
  public void changeVarcharToNVarchar() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE4.xml");
  }

  @Test
  public void changeNVarcharToVarchar() {
    worksOnlyIn(Rdbms.PG);
    assertTablesAreNotRecreated("DATA_TYPE4.xml", "DATA_TYPE_BASE.xml");
  }

  @Test
  public void changeNVarcharToVarcharORA() {
    worksOnlyIn(Rdbms.ORA);
    assertTablesAreRecreated("DATA_TYPE4.xml", "DATA_TYPE_BASE.xml");
  }

  @Test
  public void changeVarcharToText() {
    worksOnlyIn(Rdbms.PG);
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE5.xml");
  }

  @Test
  public void changeVarcharToORA() {
    worksOnlyIn(Rdbms.ORA);
    assertTablesAreRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE5.xml");
  }

  @Test
  public void changeCharToText() {
    worksOnlyIn(Rdbms.PG);
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE6.xml");
  }

  @Test
  public void changeCharToTextORA() {
    worksOnlyIn(Rdbms.ORA);
    assertTablesAreRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE6.xml");
  }

}
