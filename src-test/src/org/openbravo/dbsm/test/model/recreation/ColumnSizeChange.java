package org.openbravo.dbsm.test.model.recreation;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;

public class ColumnSizeChange extends DataTypeChanges {

  public ColumnSizeChange(String rdbms, String driver, String url, String sid, String user,
      String password, String name, ActionType type, RecreationMode recMode)
      throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name, type, recMode);
  }

  @Test
  public void increaseVarcharSize() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE2.xml");
  }

  @Test
  public void increaseCharSize() {
    assertTablesAreNotRecreated("DATA_TYPE_BASE.xml", "DATA_TYPE3.xml");
  }

  @Test
  public void increaseNumberPrecisionNoScale() {
    assertTablesAreNotRecreated("DATA_TYPE_NUMBERS_BASE.xml", "DATA_TYPE_NUMBERS1.xml");
  }

  @Ignore("This case cannot be supported")
  @Test
  public void increaseScaleKeepPrecision() {
    assertTablesAreNotRecreated("DATA_TYPE_NUMBERS_BASE.xml", "DATA_TYPE_NUMBERS2.xml");
  }

  @Test
  public void decreaseScaleKeepPrecision() {
    assertTablesAreNotRecreated("DATA_TYPE_NUMBERS2.xml", "DATA_TYPE_NUMBERS_BASE.xml");
  }

  @Test
  public void fromAnyPrecisionToFixedPrecision() {
    // it works in PG as far as data doesn't overflow new restriction
    assertTablesAreNotRecreated("DATA_TYPE_NUMBERS_BASE.xml", "DATA_TYPE_NUMBERS3.xml");
  }
}
