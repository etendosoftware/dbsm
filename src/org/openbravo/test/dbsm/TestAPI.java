package org.openbravo.test.dbsm;

import java.io.File;

import junit.framework.TestCase;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.model.Database;
import org.openbravo.ddlutils.task.DatabaseUtils;
import org.openbravo.ddlutils.util.ValidateAPIModel;

public class TestAPI extends TestCase {
  public void testAPI() {
    final BasicDataSource ds = new BasicDataSource();
    ds.setDriverClassName("oracle.jdbc.driver.OracleDriver");
    ds.setUrl("jdbc:oracle:thin:pi/TAD@localhost:1521:xe");
    ds.setUsername("pi");
    ds.setPassword("tad");
    final Platform platform = PlatformFactory.createNewPlatformInstance(ds);

    // Database db = platform.loadModelFromDatabase(DatabaseUtils
    // .getExcludeFilter("org.apache.ddlutils.platform.ExcludeFilter"));
    Database db1 = DatabaseUtils.readDatabase(new File("/ws/projects/api-test/test/db1/model"));
    Database db2 = DatabaseUtils.readDatabase(new File("/ws/projects/api-test/test/db2/model"));
    ValidateAPIModel validate = new ValidateAPIModel(platform, db1, db2);
    validate.execute();
    validate.printErrors();
    validate.printWarnings();
  }
}
