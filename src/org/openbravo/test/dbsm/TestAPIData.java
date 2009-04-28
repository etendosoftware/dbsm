package org.openbravo.test.dbsm;

import java.io.File;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.DataComparator;
import org.apache.ddlutils.io.DataReader;
import org.apache.ddlutils.io.DataToArraySink;
import org.apache.ddlutils.io.DatabaseDataIO;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;
import org.openbravo.base.provider.OBConfigFileProvider;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.ddlutils.task.DatabaseUtils;
import org.openbravo.ddlutils.util.DBSMOBUtil;
import org.openbravo.ddlutils.util.ValidateAPIData;
import org.openbravo.test.base.BaseTest;

public class TestAPIData extends BaseTest {
  protected void setConfigPropertyFiles() {
    File configDirectory = new File("/ws/pi/openbravo/config");
    File f = new File(configDirectory, "Openbravo.properties");
    OBPropertiesProvider.getInstance().setProperties(f.getAbsolutePath());
    OBConfigFileProvider.getInstance().setFileLocation(configDirectory.getAbsolutePath());
  }

  public void testAPIData() {
    setUserContext("0");
    long ini = System.currentTimeMillis();
    final BasicDataSource ds = new BasicDataSource();
    ds.setDriverClassName("oracle.jdbc.driver.OracleDriver");
    ds.setUrl("jdbc:oracle:thin:pi/TAD@localhost:1521:xe");
    ds.setUsername("pi");
    ds.setPassword("tad");
    final Platform platform = PlatformFactory.createNewPlatformInstance(ds);

    Vector<File> dataFiles = DBSMOBUtil
        .loadFilesFromFolder("/ws/projects/api-test/test/db3/sourcedata");
    System.out.println("Reading xml model...");
    Database xmlModel = DatabaseUtils
        .readDatabase(new File("/ws/projects/api-test/test/db3/model"));
    final DatabaseDataIO dbdio = new DatabaseDataIO();
    dbdio.setEnsureFKOrder(false);
    dbdio.setDatabaseFilter(DatabaseUtils.getDynamicDatabaseFilter(
        "com.openbravo.db.OpenbravoMetadataFilter", xmlModel));

    DataReader dataReader = dbdio.getConfiguredCompareDataReader(xmlModel);

    final DatabaseData databaseXMLData = new DatabaseData(xmlModel);
    for (int j = 0; j < dataFiles.size(); j++) {
      // getLog().info("Loading data for module. Path:
      // "+dataFiles.get(i).getAbsolutePath());
      try {
        dataReader.getSink().start();
        final String tablename = dataFiles.get(j).getName().substring(0,
            dataFiles.get(j).getName().length() - 4);
        final Vector<DynaBean> vectorDynaBeans = ((DataToArraySink) dataReader.getSink())
            .getVector();
        dataReader.parse(dataFiles.get(j));
        databaseXMLData.insertDynaBeansFromVector(tablename, vectorDynaBeans);
        dataReader.getSink().end();
      } catch (final Exception e) {
        e.printStackTrace();
      }
    }

    final DataComparator dataComparator = new DataComparator(platform.getSqlBuilder()
        .getPlatformInfo(), platform.isDelimitedIdentifierModeOn());
    dataComparator.setFilter(DatabaseUtils.getDynamicDatabaseFilter(
        "com.openbravo.db.OpenbravoMetadataFilter", xmlModel));
    System.out.println("Comparing models");
    dataComparator.compareUsingDAL(xmlModel, xmlModel, platform, databaseXMLData, "ADCS", null);

    ValidateAPIData val = new ValidateAPIData(dataComparator.getChanges());
    val.execute();
    val.printErrors();
    val.printWarnings();
    System.out.println("Time: " + (System.currentTimeMillis() - ini));
  }
}
