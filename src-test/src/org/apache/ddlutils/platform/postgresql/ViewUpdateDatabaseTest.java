/*
 ************************************************************************************
 * Copyright (C) 2026 Etendo Software
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 ************************************************************************************
 */
package org.apache.ddlutils.platform.postgresql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.alteration.ModelChange;
import org.apache.ddlutils.alteration.ModelComparator;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.View;
import org.apache.ddlutils.platform.PGStandardBatchEvaluator;
import org.apache.ddlutils.platform.PlatformImplBase;
import org.apache.ddlutils.platform.SqlBuilder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * PostgreSQL integration tests in a disposable schema. The database comes from
 * -Ddbsm.test.coreProperties=/path/to/config/Openbravo.properties when set, otherwise from the
 * first PostgreSQL entry of config/db-config.json, as in the rest of ModelSuite. The tests are
 * skipped only if neither is available.
 */
public class ViewUpdateDatabaseTest {
  private BasicDataSource dataSource;
  private Connection connection;
  private Platform platform;
  private String schema;

  @Before
  public void createSchema() throws Exception {
    String driver;
    String url;
    String user;
    String password;
    String config = System.getProperty("dbsm.test.coreProperties");
    if (config != null) {
      Properties properties = new Properties();
      try (FileReader reader = new FileReader(config)) {
        properties.load(reader);
      }
      driver = properties.getProperty("bbdd.driver");
      url = properties.getProperty("bbdd.url") + "/" + properties.getProperty("bbdd.sid");
      user = properties.getProperty("bbdd.user");
      password = properties.getProperty("bbdd.password");
    } else {
      File suiteConfig = new File("config/db-config.json");
      Assume.assumeTrue("No database configuration available", suiteConfig.exists());
      JSONObject pg = findPostgreSql(suiteConfig);
      Assume.assumeNotNull(pg);
      driver = pg.getString("driver");
      url = pg.getString("url") + "/" + pg.getString("sid");
      user = pg.getString("user");
      password = pg.getString("password");
    }
    dataSource = new BasicDataSource();
    dataSource.setDriverClassName(driver);
    dataSource.setUrl(url);
    dataSource.setUsername(user);
    dataSource.setPassword(password);
    platform = PlatformFactory.createNewPlatformInstance(dataSource);
    connection = dataSource.getConnection();
    schema = "dbsm_view_test_" + UUID.randomUUID().toString().replace("-", "");
    execute("CREATE SCHEMA " + schema);
    execute("SET search_path TO " + schema + ",public");
  }

  private static JSONObject findPostgreSql(File suiteConfig) throws Exception {
    String content = new String(java.nio.file.Files.readAllBytes(suiteConfig.toPath()), "UTF-8");
    JSONArray dbs = new JSONObject(content).getJSONArray("testDBs");
    for (int i = 0; i < dbs.length(); i++) {
      JSONObject db = dbs.getJSONObject(i);
      if ("POSTGRE".equals(db.getString("rdbms")) && !(db.has("skip") && db.getBoolean("skip"))) {
        return db;
      }
    }
    return null;
  }

  @After
  public void removeSchema() throws Exception {
    try {
      if (connection != null) {
        try {
          if (schema != null) {
            execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
          }
        } finally {
          connection.close();
        }
      }
    } finally {
      if (dataSource != null) {
        dataSource.close();
      }
    }
  }

  @Test
  public void unchangedViewsKeepTheirOids() throws Exception {
    Database desired = invoices("SELECT 1 AS id");
    apply(new Database(), desired);
    Map<String, Long> before = oids();
    assertEquals("", apply(readViews(), desired));
    assertEquals(before, oids());
  }

  @Test
  public void changesToLiteralContentsAreApplied() throws Exception {
    Database original = ViewUpdatePlanTest.model("BASE", "SELECT 'a;b' AS value");
    apply(new Database(), original);
    assertEquals("", apply(readViews(), original));
    apply(readViews(), ViewUpdatePlanTest.model("BASE", "SELECT 'ab' AS value"));
    try (Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT value FROM base")) {
      assertTrue(rows.next());
      assertEquals("ab", rows.getString(1));
    }
  }

  @Test
  public void invoiceChangeUpdatesTheDependentWithoutRetries() throws Exception {
    apply(new Database(), invoices("SELECT 1 AS id"));
    Map<String, Long> before = oids();
    String sql = apply(readViews(), invoices("SELECT 2 AS id"));
    assertEquals(2, value("c_invoiceline_v"));
    assertNotEquals(before.get("c_invoice_v"), oids().get("c_invoice_v"));
    assertNotEquals(before.get("c_invoiceline_v"), oids().get("c_invoiceline_v"));
    assertEquals(before.get("unrelated"), oids().get("unrelated"));
    assertFalse(sql.contains("CASCADE"));
  }

  @Test
  public void viewsRemovedByPrescriptsAreRestoredInTheSameUpdate() throws Exception {
    Database desired = invoices("SELECT 1 AS id");
    apply(new Database(), desired);
    Database current = readViews();
    Map<String, Long> before = oids();
    execute("DROP VIEW c_invoiceline_v");
    platform.alterTables(connection, current, desired, true);
    ((PlatformImplBase) platform).alterTablesPostScript(connection, current, desired, true,
        java.util.Collections.<ModelChange>emptyList(), current, null);
    assertEquals(1, value("c_invoiceline_v"));
    assertEquals(before.get("c_invoice_v"), oids().get("c_invoice_v"));
    assertEquals(before.get("unrelated"), oids().get("unrelated"));
    assertNotEquals(before.get("c_invoiceline_v"), oids().get("c_invoiceline_v"));
  }

  @Test
  public void paymentChangeUpdatesTransitiveDependentsWithoutRetries() throws Exception {
    apply(new Database(), payments("SELECT 1 AS id"));
    Map<String, Long> before = oids();
    apply(readViews(), payments("SELECT 2 AS id"));
    assertEquals(2, value("fin_payment_prop_detail_v"));
    assertNotEquals(before.get("fin_payment_prop_detail_v"), oids().get("fin_payment_prop_detail_v"));
    assertEquals(before.get("fin_payment_sched_ord_v"), oids().get("fin_payment_sched_ord_v"));
  }

  @Test
  public void columnTypeChangesAreNotBlockedByDependentViews() throws Exception {
    execute("CREATE TABLE view_test_table (text VARCHAR(20))");
    execute("INSERT INTO view_test_table VALUES ('preserved')");
    Database current = ViewUpdatePlanTest.model("BASE", "SELECT text FROM view_test_table",
        "CHILD", "SELECT text FROM base");
    apply(new Database(), current);
    Table table = new Table();
    table.setName("VIEW_TEST_TABLE");
    Column text = new Column();
    text.setName("TEXT");
    text.setType("VARCHAR");
    text.setSize("20");
    table.addColumn(text);
    current.addTable(table);
    Database desired = (Database) current.clone();
    desired.findTable("VIEW_TEST_TABLE").findColumn("TEXT").setSize("40");
    apply(current, desired);
    try (Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT text FROM child")) {
      assertTrue(rows.next());
      assertEquals("preserved", rows.getString(1));
    }
  }

  @Test
  public void coreViewsRoundTripWithoutRecreation() throws Exception {
    String directory = System.getProperty("dbsm.test.viewDirectory");
    Assume.assumeNotNull(directory);
    Database desired = new Database();
    desired.setName("CORE_VIEWS");
    DatabaseIO io = new DatabaseIO();
    io.setValidateXml(false);
    File[] files = new File(directory).listFiles((dir, name) -> name.endsWith(".xml"));
    assertTrue(files != null && files.length > 0);
    for (File file : files) {
      for (View view : io.read(file).getViews()) {
        desired.addView(view);
      }
    }
    apply(new Database(), desired);
    Map<String, Long> before = oids();
    assertEquals(desired.getViewCount(), before.size());
    assertEquals("", apply(readViews(), desired));
    assertEquals(before, oids());
  }

  @Test
  public void unrecoverableForcedCommandsLogTheSqlStatement() {
    List<String> messages = new ArrayList<>();
    AbstractAppender capture = new AbstractAppender("view-error-test", null,
        PatternLayout.createDefaultLayout(), true) {
      @Override
      public void append(LogEvent event) {
        messages.add(event.getMessage().getFormattedMessage());
      }
    };
    Logger logger = (Logger) LogManager.getLogger(PGStandardBatchEvaluator.class);
    Level previousLevel = logger.getLevel();
    capture.start();
    logger.addAppender(capture);
    logger.setLevel(Level.INFO);
    try {
      int failures = new PGStandardBatchEvaluator(platform).evaluateBatch(connection,
          Collections.singletonList("-- SCRIPT OPTIONS (FORCE = TRUE)\nDROP VIEW missing_test_view"),
          true, 0);
      assertEquals(1, failures);
      assertTrue(messages.stream().anyMatch(m -> m.contains("SQL Command failed with:")
          && m.contains("DROP VIEW missing_test_view")));
    } finally {
      logger.setLevel(previousLevel);
      logger.removeAppender(capture);
      capture.stop();
    }
  }

  private String apply(Database current, Database desired) throws Exception {
    SqlBuilder builder = platform.getSqlBuilder();
    List<ModelChange> changes = new ModelComparator(platform.getPlatformInfo(), false)
        .compare(current, desired);
    StringWriter sql = new StringWriter();
    builder.setWriter(sql);
    builder.prepareDatabaseForAlter(current, desired, null, changes);
    String dropSql = sql.toString();
    executeScript(dropSql);
    sql = new StringWriter();
    builder.setWriter(sql);
    builder.alterDatabase(current, desired, null, changes);
    builder.alterDatabasePostScript(current, desired, null, changes, current, null);
    executeScript(sql.toString());
    return dropSql + sql;
  }

  private void executeScript(String sql) throws Exception {
    // Execute each generated command once: any dependency/order error fails the test immediately.
    for (String command : sql.split("/-- END[^\\n]*\\n?")) {
      if (!command.replaceAll("(?m)^\\s*--[^\\n]*", "").trim().isEmpty()) {
        execute(command);
      }
    }
  }

  private void execute(String sql) throws Exception {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private Database readViews() throws Exception {
    Database model = new Database();
    model.setName("CURRENT_VIEWS");
    try (Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT upper(viewname), "
            + "pg_get_viewdef(quote_ident(schemaname) || '.' || quote_ident(viewname), true) "
            + "FROM pg_views WHERE schemaname = current_schema()")) {
      while (rows.next()) {
        View view = new View(rows.getString(1));
        view.setStatement(ViewDefinition.standardize(rows.getString(2)));
        model.addView(view);
      }
    }
    return model;
  }

  private Map<String, Long> oids() throws Exception {
    Map<String, Long> result = new TreeMap<>();
    try (Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT c.relname, c.oid FROM pg_class c "
            + "JOIN pg_namespace n ON n.oid=c.relnamespace "
            + "WHERE n.nspname=current_schema() AND c.relkind='v'")) {
      while (rows.next()) {
        result.put(rows.getString(1), rows.getLong(2));
      }
    }
    return result;
  }

  private int value(String view) throws Exception {
    try (Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT id FROM " + view)) {
      assertTrue(rows.next());
      return rows.getInt(1);
    }
  }

  private static Database invoices(String base) {
    return ViewUpdatePlanTest.model("C_INVOICE_V", base, "C_INVOICELINE_V",
        "SELECT id FROM c_invoice_v", "UNRELATED", "SELECT 0 AS id");
  }

  private static Database payments(String base) {
    return ViewUpdatePlanTest.model("FIN_PAYMENT_DETAIL_V", base, "FIN_PAYMENT_SCHED_INV_V",
        "SELECT id FROM fin_payment_detail_v", "FIN_PAYMENT_SCHED_ORD_V", "SELECT 1 AS id",
        "FIN_PAYMENT_PROP_DETAIL_V",
        "SELECT i.id, o.id AS order_id FROM fin_payment_sched_inv_v i CROSS JOIN fin_payment_sched_ord_v o");
  }
}
