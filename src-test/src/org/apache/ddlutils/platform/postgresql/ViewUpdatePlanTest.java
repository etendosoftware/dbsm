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
import static org.junit.Assert.assertTrue;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.ddlutils.DatabaseOperationException;
import org.apache.ddlutils.alteration.ModelChange;
import org.apache.ddlutils.alteration.AddViewChange;
import org.apache.ddlutils.alteration.RemoveViewChange;
import org.apache.ddlutils.alteration.RemoveFunctionChange;
import org.apache.ddlutils.alteration.RemoveTableChange;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.MaterializedView;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.StructureObject;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.View;
import org.apache.ddlutils.platform.SqlBuilder;
import org.junit.Test;

public class ViewUpdatePlanTest {
  @Test
  public void unchangedViewsProduceNoSql() throws Exception {
    Database current = model("BASE", "SELECT 1 AS id", "CHILD", "SELECT id FROM base");
    SqlBuilder builder = new PostgreSql11Platform().getSqlBuilder();
    StringWriter sql = new StringWriter();
    builder.setWriter(sql);
    Database desired = (Database) current.clone();
    builder.prepareDatabaseForAlter(current, desired, null);
    builder.alterDatabasePostScript(current, desired, null, Collections.<ModelChange>emptyList(),
        current, null);
    assertEquals("", sql.toString());
  }

  @Test
  public void whitespaceCaseAndSingleSourceQualifiersAreNotChanges() {
    Database current = model("BASE", "SELECT id, sum(amount) AS amount FROM payments p GROUP BY id");
    Database desired = model("BASE",
        "select p.id, sum (p.amount) AS amount FROM payments p GROUP BY p.id; -- comment");
    assertTrue(plan(current, desired).getDrops().isEmpty());
  }

  @Test
  public void derivedTableQualifiersAreNotChanges() {
    assertEquals(normalized("SELECT id FROM (SELECT 1 AS id) p"),
        normalized("SELECT p.id FROM (SELECT 1 AS id) p"));
  }

  @Test
  public void literalAndQuotedIdentifierChangesAreDetected() {
    assertFalse(normalized("SELECT 'A B; FROM child' AS value")
        .equals(normalized("SELECT 'A  B; FROM child' AS value")));
    assertFalse(normalized("SELECT \"AccountType\" FROM accounts")
        .equals(normalized("SELECT \"accounttype\" FROM accounts")));
  }

  @Test
  public void standardizationPreservesLiteralAndIdentifierContents() {
    assertEquals(normalized("SELECT 'a;b::text' AS \"a;label\""),
        normalized(ViewDefinition.standardize("SELECT 'a;b::text'::text AS \"a;label\";")));
    assertEquals(normalized("SELECT (-1) AS id"),
        normalized(ViewDefinition.standardize("SELECT '-1'::integer::numeric AS id;")));
    assertEquals(normalized("SELECT TRUNC(p.created, 'MM') FROM payments p"),
        normalized(ViewDefinition.standardize("SELECT date_trunc('month', created) FROM payments p;")));
  }

  @Test
  public void correlatedReferencesArePreserved() {
    String prefix = "SELECT id, (SELECT max(c.id) FROM child c WHERE c.id = ";
    assertEquals(normalized("SELECT p.id, (SELECT max(c.id) FROM child c WHERE c.id = p.id) FROM parent p"),
        normalized(prefix + "p.id) FROM parent p"));
    assertFalse(normalized(prefix + "p.id) FROM parent p")
        .equals(normalized(prefix + "id) FROM parent p")));
  }

  @Test
  public void invoiceDependentsDropFirstAndCreateLast() {
    Database current = model("C_INVOICE_V", "SELECT 1 AS id", "C_INVOICELINE_V",
        "SELECT id FROM c_invoice_v", "UNRELATED", "SELECT 0 AS id");
    Database desired = model("C_INVOICE_V", "SELECT 2 AS id", "C_INVOICELINE_V",
        "SELECT id FROM c_invoice_v", "UNRELATED", "SELECT 0 AS id");
    ViewUpdatePlan plan = plan(current, desired);
    assertEquals(Arrays.asList("C_INVOICELINE_V", "C_INVOICE_V"), names(plan.getDrops()));
    assertEquals(Arrays.asList("C_INVOICE_V", "C_INVOICELINE_V"), names(plan.getCreates()));
  }

  @Test
  public void paymentDependentsIncludeTheEntireChain() {
    Database current = payments("SELECT 1 AS id");
    ViewUpdatePlan plan = plan(current, payments("SELECT 2 AS id"));
    assertEquals(Arrays.asList("FIN_PAYMENT_PROP_DETAIL_V", "FIN_PAYMENT_SCHED_INV_V",
        "FIN_PAYMENT_DETAIL_V"), names(plan.getDrops()));
    assertEquals(Arrays.asList("FIN_PAYMENT_DETAIL_V", "FIN_PAYMENT_SCHED_INV_V",
        "FIN_PAYMENT_PROP_DETAIL_V"), names(plan.getCreates()));
  }

  @Test
  public void newViewsDoNotRecreateExistingDependencies() {
    ViewUpdatePlan plan = plan(model("BASE", "SELECT 1 AS id"),
        model("BASE", "SELECT 1 AS id", "CHILD", "SELECT id FROM base"));
    assertTrue(plan.getDrops().isEmpty());
    assertEquals(Collections.singletonList("CHILD"), names(plan.getCreates()));
  }

  @Test
  public void removedDependentsAreDroppedInReverseOrder() {
    ViewUpdatePlan plan = plan(model("BASE", "SELECT 1 AS id", "CHILD", "SELECT id FROM base"),
        new Database());
    assertEquals(Arrays.asList("CHILD", "BASE"), names(plan.getDrops()));
    assertTrue(plan.getCreates().isEmpty());
  }

  @Test
  public void tableChangesStillRecreateViews() throws Exception {
    Database current = model("BASE", "SELECT id FROM test", "CHILD", "SELECT id FROM base",
        "UNRELATED", "SELECT 1 AS id");
    Table removed = new Table();
    removed.setName("TEST");
    ViewUpdatePlan plan = new ViewUpdatePlan(current, (Database) current.clone(),
        Collections.<ModelChange>singletonList(new RemoveTableChange(removed)), false);
    assertEquals(Arrays.asList("CHILD", "BASE"), names(plan.getDrops()));
    assertEquals(Arrays.asList("BASE", "CHILD"), names(plan.getCreates()));
  }

  @Test
  public void unrelatedFunctionChangesKeepViews() throws Exception {
    Database current = model("BASE", "SELECT 1 AS id", "CHILD", "SELECT id FROM base");
    Function function = new Function();
    function.setName("UNRELATED_HELPER");
    ViewUpdatePlan plan = new ViewUpdatePlan(current, (Database) current.clone(),
        Collections.<ModelChange>singletonList(new RemoveFunctionChange(function)), false);
    assertTrue(plan.getDrops().isEmpty());
    assertTrue(plan.getCreates().isEmpty());
  }

  @Test
  public void functionChangesIncludeTheirViewDependents() throws Exception {
    Database current = model("BASE", "SELECT helper(1) AS id", "CHILD", "SELECT id FROM base",
        "UNRELATED", "SELECT 1 AS id");
    Function function = new Function();
    function.setName("HELPER");
    ViewUpdatePlan plan = new ViewUpdatePlan(current, (Database) current.clone(),
        Collections.<ModelChange>singletonList(new RemoveFunctionChange(function)), false);
    assertEquals(Arrays.asList("CHILD", "BASE"), names(plan.getDrops()));
    assertEquals(Arrays.asList("BASE", "CHILD"), names(plan.getCreates()));
  }

  @Test
  public void forcedRecreationWithoutTableChangesKeepsViews() throws Exception {
    Database current = model("BASE", "SELECT id FROM test", "CHILD", "SELECT id FROM base");
    Table table = new Table();
    table.setName("TEST");
    current.addTable(table);
    SqlBuilder builder = new PostgreSql11Platform().getSqlBuilder();
    builder.setForcedRecreation("TEST");
    StringWriter sql = new StringWriter();
    builder.setWriter(sql);
    builder.prepareDatabaseForAlter(current, (Database) current.clone(), null);
    assertEquals("", sql.toString());
  }

  @Test
  public void explicitViewChangesAreAppliedWithoutDuplicateSql() throws Exception {
    Database current = model("BASE", "SELECT 1 AS id", "CHILD", "SELECT id FROM base");
    Database desired = model("BASE", "SELECT 2 AS id", "CHILD", "SELECT id FROM base");
    List<ModelChange> changes = new ArrayList<>(Arrays.<ModelChange>asList(
        new RemoveViewChange(current.findView("BASE")), new AddViewChange(desired.findView("BASE"))));
    SqlBuilder builder = new PostgreSql11Platform().getSqlBuilder();
    builder.setWriter(new StringWriter());
    builder.prepareDatabaseForAlter(current, desired, null, changes);
    StringWriter sql = new StringWriter();
    builder.setWriter(sql);
    builder.alterDatabase(current, desired, null, changes);
    assertEquals("", sql.toString());
    assertEquals("SELECT 2 AS id", current.findView("BASE").getStatement());
    builder.alterDatabasePostScript(current, desired, null, changes, current, null);
    assertTrue(sql.toString().indexOf("CREATE OR REPLACE VIEW BASE")
        < sql.toString().indexOf("CREATE OR REPLACE VIEW CHILD"));
  }

  @Test
  public void materializedViewsShareTheDependencyOrder() throws Exception {
    Database current = model("BASE", "SELECT 1 AS id", "CHILD", "SELECT id FROM materialized");
    MaterializedView materialized = new MaterializedView("MATERIALIZED");
    materialized.setStatement("SELECT id FROM base");
    current.addMaterializedView(materialized);
    Database desired = (Database) current.clone();
    desired.findView("BASE").setStatement("SELECT 2 AS id");
    ViewUpdatePlan plan = plan(current, desired);
    assertEquals(Arrays.asList("CHILD", "MATERIALIZED", "BASE"), names(plan.getDrops()));
    assertEquals(Arrays.asList("BASE", "MATERIALIZED", "CHILD"), names(plan.getCreates()));
  }

  @Test
  public void cteAliasesDoNotCreateFalseDependencies() {
    ViewDefinition query = definition("WITH child AS (SELECT id FROM base) SELECT id FROM child");
    assertEquals(Collections.singleton("BASE"), query.getRelations());
    assertEquals(Collections.singleton("CHILD"),
        definition("WITH child AS (SELECT id FROM child) SELECT id FROM child").getRelations());
    assertTrue(definition("WITH RECURSIVE child AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM child WHERE id < 3) SELECT * FROM child")
        .getRelations().isEmpty());
  }

  @Test
  public void relationsAreFoundInJoinsCommaListsAndNestedQueries() {
    assertEquals(new java.util.HashSet<>(Arrays.asList("BASE", "SECOND", "THIRD", "FOURTH")),
        definition("SELECT 'FROM fake', $$JOIN fake$$, (SELECT count(*) FROM fourth) "
            + "FROM public.\"base\" b LEFT JOIN second s ON s.id=b.id, third t").getRelations());
    assertEquals(Collections.singleton("BASE"),
        definition("SELECT extract(year FROM created) FROM base").getRelations());
    assertEquals(Collections.singleton("BASE"),
        definition("SELECT 'backslash\\', E'escaped\\\' quote' FROM base").getRelations());
  }

  @Test(expected = DatabaseOperationException.class)
  public void cyclesAreRejectedBeforeDroppingAnything() {
    plan(new Database(), model("A", "SELECT * FROM b", "B", "SELECT * FROM a"));
  }

  private static ViewDefinition definition(String sql) {
    return new ViewDefinition(sql, false);
  }

  private static List<String> normalized(String sql) {
    return definition(sql).normalizedTokens();
  }

  private static ViewUpdatePlan plan(Database current, Database desired) {
    return new ViewUpdatePlan(current, desired, Collections.<ModelChange>emptyList(), false);
  }

  private static Database payments(String base) {
    return model("FIN_PAYMENT_DETAIL_V", base, "FIN_PAYMENT_SCHED_INV_V",
        "SELECT id FROM fin_payment_detail_v", "FIN_PAYMENT_SCHED_ORD_V", "SELECT 1 AS id",
        "FIN_PAYMENT_PROP_DETAIL_V",
        "SELECT i.id FROM fin_payment_sched_inv_v i JOIN fin_payment_sched_ord_v o ON i.id=o.id");
  }

  static Database model(String... definitions) {
    Database model = new Database();
    model.setName("VIEW_TEST");
    for (int i = 0; i < definitions.length; i += 2) {
      View view = new View(definitions[i]);
      view.setStatement(definitions[i + 1]);
      model.addView(view);
    }
    return model;
  }

  private static List<String> names(List<StructureObject> objects) {
    List<String> names = new ArrayList<>();
    for (StructureObject object : objects) {
      names.add(object.getName());
    }
    return names;
  }
}
